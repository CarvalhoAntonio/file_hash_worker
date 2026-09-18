from __future__ import annotations

import hashlib
import sqlite3
import tempfile
from pathlib import Path
import unittest

from app.worker import WorkerConfig, parse_mssql_url, run_worker


class FileHashWorkerTests(unittest.TestCase):
    def test_hashes_file_and_adds_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_file = root / "doc.txt"
            source_file.write_bytes(b"hello world")
            db_path = root / "files.sqlite"
            self._create_database(db_path, [(1, str(source_file))])

            stats = run_worker(self._config(db_path))

            self.assertEqual(stats.claimed, 1)
            self.assertEqual(stats.hashed, 1)
            row = self._fetch_row(db_path, 1)
            self.assertEqual(row["content_hash"], hashlib.sha256(b"hello world").hexdigest())
            self.assertEqual(row["content_hash_algo"], "sha256")
            self.assertEqual(row["content_hash_status"], "hashed")
            self.assertEqual(row["content_hash_size_bytes"], 11)

    def test_missing_file_is_marked_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "files.sqlite"
            self._create_database(db_path, [(1, str(root / "missing.txt"))])

            stats = run_worker(self._config(db_path))

            self.assertEqual(stats.missing, 1)
            row = self._fetch_row(db_path, 1)
            self.assertEqual(row["content_hash_status"], "missing")
            self.assertIsNone(row["content_hash"])
            self.assertIn("missing.txt", row["content_hash_error"])

    def test_rerun_skips_hashed_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_file = root / "doc.txt"
            source_file.write_bytes(b"hello world")
            db_path = root / "files.sqlite"
            self._create_database(db_path, [(1, str(source_file))])

            first = run_worker(self._config(db_path))
            second = run_worker(self._config(db_path))

            self.assertEqual(first.claimed, 1)
            self.assertEqual(second.claimed, 0)

    def test_path_prefix_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            mounted_file = root / "data" / "doc.txt"
            mounted_file.parent.mkdir()
            mounted_file.write_bytes(b"mapped")
            db_path = root / "files.sqlite"
            self._create_database(db_path, [(1, "/host/doc.txt")])

            config = self._config(
                db_path,
                path_prefix_from="/host",
                path_prefix_to=str(root / "data"),
            )
            stats = run_worker(config)

            self.assertEqual(stats.hashed, 1)
            row = self._fetch_row(db_path, 1)
            self.assertEqual(row["content_hash"], hashlib.sha256(b"mapped").hexdigest())

    def test_csv_path_mapping_uses_real_file_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            real_file = root / "real" / "doc.txt"
            real_file.parent.mkdir()
            real_file.write_bytes(b"real content")
            mapping_csv = root / "mapping.csv"
            mapping_csv.write_text(
                "wrong_path,real_path\n"
                f"/wrong/doc.txt,{real_file}\n",
                encoding="utf-8",
            )
            db_path = root / "files.sqlite"
            self._create_database(db_path, [(1, "/wrong/doc.txt")])

            stats = run_worker(
                self._config(
                    db_path,
                    path_mapping_csv=str(mapping_csv),
                    path_mapping_key_column="wrong_path",
                    path_mapping_value_column="real_path",
                    path_mapping_index=str(root / "mapping_index.sqlite"),
                )
            )

            self.assertEqual(stats.hashed, 1)
            row = self._fetch_row(db_path, 1)
            self.assertEqual(row["content_hash"], hashlib.sha256(b"real content").hexdigest())

    def test_missing_csv_mapping_is_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            mapping_csv = root / "mapping.csv"
            mapping_csv.write_text("wrong_path,real_path\n/other/path,/real/path\n", encoding="utf-8")
            db_path = root / "files.sqlite"
            self._create_database(db_path, [(1, "/wrong/doc.txt")])

            stats = run_worker(
                self._config(
                    db_path,
                    path_mapping_csv=str(mapping_csv),
                    path_mapping_key_column="wrong_path",
                    path_mapping_value_column="real_path",
                    path_mapping_index=str(root / "mapping_index.sqlite"),
                )
            )

            self.assertEqual(stats.failed, 1)
            row = self._fetch_row(db_path, 1)
            self.assertEqual(row["content_hash_status"], "failed")
            self.assertIn("path mapping not found", row["content_hash_error"])

    def test_csv_share_prefix_mapping_replaces_backslash_share(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            share_root = root / "share_a"
            real_file = share_root / "folder" / "doc.txt"
            real_file.parent.mkdir(parents=True)
            real_file.write_bytes(b"share content")
            mapping_csv = root / "shares.csv"
            mapping_csv.write_text(
                "SHARE_NAME,SHARE\n"
                f"ARCHIVE01,{share_root}\n",
                encoding="utf-8",
            )
            db_path = root / "files.sqlite"
            self._create_database(db_path, [(1, r"ARCHIVE01\\folder\\doc.txt")])

            stats = run_worker(
                self._config(
                    db_path,
                    path_mapping_csv=str(mapping_csv),
                    path_mapping_key_column="SHARE_NAME",
                    path_mapping_value_column="SHARE",
                    path_mapping_index=str(root / "share_index.sqlite"),
                    path_mapping_mode="share_prefix",
                )
            )

            self.assertEqual(stats.hashed, 1)
            row = self._fetch_row(db_path, 1)
            self.assertEqual(row["content_hash"], hashlib.sha256(b"share content").hexdigest())

    def test_csv_share_prefix_mapping_missing_share_is_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            mapping_csv = root / "shares.csv"
            mapping_csv.write_text("SHARE_NAME,SHARE\nOTHER,/real/share\n", encoding="utf-8")
            db_path = root / "files.sqlite"
            self._create_database(db_path, [(1, r"ARCHIVE01\\folder\\doc.txt")])

            stats = run_worker(
                self._config(
                    db_path,
                    path_mapping_csv=str(mapping_csv),
                    path_mapping_key_column="SHARE_NAME",
                    path_mapping_value_column="SHARE",
                    path_mapping_index=str(root / "share_index.sqlite"),
                    path_mapping_mode="share_prefix",
                )
            )

            self.assertEqual(stats.failed, 1)
            row = self._fetch_row(db_path, 1)
            self.assertEqual(row["content_hash_status"], "failed")
            self.assertIn("path mapping not found", row["content_hash_error"])

    def test_parse_mssql_url(self) -> None:
        parsed = parse_mssql_url("mssql+pymssql://user:pass@db-host:1433/mydb?timeout=10")

        self.assertEqual(parsed["host"], "db-host")
        self.assertEqual(parsed["port"], 1433)
        self.assertEqual(parsed["user"], "user")
        self.assertEqual(parsed["password"], "pass")
        self.assertEqual(parsed["database"], "mydb")
        self.assertEqual(parsed["timeout"], 10)

    def _config(self, db_path: Path, **overrides) -> WorkerConfig:
        values = {
            "db_type": "sqlite",
            "db_url": f"sqlite:///{db_path}",
            "table_name": "documents",
            "id_column": "id",
            "path_column": "path",
            "hash_algo": "sha256",
            "batch_size": 10,
            "workers": 2,
        }
        values.update(overrides)
        return WorkerConfig(**values)

    def _create_database(self, db_path: Path, rows) -> None:
        with sqlite3.connect(db_path) as connection:
            connection.execute("CREATE TABLE documents(id INTEGER PRIMARY KEY, path TEXT NOT NULL)")
            connection.executemany("INSERT INTO documents(id, path) VALUES(?, ?)", rows)

    def _fetch_row(self, db_path: Path, row_id: int) -> sqlite3.Row:
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        try:
            return connection.execute("SELECT * FROM documents WHERE id = ?", (row_id,)).fetchone()
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
