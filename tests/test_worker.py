from __future__ import annotations

import hashlib
import sqlite3
import tempfile
from pathlib import Path
import unittest

from app.worker import WorkerConfig, run_worker


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
