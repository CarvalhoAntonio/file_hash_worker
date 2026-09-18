from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import logging
import os
from pathlib import Path
import re
import sqlite3
import sys
from typing import List, Optional, Protocol, Sequence
from urllib.parse import parse_qs, unquote, urlparse


LOGGER = logging.getLogger("file_hash_worker")
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class WorkerConfig:
    db_type: str
    db_url: str
    table_name: str
    id_column: str
    path_column: str
    hash_column: str = "content_hash"
    algo_column: str = "content_hash_algo"
    status_column: str = "content_hash_status"
    error_column: str = "content_hash_error"
    size_column: str = "content_hash_size_bytes"
    updated_at_column: str = "content_hash_updated_at"
    hash_algo: str = "blake3"
    batch_size: int = 1000
    workers: int = 16
    read_chunk_bytes: int = 8 * 1024 * 1024
    max_rows: int = 0
    ensure_columns: bool = True
    reset_processing_on_start: bool = False
    path_prefix_from: str = ""
    path_prefix_to: str = ""
    where_sql: str = ""
    claim_statuses: Sequence[str] = ("pending", "failed")


@dataclass(frozen=True)
class ClaimedFile:
    row_id: object
    path: str


@dataclass(frozen=True)
class HashResult:
    row_id: object
    content_hash: Optional[str]
    hash_algo: str
    status: str
    error: Optional[str]
    size_bytes: Optional[int]
    updated_at: str


@dataclass
class WorkerStats:
    claimed: int = 0
    hashed: int = 0
    missing: int = 0
    failed: int = 0


class DatabaseClient(Protocol):
    def ensure_columns(self) -> None:
        ...

    def reset_processing(self) -> int:
        ...

    def claim_batch(self, limit: int) -> List[ClaimedFile]:
        ...

    def update_results(self, results: Sequence[HashResult]) -> None:
        ...

    def close(self) -> None:
        ...


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = parse_config(argv)
    stats = run_worker(config)
    LOGGER.info(
        "finished claimed=%s hashed=%s missing=%s failed=%s",
        stats.claimed,
        stats.hashed,
        stats.missing,
        stats.failed,
    )
    return 0


def run_worker(config: WorkerConfig) -> WorkerStats:
    validate_config(config)
    ensure_hash_available(config.hash_algo)

    client = create_database_client(config)
    stats = WorkerStats()
    try:
        if config.ensure_columns:
            client.ensure_columns()
        if config.reset_processing_on_start:
            reset_count = client.reset_processing()
            LOGGER.info("reset processing rows count=%s", reset_count)

        while True:
            remaining = config.max_rows - stats.claimed if config.max_rows else config.batch_size
            if remaining <= 0:
                break
            batch_limit = min(config.batch_size, remaining)
            batch = client.claim_batch(batch_limit)
            if not batch:
                break

            stats.claimed += len(batch)
            results = hash_batch(batch, config)
            client.update_results(results)

            for result in results:
                if result.status == "hashed":
                    stats.hashed += 1
                elif result.status == "missing":
                    stats.missing += 1
                elif result.status == "failed":
                    stats.failed += 1

            LOGGER.info(
                "progress claimed=%s hashed=%s missing=%s failed=%s",
                stats.claimed,
                stats.hashed,
                stats.missing,
                stats.failed,
            )
    finally:
        client.close()
    return stats


def hash_batch(batch: Sequence[ClaimedFile], config: WorkerConfig) -> List[HashResult]:
    max_workers = max(1, config.workers)
    if max_workers == 1:
        return [hash_one_file(row, config) for row in batch]

    results: List[HashResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(hash_one_file, row, config) for row in batch]
        for future in as_completed(futures):
            results.append(future.result())
    return results


def hash_one_file(row: ClaimedFile, config: WorkerConfig) -> HashResult:
    resolved_path = map_path(row.path, config.path_prefix_from, config.path_prefix_to)
    now = utc_now()
    try:
        path = Path(resolved_path)
        stat_result = path.stat()
        if not path.is_file():
            return HashResult(row.row_id, None, config.hash_algo, "failed", "path is not a regular file", None, now)

        hasher = make_hasher(config.hash_algo)
        with path.open("rb") as file_obj:
            while True:
                chunk = file_obj.read(config.read_chunk_bytes)
                if not chunk:
                    break
                hasher.update(chunk)
        return HashResult(row.row_id, hasher.hexdigest(), config.hash_algo, "hashed", None, stat_result.st_size, now)
    except FileNotFoundError as exc:
        return HashResult(row.row_id, None, config.hash_algo, "missing", str(exc), None, now)
    except OSError as exc:
        return HashResult(row.row_id, None, config.hash_algo, "failed", str(exc), None, now)


def make_hasher(hash_algo: str):
    normalized = hash_algo.casefold()
    if normalized == "sha256":
        return hashlib.sha256()
    if normalized == "blake3":
        from blake3 import blake3

        return blake3()
    raise ValueError(f"Unsupported HASH_ALGO `{hash_algo}`")


def ensure_hash_available(hash_algo: str) -> None:
    hasher = make_hasher(hash_algo)
    hasher.update(b"")


def map_path(path: str, prefix_from: str, prefix_to: str) -> str:
    if prefix_from and path.startswith(prefix_from):
        return f"{prefix_to}{path[len(prefix_from):]}"
    return path


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SqliteClient:
    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self.table = quote_table(config.table_name, '"')
        self.id_column = quote_identifier(config.id_column, '"')
        self.path_column = quote_identifier(config.path_column, '"')
        self.hash_column = quote_identifier(config.hash_column, '"')
        self.algo_column = quote_identifier(config.algo_column, '"')
        self.status_column = quote_identifier(config.status_column, '"')
        self.error_column = quote_identifier(config.error_column, '"')
        self.size_column = quote_identifier(config.size_column, '"')
        self.updated_at_column = quote_identifier(config.updated_at_column, '"')
        self.claim_started_at = utc_now()
        db_path = sqlite_path_from_url(config.db_url)
        self.connection = sqlite3.connect(db_path, timeout=60)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")

    def ensure_columns(self) -> None:
        existing = self._existing_columns()
        column_definitions = {
            self.config.hash_column: "TEXT",
            self.config.algo_column: "TEXT",
            self.config.status_column: "TEXT",
            self.config.error_column: "TEXT",
            self.config.size_column: "INTEGER",
            self.config.updated_at_column: "TEXT",
        }
        for column, column_type in column_definitions.items():
            if column not in existing:
                self.connection.execute(
                    f"ALTER TABLE {self.table} ADD COLUMN {quote_identifier(column, '\"')} {column_type}"
                )
        self.connection.commit()

    def reset_processing(self) -> int:
        cursor = self.connection.execute(
            f"""
            UPDATE {self.table}
            SET {self.status_column} = 'pending',
                {self.error_column} = NULL,
                {self.updated_at_column} = ?
            WHERE {self.status_column} = 'processing'
            """,
            (utc_now(),),
        )
        self.connection.commit()
        return int(cursor.rowcount)

    def claim_batch(self, limit: int) -> List[ClaimedFile]:
        self.connection.execute("BEGIN IMMEDIATE")
        rows = self.connection.execute(self._claim_select_sql(), self._claim_select_params(limit)).fetchall()
        claimed = [
            ClaimedFile(row_id=row[self.config.id_column], path=str(row[self.config.path_column]))
            for row in rows
        ]
        if claimed:
            self.connection.execute(
                f"""
                UPDATE {self.table}
                SET {self.status_column} = 'processing',
                    {self.error_column} = NULL,
                    {self.updated_at_column} = ?
                WHERE {self.id_column} IN ({placeholders(len(claimed), '?')})
                """,
                (utc_now(), *[row.row_id for row in claimed]),
            )
        self.connection.commit()
        return claimed

    def update_results(self, results: Sequence[HashResult]) -> None:
        if not results:
            return
        self.connection.executemany(
            f"""
            UPDATE {self.table}
            SET {self.hash_column} = ?,
                {self.algo_column} = ?,
                {self.status_column} = ?,
                {self.error_column} = ?,
                {self.size_column} = ?,
                {self.updated_at_column} = ?
            WHERE {self.id_column} = ?
            """,
            [
                (
                    result.content_hash,
                    result.hash_algo,
                    result.status,
                    result.error,
                    result.size_bytes,
                    result.updated_at,
                    result.row_id,
                )
                for result in results
            ],
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def _existing_columns(self) -> set[str]:
        table_for_pragma = quote_identifier(self.config.table_name, '"')
        rows = self.connection.execute(f"PRAGMA table_info({table_for_pragma})").fetchall()
        return {str(row["name"]) for row in rows}

    def _claim_select_sql(self) -> str:
        return f"""
            SELECT {self.id_column} AS {quote_identifier(self.config.id_column, '"')},
                   {self.path_column} AS {quote_identifier(self.config.path_column, '"')}
            FROM {self.table}
            WHERE {self._eligible_where_sql('?')}
            ORDER BY {self.id_column}
            LIMIT ?
        """

    def _claim_select_params(self, limit: int) -> tuple:
        return (*self._eligible_status_params(), limit)

    def _eligible_where_sql(self, placeholder: str) -> str:
        status_sql = eligible_status_sql(
            status_column=self.status_column,
            updated_at_column=self.updated_at_column,
            placeholder=placeholder,
            claim_statuses=self.config.claim_statuses,
        )
        where = (
            f"({self.hash_column} IS NULL OR {self.hash_column} = '') "
            f"AND ({status_sql})"
        )
        if self.config.where_sql:
            where = f"({where}) AND ({self.config.where_sql})"
        return where

    def _eligible_status_params(self) -> tuple:
        return eligible_status_params(self.config.claim_statuses, self.claim_started_at)


class MysqlClient:
    def __init__(self, config: WorkerConfig) -> None:
        try:
            import pymysql
            import pymysql.cursors
        except ImportError as exc:
            raise RuntimeError("MySQL support requires the `pymysql` package") from exc

        self.config = config
        self.pymysql = pymysql
        self.table = quote_table(config.table_name, "`")
        self.id_column = quote_identifier(config.id_column, "`")
        self.path_column = quote_identifier(config.path_column, "`")
        self.hash_column = quote_identifier(config.hash_column, "`")
        self.algo_column = quote_identifier(config.algo_column, "`")
        self.status_column = quote_identifier(config.status_column, "`")
        self.error_column = quote_identifier(config.error_column, "`")
        self.size_column = quote_identifier(config.size_column, "`")
        self.updated_at_column = quote_identifier(config.updated_at_column, "`")
        self.claim_started_at = utc_now()
        self.connection = self._connect()

    def ensure_columns(self) -> None:
        existing = self._existing_columns()
        column_definitions = {
            self.config.hash_column: "VARCHAR(128)",
            self.config.algo_column: "VARCHAR(32)",
            self.config.status_column: "VARCHAR(32)",
            self.config.error_column: "TEXT",
            self.config.size_column: "BIGINT",
            self.config.updated_at_column: "VARCHAR(40)",
        }
        with self.connection.cursor() as cursor:
            for column, column_type in column_definitions.items():
                if column not in existing:
                    cursor.execute(
                        f"ALTER TABLE {self.table} ADD COLUMN {quote_identifier(column, '`')} {column_type}"
                    )
        self.connection.commit()

    def reset_processing(self) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE {self.table}
                SET {self.status_column} = 'pending',
                    {self.error_column} = NULL,
                    {self.updated_at_column} = %s
                WHERE {self.status_column} = 'processing'
                """,
                (utc_now(),),
            )
            rowcount = cursor.rowcount
        self.connection.commit()
        return int(rowcount)

    def claim_batch(self, limit: int) -> List[ClaimedFile]:
        self.connection.begin()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(self._claim_select_sql(), self._claim_select_params(limit))
                rows = cursor.fetchall()
                claimed = [
                    ClaimedFile(row_id=row[self.config.id_column], path=str(row[self.config.path_column]))
                    for row in rows
                ]
                if claimed:
                    cursor.execute(
                        f"""
                        UPDATE {self.table}
                        SET {self.status_column} = 'processing',
                            {self.error_column} = NULL,
                            {self.updated_at_column} = %s
                        WHERE {self.id_column} IN ({placeholders(len(claimed), '%s')})
                        """,
                        (utc_now(), *[row.row_id for row in claimed]),
                    )
            self.connection.commit()
            return claimed
        except Exception:
            self.connection.rollback()
            raise

    def update_results(self, results: Sequence[HashResult]) -> None:
        if not results:
            return
        with self.connection.cursor() as cursor:
            cursor.executemany(
                f"""
                UPDATE {self.table}
                SET {self.hash_column} = %s,
                    {self.algo_column} = %s,
                    {self.status_column} = %s,
                    {self.error_column} = %s,
                    {self.size_column} = %s,
                    {self.updated_at_column} = %s
                WHERE {self.id_column} = %s
                """,
                [
                    (
                        result.content_hash,
                        result.hash_algo,
                        result.status,
                        result.error,
                        result.size_bytes,
                        result.updated_at,
                        result.row_id,
                    )
                    for result in results
                ],
            )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def _connect(self):
        parsed = parse_mysql_url(self.config.db_url)
        return self.pymysql.connect(
            host=parsed["host"],
            port=parsed["port"],
            user=parsed["user"],
            password=parsed["password"],
            database=parsed["database"],
            charset=parsed["charset"],
            autocommit=False,
            cursorclass=self.pymysql.cursors.DictCursor,
        )

    def _existing_columns(self) -> set[str]:
        with self.connection.cursor() as cursor:
            cursor.execute(f"SHOW COLUMNS FROM {self.table}")
            return {str(row["Field"]) for row in cursor.fetchall()}

    def _claim_select_sql(self) -> str:
        return f"""
            SELECT {self.id_column} AS {quote_identifier(self.config.id_column, '`')},
                   {self.path_column} AS {quote_identifier(self.config.path_column, '`')}
            FROM {self.table}
            WHERE {self._eligible_where_sql('%s')}
            ORDER BY {self.id_column}
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        """

    def _claim_select_params(self, limit: int) -> tuple:
        return (*self._eligible_status_params(), limit)

    def _eligible_where_sql(self, placeholder: str) -> str:
        status_sql = eligible_status_sql(
            status_column=self.status_column,
            updated_at_column=self.updated_at_column,
            placeholder=placeholder,
            claim_statuses=self.config.claim_statuses,
        )
        where = (
            f"({self.hash_column} IS NULL OR {self.hash_column} = '') "
            f"AND ({status_sql})"
        )
        if self.config.where_sql:
            where = f"({where}) AND ({self.config.where_sql})"
        return where

    def _eligible_status_params(self) -> tuple:
        return eligible_status_params(self.config.claim_statuses, self.claim_started_at)


def create_database_client(config: WorkerConfig) -> DatabaseClient:
    if config.db_type == "sqlite":
        return SqliteClient(config)
    if config.db_type == "mysql":
        return MysqlClient(config)
    raise ValueError("DB_TYPE must be `sqlite` or `mysql`")


def parse_config(argv: Optional[Sequence[str]] = None) -> WorkerConfig:
    parser = argparse.ArgumentParser(description="Hash files referenced by a SQL table.")
    parser.add_argument("--db-type", default=os.getenv("DB_TYPE", "sqlite").casefold())
    parser.add_argument("--db-url", default=os.getenv("DB_URL", ""))
    parser.add_argument("--table-name", default=os.getenv("TABLE_NAME", ""))
    parser.add_argument("--id-column", default=os.getenv("ID_COLUMN", ""))
    parser.add_argument("--path-column", default=os.getenv("PATH_COLUMN", ""))
    parser.add_argument("--hash-column", default=os.getenv("HASH_COLUMN", "content_hash"))
    parser.add_argument("--algo-column", default=os.getenv("HASH_ALGO_COLUMN", "content_hash_algo"))
    parser.add_argument("--status-column", default=os.getenv("HASH_STATUS_COLUMN", "content_hash_status"))
    parser.add_argument("--error-column", default=os.getenv("HASH_ERROR_COLUMN", "content_hash_error"))
    parser.add_argument("--size-column", default=os.getenv("HASH_SIZE_BYTES_COLUMN", "content_hash_size_bytes"))
    parser.add_argument("--updated-at-column", default=os.getenv("HASH_UPDATED_AT_COLUMN", "content_hash_updated_at"))
    parser.add_argument("--hash-algo", default=os.getenv("HASH_ALGO", "blake3").casefold())
    parser.add_argument("--batch-size", type=int, default=env_int("BATCH_SIZE", 1000))
    parser.add_argument("--workers", type=int, default=env_int("WORKERS", default_workers()))
    parser.add_argument("--read-chunk-bytes", type=int, default=env_int("READ_CHUNK_BYTES", 8 * 1024 * 1024))
    parser.add_argument("--max-rows", type=int, default=env_int("MAX_ROWS", 0))
    parser.add_argument("--ensure-columns", dest="ensure_columns", action="store_true", default=env_bool("ENSURE_COLUMNS", True))
    parser.add_argument("--no-ensure-columns", dest="ensure_columns", action="store_false")
    parser.add_argument("--reset-processing-on-start", action="store_true", default=env_bool("RESET_PROCESSING_ON_START", False))
    parser.add_argument("--path-prefix-from", default=os.getenv("PATH_PREFIX_FROM", ""))
    parser.add_argument("--path-prefix-to", default=os.getenv("PATH_PREFIX_TO", ""))
    parser.add_argument("--where-sql", default=os.getenv("WHERE_SQL", ""))
    parser.add_argument("--claim-statuses", default=os.getenv("CLAIM_STATUSES", "pending"))
    args = parser.parse_args(argv)

    return WorkerConfig(
        db_type=args.db_type.casefold(),
        db_url=args.db_url,
        table_name=args.table_name,
        id_column=args.id_column,
        path_column=args.path_column,
        hash_column=args.hash_column,
        algo_column=args.algo_column,
        status_column=args.status_column,
        error_column=args.error_column,
        size_column=args.size_column,
        updated_at_column=args.updated_at_column,
        hash_algo=args.hash_algo.casefold(),
        batch_size=args.batch_size,
        workers=args.workers,
        read_chunk_bytes=args.read_chunk_bytes,
        max_rows=args.max_rows,
        ensure_columns=args.ensure_columns,
        reset_processing_on_start=args.reset_processing_on_start,
        path_prefix_from=args.path_prefix_from,
        path_prefix_to=args.path_prefix_to,
        where_sql=args.where_sql,
        claim_statuses=tuple(status.strip() for status in args.claim_statuses.split(",") if status.strip()),
    )


def validate_config(config: WorkerConfig) -> None:
    if config.db_type not in {"sqlite", "mysql"}:
        raise ValueError("DB_TYPE must be `sqlite` or `mysql`")
    if not config.db_url:
        raise ValueError("DB_URL is required")
    validate_table_name(config.table_name)
    for column in (
        config.id_column,
        config.path_column,
        config.hash_column,
        config.algo_column,
        config.status_column,
        config.error_column,
        config.size_column,
        config.updated_at_column,
    ):
        validate_identifier(column)
    if config.batch_size < 1:
        raise ValueError("BATCH_SIZE must be >= 1")
    if config.workers < 1:
        raise ValueError("WORKERS must be >= 1")
    if config.read_chunk_bytes < 1:
        raise ValueError("READ_CHUNK_BYTES must be >= 1")
    if config.max_rows < 0:
        raise ValueError("MAX_ROWS must be >= 0")
    if not config.claim_statuses:
        raise ValueError("CLAIM_STATUSES must contain at least one status")


def validate_table_name(table_name: str) -> None:
    if not table_name:
        raise ValueError("TABLE_NAME is required")
    for part in table_name.split("."):
        validate_identifier(part)


def validate_identifier(identifier: str) -> None:
    if not identifier or not IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Unsafe SQL identifier `{identifier}`")


def quote_identifier(identifier: str, quote: str) -> str:
    validate_identifier(identifier)
    return f"{quote}{identifier}{quote}"


def quote_table(table_name: str, quote: str) -> str:
    validate_table_name(table_name)
    return ".".join(quote_identifier(part, quote) for part in table_name.split("."))


def placeholders(count: int, marker: str) -> str:
    return ", ".join([marker] * count)


def eligible_status_sql(
    *,
    status_column: str,
    updated_at_column: str,
    placeholder: str,
    claim_statuses: Sequence[str],
) -> str:
    unique_statuses = tuple(dict.fromkeys(claim_statuses))
    clauses = [f"{status_column} IS NULL"]
    if "pending" in unique_statuses:
        clauses.append(f"{status_column} = {placeholder}")
    retry_statuses = [status for status in unique_statuses if status != "pending"]
    if retry_statuses:
        clauses.append(
            f"({status_column} IN ({placeholders(len(retry_statuses), placeholder)}) "
            f"AND ({updated_at_column} IS NULL OR {updated_at_column} < {placeholder}))"
        )
    return " OR ".join(clauses)


def eligible_status_params(claim_statuses: Sequence[str], claim_started_at: str) -> tuple:
    unique_statuses = tuple(dict.fromkeys(claim_statuses))
    params: list[str] = []
    if "pending" in unique_statuses:
        params.append("pending")
    retry_statuses = [status for status in unique_statuses if status != "pending"]
    if retry_statuses:
        params.extend(retry_statuses)
        params.append(claim_started_at)
    return tuple(params)


def sqlite_path_from_url(db_url: str) -> str:
    if db_url == ":memory:":
        return db_url
    if db_url.startswith("sqlite:///"):
        return db_url[len("sqlite:///"):]
    if db_url.startswith("sqlite://"):
        return db_url[len("sqlite://"):]
    return db_url


def parse_mysql_url(db_url: str) -> dict:
    url = db_url.replace("mysql+pymysql://", "mysql://", 1)
    parsed = urlparse(url)
    if parsed.scheme != "mysql":
        raise ValueError("MySQL DB_URL must start with mysql:// or mysql+pymysql://")
    if not parsed.hostname:
        raise ValueError("MySQL DB_URL is missing host")
    database = parsed.path.lstrip("/")
    if not database:
        raise ValueError("MySQL DB_URL is missing database name")
    query = parse_qs(parsed.query)
    return {
        "host": parsed.hostname,
        "port": parsed.port or 3306,
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "database": database,
        "charset": query.get("charset", ["utf8mb4"])[0],
    }


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value not in {None, ""} else default


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value in {None, ""}:
        return default
    return value.casefold() in {"1", "true", "yes", "on"}


def default_workers() -> int:
    return min(64, max(4, (os.cpu_count() or 1) * 4))


if __name__ == "__main__":
    sys.exit(main())
