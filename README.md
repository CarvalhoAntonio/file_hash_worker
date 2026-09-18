# File Hash Worker

Drop this on a machine that has Docker, a database, and access to the files.

It reads file paths from a SQL table, hashes the raw file bytes, and writes the result back to the same row.

Supported DBs:

- SQLite
- MySQL
- Microsoft SQL Server

It adds these columns if missing:

```text
content_hash
content_hash_algo
content_hash_status
content_hash_error
content_hash_size_bytes
content_hash_updated_at
```

## Run

Load from airgap image tar:

```bash
docker load -i file_hash_worker.tar
```

Or build locally:

```bash
docker build -t file_hash_worker:latest .
```

SQLite:

```bash
docker run --rm \
  -v /real/files:/data:ro \
  -v /real/db:/db \
  -e DB_TYPE=sqlite \
  -e DB_URL='sqlite:////db/files.sqlite' \
  -e TABLE_NAME=documents \
  -e ID_COLUMN=id \
  -e PATH_COLUMN=file_path \
  file_hash_worker:latest
```

MySQL:

```bash
docker run --rm \
  -v /real/files:/data:ro \
  -e DB_TYPE=mysql \
  -e DB_URL='mysql+pymysql://user:pass@mysql-host:3306/mydb' \
  -e TABLE_NAME=documents \
  -e ID_COLUMN=id \
  -e PATH_COLUMN=file_path \
  file_hash_worker:latest
```

Microsoft SQL Server:

```bash
docker run --rm \
  -v /real/files:/data:ro \
  -e DB_TYPE=mssql \
  -e DB_URL='mssql+pymssql://user:pass@sql-host:1433/mydb' \
  -e TABLE_NAME=dbo.documents \
  -e ID_COLUMN=id \
  -e PATH_COLUMN=file_path \
  file_hash_worker:latest
```

If DB paths are host paths but the container sees files under `/data`:

```bash
-e PATH_PREFIX_FROM=/real/files \
-e PATH_PREFIX_TO=/data
```

If the DB path is wrong and you have a CSV mapping wrong path -> real path:

```bash
-v /real/mapping:/mapping:ro \
-e PATH_MAPPING_CSV=/mapping/paths.csv \
-e PATH_MAPPING_KEY_COLUMN=wrong_path \
-e PATH_MAPPING_VALUE_COLUMN=real_path
```

Default hash is `blake3`. Use `HASH_ALGO=sha256` if needed.

Rerun the same command to resume. Rows already marked `hashed` are skipped.

## Useful Knobs

```text
WORKERS=16
BATCH_SIZE=1000
HASH_ALGO=blake3
RESET_PROCESSING_ON_START=true
WHERE_SQL='id >= 0 AND id < 1000000'
MAX_ROWS=0
CLAIM_STATUSES=pending
PATH_MAPPING_CSV=/mapping/paths.csv
```

- `WORKERS`: how many files to hash at the same time.
- `BATCH_SIZE`: how many DB rows to claim/update per loop.
- `HASH_ALGO`: `blake3` is fast; `sha256` is slower but more standard.
- `RESET_PROCESSING_ON_START`: moves stuck `processing` rows back to `pending` after a crash.
- `WHERE_SQL`: extra filter to split work, for example by ID range or path.
- `MAX_ROWS`: stop after this many rows; `0` means no limit.
- `CLAIM_STATUSES`: which row statuses are allowed to be picked up.
- `PATH_MAPPING_CSV`: optional CSV that maps the DB path to the real file path.
