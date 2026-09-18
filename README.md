# File Hash Worker

Drop this on a machine that has Docker, a database, and access to the files.

It reads file paths from a SQL table, hashes the raw file bytes, and writes the result back to the same row.

Supported DBs:

- SQLite
- MySQL

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

Build:

```bash
docker build -t file_hash_worker:latest .
```

Load from airgap image tar:

```bash
docker load -i file_hash_worker_image_dev_antonio.tar
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

If DB paths are host paths but the container sees files under `/data`:

```bash
-e PATH_PREFIX_FROM=/real/files \
-e PATH_PREFIX_TO=/data
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
```
