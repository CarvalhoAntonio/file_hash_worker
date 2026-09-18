# File Hash Worker

Small batch worker that reads file paths from a SQL table, hashes file bytes, and writes the hash/status back to the same table.

Supported databases:

- SQLite
- MySQL

Default hash algorithm is `blake3` for speed. Use `HASH_ALGO=sha256` if you need a standard library hash.

## Required Configuration

```bash
DB_TYPE=sqlite|mysql
DB_URL=...
TABLE_NAME=documents
ID_COLUMN=id
PATH_COLUMN=file_path
```

The worker can create these columns when `ENSURE_COLUMNS=true`:

```text
content_hash
content_hash_algo
content_hash_status
content_hash_error
content_hash_size_bytes
content_hash_updated_at
```

Statuses:

```text
processing
hashed
missing
failed
```

## Docker Examples

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

MySQL parallel workers require MySQL 8+ because row claiming uses `FOR UPDATE SKIP LOCKED`.

If database paths do not match container paths, map prefixes:

```bash
-e PATH_PREFIX_FROM=/host/source/root \
-e PATH_PREFIX_TO=/data
```

## Performance Options

```bash
HASH_ALGO=blake3
BATCH_SIZE=1000
WORKERS=16
READ_CHUNK_BYTES=8388608
MAX_ROWS=0
```

For multiple machines, split work with a trusted SQL predicate:

```bash
WHERE_SQL='id >= 0 AND id < 1000000'
```

or by path:

```bash
WHERE_SQL="file_path LIKE '/data/shard_01/%'"
```

`WHERE_SQL` is inserted into SQL as-is, so only use trusted values.

## Resume

Rerun the same command. Rows with `content_hash_status='hashed'` are skipped.

If a worker crashed and left rows in `processing`, reset them:

```bash
RESET_PROCESSING_ON_START=true
```

Retry only pending rows by default:

```bash
CLAIM_STATUSES=pending
```

To retry rows from previous failed runs, include those statuses:

```bash
CLAIM_STATUSES=pending,failed,processing
```
