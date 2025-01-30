suffixes:=$(shell jot 20)

$(suffixes:=.test.dst): %.test.dst:
	../../../../ya test    --build=relwithdebinfo    --test-disable-timeout    --test-param ydb-endpoint=$(YDB_ENDPOINT)    --test-param ydb-db=$(YDB_DB)    --test-param tables-path=scenario    --test-param s3-endpoint=http://storage.yandexcloud.net    --test-param s3-access-key=$(S3_ACCESS_KEY)    --test-param s3-secret-key=$(S3_SECRET_KEY)    --test-param s3-buckets=ydb-test-test,ydb-test-test-2    --test-param test-duration-seconds=2400 --test-param table_suffix=$* --test-param rows_count=100 --test-param batches_count=1000 --test-param reuse-tables=True --test-param keep-tables=True --test-param tables_count=10 --test-param ignore_read_errors=True -F test_insert.py::TestInsert::test[read_data_during_bulk_upsert]

all.test.dst: $(suffixes:=.test.dst)
