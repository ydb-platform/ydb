import logging

from ydb.tests.olap.s3_import.base import S3ImportTestBase

logger = logging.getLogger(__name__)


class TestS3TpchImport(S3ImportTestBase):
    _LINEITEM_COLUMNS = """
        l_linenumber, l_orderkey,
        l_comment, l_commitdate, l_discount, l_extendedprice,
        l_linestatus, l_partkey, l_quantity, l_receiptdate,
        l_returnflag, l_shipdate, l_shipinstruct, l_shipmode,
        l_suppkey, l_tax
    """

    _LINEITEM_COLUMNS_OPTIONAL = """
        l_linenumber, l_orderkey,
        JUST(l_comment) AS l_comment,
        JUST(l_commitdate) AS l_commitdate,
        JUST(l_discount) AS l_discount,
        JUST(l_extendedprice) AS l_extendedprice,
        JUST(l_linestatus) AS l_linestatus,
        JUST(l_partkey) AS l_partkey,
        JUST(l_quantity) AS l_quantity,
        JUST(l_receiptdate) AS l_receiptdate,
        JUST(l_returnflag) AS l_returnflag,
        JUST(l_shipdate) AS l_shipdate,
        JUST(l_shipinstruct) AS l_shipinstruct,
        JUST(l_shipmode) AS l_shipmode,
        JUST(l_suppkey) AS l_suppkey,
        JUST(l_tax) AS l_tax
    """

    def _table_stats(self, table_name: str, with_hash: bool = True, optional_payload: bool = False):
        if with_hash:
            columns = self._LINEITEM_COLUMNS_OPTIONAL if optional_payload else self._LINEITEM_COLUMNS
            query = f"""
                $t = SELECT {columns} FROM {table_name};
                SELECT
                    String::Hex(Sum(Digest::MurMurHash32(Pickle(TableRow())))) AS hash,
                    COUNT(*) AS size
                FROM $t;
            """
        else:
            query = f"SELECT COUNT(*) AS size FROM {table_name};"
        return self.ydb_client.query(query)[0].rows[0]

    def validate_table(self, table_name: str, expected_size: int, expected_hash=None):
        logger.info(f"Validation of {table_name}...")
        result = self._table_stats(table_name, with_hash=expected_hash is not None)
        assert result.size > 0
        assert result.size == expected_size, \
            f"Row count mismatch: {table_name} has {result.size}, expected {expected_size}"
        if expected_hash is not None:
            assert result.hash == expected_hash, \
                f"Hash mismatch: {table_name} hash={result.hash}, expected {expected_hash}"

    def test_import_and_export(self):
        test_bucket = "test_import_and_export_bucket"
        self.s3_client.create_bucket(test_bucket)

        access_key_id_secret_name = f"{test_bucket}_key_id"
        access_key_secret_secret_name = f"{test_bucket}_key_secret"
        self.ydb_client.query(f"CREATE SECRET {access_key_id_secret_name} WITH (value='{self.s3_client.key_id}')")
        self.ydb_client.query(f"CREATE SECRET {access_key_secret_secret_name} WITH (value='{self.s3_client.key_secret}')")

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE s3_source WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "{self.s3_mock.endpoint}/{test_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_PATH="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_PATH="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            );

            CREATE EXTERNAL TABLE s3_table (
                l_linenumber Int32 NOT NULL,
                l_orderkey Int64 NOT NULL,
                l_comment Utf8,
                l_commitdate Date,
                l_discount Double,
                l_extendedprice Double,
                l_linestatus Utf8,
                l_partkey Int64,
                l_quantity Double,
                l_receiptdate Date,
                l_returnflag Utf8,
                l_shipdate Date,
                l_shipinstruct Utf8,
                l_shipmode Utf8,
                l_suppkey Int64,
                l_tax Double
            ) WITH (
                DATA_SOURCE="s3_source",
                LOCATION="/test_folder/",
                FORMAT="parquet"
            );
        """)

        logger.info("Creating tpc-h tables...")
        self.ydb_client.run_cli_comand(["workload", "tpch", "init", "--datetime-types=dt32", "--store", "column"])
        self.ydb_client.run_cli_comand(["workload", "tpch", "import", "generator", "--scale", "1"])

        lineitem = self._table_stats("lineitem", with_hash=True, optional_payload=True)
        logger.info(f"Lineitem reference: size={lineitem.size}, hash={lineitem.hash}")

        logger.info("Exporting into s3...")
        self.ydb_client.query("INSERT INTO s3_table SELECT * FROM lineitem")
        logger.info(f"Exporting finished, bucket stats: {self.s3_client.get_bucket_stat(test_bucket)}")
        self.validate_table("s3_table", expected_size=lineitem.size)

        logger.info("Importing into ydb...")
        self.ydb_client.query("""
            CREATE TABLE from_s3 (
                PRIMARY KEY (l_orderkey, l_linenumber)
            ) WITH (
                STORE = COLUMN
            ) AS SELECT * FROM s3_table
        """)
        self.validate_table("from_s3", expected_size=lineitem.size, expected_hash=lineitem.hash)
