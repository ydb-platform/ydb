import logging

from ydb.tests.olap.s3_import.base import S3ImportTestBase

logger = logging.getLogger(__name__)


class TestS3TpchImport(S3ImportTestBase):
    def validate_table(self, table_name: str):
        logger.info(f"Validation of {table_name}...")

        result_sets = self.ydb_client.query(f"""
            SELECT
                String::Hex(Sum(Digest::MurMurHash32(Pickle(TableRow())))) AS check_hash,
                COUNT(*) AS check_size
            FROM {table_name};

            $initial_table = SELECT
                l_linenumber,
                l_orderkey,
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
            FROM lineitem;
            SELECT
                String::Hex(Sum(Digest::MurMurHash32(Pickle(TableRow())))) AS lineitem_hash,
                COUNT(*) AS lineitem_size
            FROM $initial_table;
        """)

        check_result = result_sets[0].rows[0]
        assert check_result.check_size > 0

        lineitem_result = result_sets[1].rows[0]
        assert check_result.check_size == lineitem_result.lineitem_size
        assert check_result.check_hash == lineitem_result.lineitem_hash

    def test_import_and_export(self):
        test_bucket = "test_import_and_export_bucket"
        self.s3_client.create_bucket(test_bucket)

        access_key_id_secret_name = f"{test_bucket}_key_id"
        access_key_secret_secret_name = f"{test_bucket}_key_secret"
        self.ydb_client.query(f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'")
        self.ydb_client.query(f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'")

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE s3_source WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "{self.s3_mock.endpoint}/{test_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
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
        self.ydb_client.run_cli_comand(["workload", "tpch", "init", "--datetime", "--store", "column"])
        self.ydb_client.run_cli_comand(["workload", "tpch", "import", "generator", "--scale", "1"])

        logger.info("Exporting into s3...")
        self.ydb_client.query("INSERT INTO s3_table SELECT * FROM lineitem")
        logger.info(f"Exporting finished, bucket stats: {self.s3_client.get_bucket_stat(test_bucket)}")
        self.validate_table("s3_table")

        logger.info("Importing into ydb...")
        self.ydb_client.query("""
            CREATE TABLE from_s3 (
                PRIMARY KEY (l_orderkey, l_linenumber)
            ) WITH (
                STORE = COLUMN
            ) AS SELECT * FROM s3_table
        """)
        self.validate_table("from_s3")
