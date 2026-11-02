#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

import pytest

import ydb.public.api.protos.ydb_value_pb2 as ydb

import ydb.tests.fq.s3.s3_helpers as s3_helpers


class TestS3Formats:
    def create_bucket_and_upload_file(self, filename, s3, kikimr):
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", "ydb/tests/fq/s3/test_format_data")

    def create_bucket_and_upload_file_body(
        self, body, object_key, s3, kikimr, content_type="text/plain", bucket_name="fbucket"
    ):
        s3_helpers.create_bucket_and_put_object(s3.s3_url, bucket_name, object_key, body, content_type)

    def validate_result(self, result_set):
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.type_id == ydb.Type.INT32
        assert result_set.columns[2].name == "Weight"
        assert result_set.columns[2].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 3
        assert result_set.rows[0][0] == b"Banana"
        assert result_set.rows[0][1] == 3
        assert result_set.rows[0][2] == 100
        assert result_set.rows[1][0] == b"Apple"
        assert result_set.rows[1][1] == 2
        assert result_set.rows[1][2] == 22
        assert result_set.rows[2][0] == b"Pear"
        assert result_set.rows[2][1] == 15
        assert result_set.rows[2][2] == 33

    def validate_pg_result(self, result_set):
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.pg_type.oid == 25
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.pg_type.oid == 23
        assert result_set.columns[2].name == "Weight"
        assert result_set.columns[2].type.pg_type.oid == 20
        assert len(result_set.rows) == 3
        assert result_set.rows[0][0] == "Banana"
        assert result_set.rows[0][1] == "3"
        assert result_set.rows[0][2] == "100"
        assert result_set.rows[1][0] == "Apple"
        assert result_set.rows[1][1] == "2"
        assert result_set.rows[1][2] == "22"
        assert result_set.rows[2][0] == "Pear"
        assert result_set.rows[2][1] == "15"
        assert result_set.rows[2][2] == "33"

    @pytest.mark.parametrize(
        "filename, type_format",
        [
            ("test.csv", "csv_with_names"),
            ("test.tsv", "tsv_with_names"),
            ("test_each_row.json", "json_each_row"),
            ("test_list.json", "json_list"),
            ("test.parquet", "parquet"),
        ],
    )
    def test_format(self, kikimr, s3, client, filename, type_format, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`{filename}`
            WITH (format=`{type_format}`, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ));
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        self.validate_result(result_set)

        assert query.metric('IngressBytes') > 0
        if type_format != "json_list":
            assert query.metric('IngressRows') == 3

    def test_btc(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file("btct.parquet", s3, kikimr)
        storage_connection_name = client.path(unique_prefix + "btct")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT
                *
            FROM `{storage_connection_name}`.`btct.parquet`
            WITH (format=`parquet`,
                SCHEMA=(
                    hash STRING,
                    version INT64,
                    size INT64,
                    block_hash UTF8,
                    block_number INT64,
                    virtual_size INT64,
                    lock_time INT64,
                    input_count INT64,
                    output_count INT64,
                    is_coinbase BOOL,
                    output_value DOUBLE,
                    block_timestamp DATETIME,
                    date DATE
                )
            );
            '''

        query = client.execute(sql)
        query.check(False)
        describe_result = query
        issues = describe_result.error
        assert "Error while reading file btct.parquet" in issues
        assert "File contains LIST field outputs and can\'t be parsed" in issues

    def test_invalid_format(self, kikimr, s3, client, unique_prefix):
        fruits = '''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        self.create_bucket_and_upload_file_body(fruits, "fruits.csv", s3, kikimr)

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=invalid_type_format, SCHEMA (
                Fruit String,
                Price Int,
                Weight Int
            ));
            '''

        query = client.execute(sql)
        query.check(False)
        describe_result = query
        logging.debug("Describe result: {}".format(describe_result))
        describe_string = "{}".format(describe_result)
        assert "Unknown format: invalid_type_format. Use one of: " in describe_string

    def test_invalid_input_compression(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file_body("blahblahblah", "fruits", s3, kikimr, bucket_name="ibucket")
        storage_connection_name = client.path(unique_prefix + "input_bucket")
        client.create_external_data_source(storage_connection_name, "ibucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`*`
            WITH (format=parquet, compression=abvgzip, SCHEMA (
                Fruit String,
                Price Int,
                Weight Int
            ));
            '''

        query = client.execute(sql)
        query.check(False)
        describe_result = query
        assert "External compression for parquet is not supported" in str(describe_result)

    def test_invalid_output_compression(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file_body("blahblahblah", "fruits", s3, kikimr, bucket_name="obucket")
        storage_connection_name = client.path(unique_prefix + "output_bucket")
        client.create_external_data_source(storage_connection_name, "obucket")

        sql = f'''
            INSERT INTO `{storage_connection_name}`.`path/` with (format=parquet, compression=abvgzip)
            SELECT *
            FROM `{storage_connection_name}`.`*`
            WITH (format=parquet, SCHEMA (
                Fruit String,
                Price Int,
                Weight Int
            ));
            '''

        query = client.execute(sql)
        query.check(False)
        describe_result = query
        assert "External compression for parquet is not supported" in str(describe_result)

    def test_custom_csv_delimiter_csv_with_names(self, kikimr, s3, client, unique_prefix):
        fruits = '''Fruit;Price;Weight
    Banana;3;100
    Apple;2;22
    Pear;15;33'''
        self.create_bucket_and_upload_file_body(fruits, "fruits.csv", s3, kikimr)

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
                SELECT *
                FROM `{storage_connection_name}`.`fruits.csv`
                WITH (
                    format = csv_with_names,
                    csv_delimiter = ";",
                    schema = (
                        Fruit String NOT NULL,
                        Price Int NOT NULL,
                        Weight Int NOT NULL
                    )
                );
                '''

        query = client.execute(sql)
        query.check()
        data = query.result_set()
        result_set = data
        self.validate_result(result_set)

    def test_custom_csv_delimiter_csv(self, kikimr, s3, client, unique_prefix):
        """Same as test_custom_csv_delimiter_csv_with_names but format=csv (no header row; SCHEMA defines order)."""
        fruits = '''Banana;3;100
Apple;2;22
Pear;15;33'''
        self.create_bucket_and_upload_file_body(fruits, "fruits_sep.csv", s3, kikimr)

        storage_connection_name = client.path(unique_prefix + "fruitbucket_csv")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
                SELECT *
                FROM `{storage_connection_name}`.`fruits_sep.csv`
                WITH (
                    format = csv,
                    csv_delimiter = ";",
                    schema = (
                        Fruit String NOT NULL,
                        Price Int NOT NULL,
                        Weight Int NOT NULL
                    )
                );
                '''

        query = client.execute(sql)
        query.check()
        data = query.result_set()
        result_set = data
        self.validate_result(result_set)

    def test_csv_delimiter_invalid_format_rejected(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file_body('{"time":"lunch"}\n', "one_row.jsonl", s3, kikimr)

        storage_connection_name = client.path(unique_prefix + "delimiterinvalidbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f"""
            SELECT *
            FROM `{storage_connection_name}`.`one_row.jsonl`
            WITH (
                format = json_each_row,
                csv_delimiter = ";",
                SCHEMA = (time String NOT NULL)
            );
            """

        query = client.execute(sql)
        query.check(False)
        describe_result = query
        describe_string = "{}".format(describe_result)
        assert "csv_delimiter can only be used with csv_with_names or csv format" in describe_string

    def test_csv_delimiter_must_be_single_character_rejected(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file_body("x\n", "one_col.csv", s3, kikimr)

        storage_connection_name = client.path(unique_prefix + "delimitersizebucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f"""
            SELECT *
            FROM `{storage_connection_name}`.`one_col.csv`
            WITH (
                format = csv,
                csv_delimiter = ";;",
                SCHEMA = (time String NOT NULL)
            );
            """

        query = client.execute(sql)
        query.check(False)
        describe_result = query
        describe_string = "{}".format(describe_result)
        assert "csv_delimiter must be single character" in describe_string

    def test_csv_with_names_projection_column_order(self, kikimr, s3, client, unique_prefix):
        """csv_with_names: header order b,a in file; SCHEMA lists a,b; SELECT a,b checks values by name.

        Locks ClickHouseClient.ParseFormat reading parsed columns by name (not only by position).
        """
        self.create_bucket_and_upload_file_body("b,a\nbb,aa\n", "two_cols.csv", s3, kikimr)

        storage_connection_name = client.path(unique_prefix + "twocolbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f"""
            SELECT a, b
            FROM `{storage_connection_name}`.`two_cols.csv`
            WITH (
                format = csv_with_names,
                SCHEMA = (
                    a String NOT NULL,
                    b String NOT NULL
                )
            );
            """

        query = client.execute(sql)
        query.check()
        data = query.result_set()
        result_set = data
        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "a"
        assert result_set.columns[1].name == "b"
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == b"aa"
        assert result_set.rows[0][1] == b"bb"

    def test_csv_projection_column_order(self, kikimr, s3, client, unique_prefix):
        """csv (no header row): SCHEMA fixes field order; SELECT b, a only reorders output columns."""
        self.create_bucket_and_upload_file_body("aa,bb", "headerless_two_cols.csv", s3, kikimr)

        storage_connection_name = client.path(unique_prefix + "headerless_twocolbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f"""
            SELECT b, a
            FROM `{storage_connection_name}`.`headerless_two_cols.csv`
            WITH (
                format = csv,
                SCHEMA = (
                    a String NOT NULL,
                    b String NOT NULL
                )
            );
            """

        query = client.execute(sql)
        query.check()
        data = query.result_set()
        result_set = data
        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "b"
        assert result_set.columns[1].name == "a"
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == b"bb"
        assert result_set.rows[0][1] == b"aa"

    def test_csv_empty_schema_rejected(self, kikimr, s3, client, unique_prefix):
        """Headerless csv: SCHEMA = () must be rejected (no column names to define field order in the file)."""
        self.create_bucket_and_upload_file_body("x", "empty_schema.csv", s3, kikimr)

        storage_connection_name = client.path(unique_prefix + "emptyschemabucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f"""
            SELECT *
            FROM `{storage_connection_name}`.`empty_schema.csv`
            WITH (
                format = csv,
                SCHEMA = ()
            );
            """

        query = client.execute(sql)
        query.check(False)
        describe_result = query
        describe_string = "{}".format(describe_result)
        assert (
            "csv format requires SCHEMA with explicitly listed column names to determine column order"
            in describe_string
        )

    def test_csv_projection_single_column(self, kikimr, s3, client, unique_prefix):
        """csv (no header): full SCHEMA (a,b) and two fields in file; SELECT only one column (b).

        Ensures columns_list / parse still maps file positions to names when output is a strict subset.
        """
        self.create_bucket_and_upload_file_body("aa,bb", "headerless_select_one_col.csv", s3, kikimr)

        storage_connection_name = client.path(unique_prefix + "headerless_onecolbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f"""
            SELECT b
            FROM `{storage_connection_name}`.`headerless_select_one_col.csv`
            WITH (
                format = csv,
                SCHEMA = (
                    a String NOT NULL,
                    b String NOT NULL
                )
            );
            """

        query = client.execute(sql)
        query.check()
        data = query.result_set()
        result_set = data
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "b"
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == b"bb"

    def test_csv_projection_column_order_non_alphabetical_schema(self, kikimr, s3, client, unique_prefix):
        """Same as test_csv_projection_column_order but SCHEMA lists b, a (not alphabetical a, b).

        First CSV field binds to b, second to a; SELECT a, b only reorders output columns.
        """
        self.create_bucket_and_upload_file_body("bb,aa", "headerless_b_a.csv", s3, kikimr)

        storage_connection_name = client.path(unique_prefix + "headerless_ba_bucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f"""
            SELECT a, b
            FROM `{storage_connection_name}`.`headerless_b_a.csv`
            WITH (
                format = csv,
                SCHEMA = (
                    b String NOT NULL,
                    a String NOT NULL
                )
            );
            """

        query = client.execute(sql)
        query.check()
        data = query.result_set()
        result_set = data
        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "a"
        assert result_set.columns[1].name == "b"
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == b"aa"
        assert result_set.rows[0][1] == b"bb"

    def test_no_not_nullable_column(self, kikimr, s3, client, unique_prefix):
        filename = "test_wrong_type.csv"
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`{filename}`
            WITH (format=`csv_with_names`, SCHEMA (
                Fruit String,
                AMOGUS String not null
            ));
            '''

        query = client.execute(sql)
        query.check(False)
        describe_result = query
        logging.debug("Describe result: {}".format(describe_result))
        describe_string = "{}".format(describe_result)
        assert (
            "Column `AMOGUS` is marked as not null, but was not found in the csv file" in describe_string
        ), describe_string

    def test_csv_with_names_header_missing_not_null_column(self, kikimr, s3, client, unique_prefix):
        """NOT NULL column from SCHEMA is absent from the file header (CSVRowInputFormat readPrefix)."""
        # Header lists only Fruit and Price; Weight is required by SCHEMA but not in the CSV header row.
        csv_body = """Fruit,Price
Banana,3
Apple,2
Pear,15
"""
        self.create_bucket_and_upload_file_body(csv_body, "fruits_missing_col.csv", s3, kikimr)

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits_missing_col.csv`
            WITH (format=`csv_with_names`, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ));
            '''

        query = client.execute(sql)
        query.check(False)
        describe_result = query
        describe_string = "{}".format(describe_result)
        logging.debug("Describe result: {}".format(describe_result))
        assert (
            "Column `Weight` is marked as not null, but was not found in the csv file" in describe_string
        ), describe_string

    def test_no_nullable_column(self, kikimr, s3, client, unique_prefix):
        filename = "test_wrong_type.csv"
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`{filename}`
            WITH (format=`csv_with_names`, SCHEMA (
                Fruit String,
                AMOGUS String
            ));
            '''

        query = client.execute(sql)
        query.check()
        data = query.result_set()
        result_set = data
        assert result_set.rows[0][0] is None

    def test_invalid_column_type_in_csv(self, kikimr, s3, client, unique_prefix):
        filename = "test_wrong_type.csv"
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql1 = f'''
            SELECT *
            FROM `{storage_connection_name}`.`{filename}`
            WITH (format=`csv_with_names`, SCHEMA (
                Fruit String,
                Owner Int
            ));
            '''

        sql2 = f'''
            SELECT *
            FROM `{storage_connection_name}`.`{filename}`
            WITH (format=`csv_with_names`, SCHEMA (
                Fruit Int,
                Owner String
            ));
            '''

        def check_result(query, corrupted_col_name):
            query.check(False)
            describe_result = query
            logging.debug("Describe result: {}".format(describe_result))
            describe_string = "{}".format(describe_result)
            assert (
                "failed to parse data in column `{}' from row 0, probably data type differs from specified in schema".format(
                    corrupted_col_name
                )
                in describe_string
            ), describe_string

        query1_id = client.execute(sql1)
        query2_id = client.execute(sql2)
        check_result(query1_id, "Owner")
        check_result(query2_id, "Fruit")

    def test_invalid_column_in_parquet(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file("test.parquet", s3, kikimr)
        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`test.parquet`
            WITH (format=parquet, SCHEMA (
                Fruit String,
                Price Int,
                ZZZZZ String NOT NULL
            ));
            '''

        query = client.execute(sql)
        query.check(False)
        describe_result = query
        logging.debug("Describe result: {}".format(describe_result))
        issues = describe_result.error
        assert '''Error while reading file test.parquet''' in issues
        assert "Missing field: ZZZZZ" in issues

    def test_missing_optional_column_in_parquet(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file("test.parquet", s3, kikimr)
        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT Fruit, Price, ZZZZZ, Missing
            FROM `{storage_connection_name}`.`test.parquet`
            WITH (format=parquet, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                ZZZZZ String?,
                Missing Json?
            ))
            ORDER BY Price;
            '''

        query = client.execute(sql)
        query.check()
        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 4
        assert result_set.columns[2].name == "ZZZZZ"
        assert result_set.columns[2].type.optional_type.item.type_id == ydb.Type.STRING
        assert result_set.columns[3].name == "Missing"
        assert result_set.columns[3].type.optional_type.item.type_id == ydb.Type.JSON
        assert len(result_set.rows) == 3
        assert result_set.rows[0][0] == b"Apple"
        assert result_set.rows[0][1] == 2
        assert result_set.rows[1][0] == b"Banana"
        assert result_set.rows[1][1] == 3
        assert result_set.rows[2][0] == b"Pear"
        assert result_set.rows[2][1] == 15
        for row in result_set.rows:
            assert row[2] is None
            assert row[3] is None

    def test_only_missing_optional_columns_in_parquet(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file("test.parquet", s3, kikimr)
        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT COUNT(*) AS cnt, COUNT(Missing) AS cnt_missing
            FROM `{storage_connection_name}`.`test.parquet`
            WITH (format=parquet, SCHEMA (
                Missing String?
            ));
            '''

        query = client.execute(sql)
        query.check()
        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == 3
        assert result_set.rows[0][1] == 0

    def test_simple_pg_types(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file("test.parquet", s3, kikimr)
        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`test.parquet`
            WITH (format=parquet, SCHEMA (
                Fruit pgtext,
                Price pgint4,
                Weight pgint8
            ));
            '''

        query = client.execute(sql)
        query.check()
        data = query.result_set()
        result_set = data
        self.validate_pg_result(result_set)

    def test_precompute(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file("test.parquet", s3, kikimr)
        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            $maxPrice = SELECT MAX(Price)
            FROM `{storage_connection_name}`.`test.parquet`
            WITH (format=`parquet`, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ));

            SELECT Fruit, Price
            FROM `{storage_connection_name}`.`test.parquet`
            WITH (format=`parquet`, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ))
            WHERE Price == $maxPrice;
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == b"Pear"
        assert result_set.rows[0][1] == 15

    def test_raw_empty_schema_query(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file("test.parquet", s3, kikimr)
        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")
        sql = f'''
            SELECT * FROM `{storage_connection_name}`.`*`
            WITH (format=raw, SCHEMA ());
            '''

        query = client.execute(sql)
        query.check(False)
        describe_result = query
        describe_string = "{}".format(describe_result)
        assert r"Only one column in schema supported in raw format" in describe_string

    def test_csv_no_header_three_rows(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file("test_no_header.csv", s3, kikimr)

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`test_no_header.csv`
            WITH (
                format = csv,
                SCHEMA (
                    Fruit String NOT NULL,
                    Price Int32 NOT NULL,
                    Weight Int32 NOT NULL
                )
            );
            '''

        query = client.execute(sql)
        query.check()
        data = query.result_set()
        result_set = data
        self.validate_result(result_set)

    def test_csv_no_header_select_price(self, kikimr, s3, client, unique_prefix):
        # Same file as test_csv_no_header_three_rows (3 columns, no header row).
        # Request a single column that is not the first in alphabetical order
        # (Fruit < Price < Weight тЖТ Price is the middle column).
        self.create_bucket_and_upload_file("test_no_header.csv", s3, kikimr)

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT Price
            FROM `{storage_connection_name}`.`test_no_header.csv`
            WITH (
                format = csv,
                SCHEMA (
                    Fruit String NOT NULL,
                    Price Int32 NOT NULL,
                    Weight Int32 NOT NULL
                )
            );
            '''

        query = client.execute(sql)
        query.check()
        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "Price"
        assert result_set.columns[0].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 3
        assert result_set.rows[0][0] == 3
        assert result_set.rows[1][0] == 2
        assert result_set.rows[2][0] == 15

    def test_csv_physical_column_order(self, kikimr, s3, client, unique_prefix):
        # Physical file columns are Weight,Price,Fruit (first row 100,3,Banana).
        # SCHEMA lists Weight, Price, Fruit тАФ parser must map by position, not by name
        # matching alphabetical order. Result column order from API may be sorted by name.
        # File columns: Weight,Price,Fruit (reversed order)
        fruits = '''100,3,Banana
22,2,Apple
33,15,Pear'''
        self.create_bucket_and_upload_file_body(fruits, "fruits_reversed.csv", s3, kikimr)

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits_reversed.csv`
            WITH (
                format = csv,
                SCHEMA (
                    Weight Int32 NOT NULL,
                    Price Int32 NOT NULL,
                    Fruit String NOT NULL
                )
            );
            '''

        query = client.execute(sql)
        query.check()
        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        col = {c.name: i for i, c in enumerate(result_set.columns)}
        for name in ("Weight", "Price", "Fruit"):
            assert name in col
        row0 = result_set.rows[0]
        assert row0[col["Weight"]] == 100
        assert row0[col["Price"]] == 3
        assert row0[col["Fruit"]] == b"Banana"

    def test_csv_format_custom_delimiter(self, kikimr, s3, client, unique_prefix):
        """csv + csv_delimiter (same scenario as `test_custom_csv_delimiter_csv`)."""
        # csv format with custom delimiter (semicolon)
        fruits = '''Banana;3;100
Apple;2;22
Pear;15;33'''
        self.create_bucket_and_upload_file_body(fruits, "fruits_semicolon.csv", s3, kikimr)

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits_semicolon.csv`
            WITH (
                format = csv,
                csv_delimiter = ";",
                SCHEMA (
                    Fruit String NOT NULL,
                    Price Int32 NOT NULL,
                    Weight Int32 NOT NULL
                )
            );
            '''

        query = client.execute(sql)
        query.check()
        data = query.result_set()
        result_set = data
        self.validate_result(result_set)
