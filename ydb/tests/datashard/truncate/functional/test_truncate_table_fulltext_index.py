from hamcrest import assert_that, equal_to

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.harness.util import LogLevels

CLUSTER_CONFIG = dict(
    additional_log_configs={
        'TX_DATASHARD': LogLevels.DEBUG,
        'KQP_PROXY': LogLevels.DEBUG,
    },
    extra_feature_flags=["enable_fulltext_index", "enable_truncate_table"]
)


def test_truncate_table_with_fulltext_index_table_service(ydb_cluster, ydb_database, ydb_client):
    driver = ydb_client(ydb_database)
    driver.wait(timeout=10)

    with ydb.SessionPool(driver) as session_pool:
        with session_pool.checkout() as session:
            session.execute_scheme('''
                CREATE TABLE `Texts` (
                    Key Uint64,
                    Text String,
                    Data String,
                    PRIMARY KEY (Key)
                );
            ''')

            session.execute_scheme('''
                ALTER TABLE `Texts` ADD INDEX fulltext_idx
                    GLOBAL USING fulltext_plain
                    ON (Text)
                    WITH (tokenizer=standard, use_filter_lowercase=true)
            ''')

            def upsert_some_texts():
                session.transaction().execute('''
                    UPSERT INTO `Texts` (Key, Text, Data) VALUES
                        (100, "Cats love cats.", "cats data"),
                        (200, "Dogs love foxes.", "dogs data")
                ''', commit_tx=True)

            def select_empty_results():
                result = session.transaction().execute('''
                    SELECT `Key`, `Text`
                    FROM `Texts` VIEW `fulltext_idx`
                    WHERE FulltextMatch(`Text`, "404 not found")
                    ORDER BY `Key`;
                ''', commit_tx=True)
                assert_that(len(result[0].rows), equal_to(0))

            def truncate_table():
                session.execute_scheme('''
                    TRUNCATE TABLE `Texts`;
                ''')

                result = session.transaction().execute('''
                    SELECT *
                    FROM `Texts`;
                ''', commit_tx=True)
                assert_that(len(result[0].rows), equal_to(0))

            def verify_index_works_correctly():
                select_empty_results()
                upsert_some_texts()
                select_empty_results()

            verify_index_works_correctly()

            for _ in range(5):
                truncate_table()
                verify_index_works_correctly()


def test_truncate_table_with_fulltext_index_with_query_service(ydb_cluster, ydb_database, ydb_client):
    driver = ydb_client(ydb_database)
    driver.wait(timeout=10)

    with ydb.QuerySessionPool(driver) as session_pool:
        session_pool.execute_with_retries('''
            CREATE TABLE `Texts` (
                Key Uint64,
                Text String,
                Data String,
                PRIMARY KEY (Key)
            );
        ''')

        session_pool.execute_with_retries('''
            ALTER TABLE `Texts` ADD INDEX fulltext_idx
                GLOBAL USING fulltext_plain
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        ''')

        def upsert_some_texts():
            session_pool.execute_with_retries('''
                UPSERT INTO `Texts` (Key, Text, Data) VALUES
                    (100, "Cats love cats.", "cats data"),
                    (200, "Dogs love foxes.", "dogs data")
            ''')

        def select_empty_results():
            result = session_pool.execute_with_retries('''
                SELECT `Key`, `Text`
                FROM `Texts` VIEW `fulltext_idx`
                WHERE FulltextMatch(`Text`, "404 not found")
                ORDER BY `Key`;
            ''')
            assert_that(len(result[0].rows), equal_to(0))

        def truncate_table():
            session_pool.execute_with_retries('''
                TRUNCATE TABLE `Texts`;
            ''')

            result = session_pool.execute_with_retries('''
                SELECT *
                FROM `Texts`;
            ''')
            assert_that(len(result[0].rows), equal_to(0))

        def verify_index_works_correctly():
            select_empty_results()
            upsert_some_texts()
            select_empty_results()

        verify_index_works_correctly()

        for _ in range(5):
            truncate_table()
            verify_index_works_correctly()


def test_truncate_table_with_fulltext_index_with_query_service_many_sessions(ydb_cluster, ydb_database, ydb_client):
    driver = ydb_client(ydb_database)
    driver.wait(timeout=10)

    with ydb.QuerySessionPool(driver) as session_pool:
        session_pool.execute_with_retries('''
            CREATE TABLE `Texts` (
                Key Uint64,
                Text String,
                Data String,
                PRIMARY KEY (Key)
            );
        ''')

    with ydb.QuerySessionPool(driver) as session_pool:
        session_pool.execute_with_retries('''
            ALTER TABLE `Texts` ADD INDEX fulltext_idx
                GLOBAL USING fulltext_plain
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        ''')

        def upsert_some_texts():
            with ydb.QuerySessionPool(driver) as session_pool:
                session_pool.execute_with_retries('''
                    UPSERT INTO `Texts` (Key, Text, Data) VALUES
                        (100, "Cats love cats.", "cats data"),
                        (200, "Dogs love foxes.", "dogs data")
                ''')

        def select_empty_results():
            with ydb.QuerySessionPool(driver) as session_pool:
                result = session_pool.execute_with_retries('''
                    SELECT `Key`, `Text`
                    FROM `Texts` VIEW `fulltext_idx`
                    WHERE FulltextMatch(`Text`, "404 not found")
                    ORDER BY `Key`;
                ''')
                assert_that(len(result[0].rows), equal_to(0))

        def truncate_table():
            with ydb.QuerySessionPool(driver) as session_pool:
                session_pool.execute_with_retries('''
                    TRUNCATE TABLE `Texts`;
                ''')

                result = session_pool.execute_with_retries('''
                    SELECT *
                    FROM `Texts`;
                ''')
                assert_that(len(result[0].rows), equal_to(0))

        def verify_index_works_correctly():
            select_empty_results()
            upsert_some_texts()
            select_empty_results()

        verify_index_works_correctly()

        for _ in range(5):
            truncate_table()
            verify_index_works_correctly()
