import logging

from hamcrest import assert_that, equal_to

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.harness.util import LogLevels

logger = logging.getLogger(__name__)

CLUSTER_CONFIG = dict(
    additional_log_configs={
        'TX_DATASHARD': LogLevels.DEBUG,
        'KQP_PROXY': LogLevels.DEBUG,
    },
    extra_feature_flags=["enable_fulltext_index", "enable_truncate_table"]
)


def test_truncate_table_with_fulltext_index(ydb_cluster, ydb_database, ydb_client):
    logger.info("Starting AddFullTextFlatIndexWithTruncate2 Python version")

    driver = ydb_client(ydb_database)
    driver.wait(timeout=10)

    with ydb.SessionPool(driver) as session_pool:
        with session_pool.checkout() as session:
            logger.info("Creating table")
            session.execute_scheme('''
                CREATE TABLE `/Root/test_truncate_table_with_fulltext_index/Texts` (
                    Key Uint64,
                    Text String,
                    Data String,
                    PRIMARY KEY (Key)
                );
            ''')

            logger.info("Adding fulltext index")
            session.execute_scheme('''
                ALTER TABLE `/Root/test_truncate_table_with_fulltext_index/Texts` ADD INDEX fulltext_idx
                    GLOBAL USING fulltext_plain
                    ON (Text)
                    WITH (tokenizer=standard, use_filter_lowercase=true)
            ''')

            def upsert_some_texts():
                logger.info("Upserting some texts")
                session.transaction().execute('''
                    UPSERT INTO `/Root/test_truncate_table_with_fulltext_index/Texts` (Key, Text, Data) VALUES
                        (100, "Cats love cats.", "cats data"),
                        (200, "Dogs love foxes.", "dogs data")
                ''', commit_tx=True)

            def select_empty_results():
                logger.info("Selecting empty results")
                result = session.transaction().execute('''
                    SELECT `Key`, `Text`
                    FROM `/Root/test_truncate_table_with_fulltext_index/Texts` VIEW `fulltext_idx`
                    WHERE FulltextMatch(`Text`, "404 not found")
                    ORDER BY `Key`;
                ''', commit_tx=True)
                assert_that(len(result[0].rows), equal_to(0))

            def truncate_table():
                logger.info("Truncating table")
                session.execute_scheme('''
                    TRUNCATE TABLE `/Root/test_truncate_table_with_fulltext_index/Texts`;
                ''')

            def verify_index_works_correctly(count):
                logger.info(f"========== begin verifyIndexWorksCorrectly {count} ==========")
                select_empty_results()
                upsert_some_texts()
                select_empty_results()
                logger.info(f"========== end verifyIndexWorksCorrectly {count} ==========")

            # Initial verification
            verify_index_works_correctly(-1)

            # Test cycle: truncate and verify 5 times
            for try_index in range(5):
                logger.info(f"========== TRUNCATE cycle {try_index} ==========")
                truncate_table()
                verify_index_works_correctly(try_index)

    logger.info("Test completed successfully")

def test_truncate_table_with_fulltext_index_with_query_service(ydb_cluster, ydb_database, ydb_client):
    logger.info("Starting AddFullTextFlatIndexWithTruncate2 Python version")

    driver = ydb_client(ydb_database)
    driver.wait(timeout=10)

    with ydb.QuerySessionPool(driver) as session_pool:
        logger.info("Creating table")
        session_pool.execute_with_retries('''
            CREATE TABLE `/Root/test_truncate_table_with_fulltext_index_with_query_service/Texts` (
                Key Uint64,
                Text String,
                Data String,
                PRIMARY KEY (Key)
            );
        ''')

        logger.info("Adding fulltext index")
        session_pool.execute_with_retries('''
            ALTER TABLE `/Root/test_truncate_table_with_fulltext_index_with_query_service/Texts` ADD INDEX fulltext_idx
                GLOBAL USING fulltext_plain
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        ''')

        def upsert_some_texts():
            logger.info("Upserting some texts")
            session_pool.execute_with_retries('''
                UPSERT INTO `/Root/test_truncate_table_with_fulltext_index_with_query_service/Texts` (Key, Text, Data) VALUES
                    (100, "Cats love cats.", "cats data"),
                    (200, "Dogs love foxes.", "dogs data")
            ''')

        def select_empty_results():
            logger.info("Selecting empty results")
            result = session_pool.execute_with_retries('''
                SELECT `Key`, `Text`
                FROM `/Root/test_truncate_table_with_fulltext_index_with_query_service/Texts` VIEW `fulltext_idx`
                WHERE FulltextMatch(`Text`, "404 not found")
                ORDER BY `Key`;
            ''')
            assert_that(len(result[0].rows), equal_to(0))

        def truncate_table():
            logger.info("Truncating table")
            session_pool.execute_with_retries('''
                TRUNCATE TABLE `/Root/test_truncate_table_with_fulltext_index_with_query_service/Texts`;
            ''')

        def verify_index_works_correctly(count):
            logger.info(f"========== begin verifyIndexWorksCorrectly {count} ==========")
            select_empty_results()
            upsert_some_texts()
            select_empty_results()
            logger.info(f"========== end verifyIndexWorksCorrectly {count} ==========")

        # Initial verification
        verify_index_works_correctly(-1)

        # Test cycle: truncate and verify 5 times
        for try_index in range(5):
            logger.info(f"========== TRUNCATE cycle {try_index} ==========")
            truncate_table()
            verify_index_works_correctly(try_index)

    logger.info("Test completed successfully")

