import concurrent.futures
import random
from ydb.tests.sql.lib.test_base import TpchTestBaseH1
from ydb.tests.sql.lib.helpers import split_data_into_fixed_size_chunks


class TestConcurrentInsertAndCount(TpchTestBaseH1):

    def test_concurrent_bulkinsert_and_count(self):
        """
        Tests concurrent bulk insert and count operations.
        Creates two tables and runs parallel operations:
        - Multiple threads performing bulk inserts of 100k records into lineitem table
        - Single thread continuously counting and storing results in count table
        Verifies consistency between insert and count operations.

        """
        lineitem_table = f"{self.tpch_default_path()}/lineitem"
        count_table = f"{self.tpch_default_path()}/lineitem_count"

        # One worker thread required be
        num_threads = 10

        # total records to UPSERT
        total_records = 100_000

        # Ensure the count table is clean and set up
        self.query(f"""
            CREATE TABLE IF NOT EXISTS `{count_table}` (
                id Int64 NOT NULL,
                cntr Int64 NOT NULL,
                PRIMARY KEY(id)
            )
            PARTITION BY HASH(id)
            WITH(STORE=COLUMN)
        """)

        def insert_data():
            data = []
            # Insert 100,000 rows into lineitem
            for _ in range(total_records//num_threads):
                lineitem = self.create_lineitem({
                    "l_orderkey": random.randint(1, self.tpch_est_records_count()),
                    "l_linenumber": random.randint(1, self.tpch_est_records_count())
                    })

                data.append(lineitem)

            for chunk in split_data_into_fixed_size_chunks(data, 1000):
                if not count_future.done():
                    self.bulk_upsert_operation(lineitem_table, chunk)

        def count_data():
            round = 0
            # Repeatedly insert count from lineitem into another table
            while not all(f.done() for f in insert_futures):
                count_query = f"""
                UPSERT INTO `{count_table}`(id, cntr)
                SELECT {round} as id, COALESCE(SUM(l_suppkey), 0) as cntr FROM `{lineitem_table}`;
                """
                self.query(count_query)
                round += 1

        # Use ThreadPoolExecutor to manage threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads+1) as executor:
            insert_futures = [executor.submit(insert_data) for _ in range(num_threads)]
            count_future = executor.submit(count_data)

            # Allow reading thread to fully process last count
            count_future.result()

            # Wait for the insert operation to complete, then allow reading to complete
            concurrent.futures.wait(insert_futures)
            for future in insert_futures:
                future.result()

        # Verify that both operations completed successfully
        final_count = self.query(f"SELECT count(*) as cntr FROM `{count_table}` WHERE cntr > 0;")
        assert len(final_count) > 0, "No count record found."

    def test_concurrent_upsert_and_count_tx(self):
        """
        Tests concurrent transactional upserts and count operations.
        Creates two tables and runs parallel operations in transactions:
        - Multiple threads performing transactional upserts of 100k records into lineitem
        - Single thread continuously counting and storing results
        Verifies data consistency between concurrent transactional operations.
        """
        lineitem_table = f"{self.tpch_default_path()}/lineitem"
        count_table = f"{self.tpch_default_path()}/lineitem_count"

        # Ensure the count table is clean and set up
        self.query(f"""
            CREATE TABLE IF NOT EXISTS `{count_table}` (
                id Int64 NOT NULL,
                cntr Int64 NOT NULL,
                PRIMARY KEY(id)
            )
            PARTITION BY HASH(id)
            WITH(STORE=COLUMN)
        """)

        count_future = None

        # One worker thread required be
        num_threads = 10

        # total records to UPSERT
        total_records = 100_000

        def insert_data():
            data = []
            # Insert N rows into lineitem
            for _ in range(total_records//num_threads):
                lineitem = self.create_lineitem({
                    "l_orderkey": random.randint(1, self.tpch_est_records_count()),
                    "l_linenumber": random.randint(1, self.tpch_est_records_count())
                    })

                data.append(lineitem)

            # Function to perform bulk upserts
            def upsert_data(session, chunk):
                tx = session.transaction().begin()

                try:
                    for lineitem in chunk:
                        self.query(self.build_lineitem_upsert_query(lineitem_table, lineitem), tx)

                    tx.commit()
                except Exception:
                    tx.rollback()
                    raise

            # Split all data to chunks and UPSERT them
            for chunk in split_data_into_fixed_size_chunks(data, 1000):
                if not count_future.done():
                    self.transactional(lambda session: upsert_data(session, chunk))

        def count_data():
            round = 0
            # Repeatedly insert count from lineitem into another table
            while not all(f.done() for f in insert_futures):
                count_query = f"""
                UPSERT INTO `{count_table}`(id, cntr)
                SELECT {round} as id, COALESCE(SUM(l_suppkey), 0) as cntr FROM `{lineitem_table}`;
                """
                self.query(count_query)
                round += 1

        # Use ThreadPoolExecutor to manage threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads+1) as executor:
            insert_futures = [executor.submit(insert_data) for _ in range(num_threads)]
            count_future = executor.submit(count_data)

            # Allow reading thread to fully process last count
            count_future.result()

            # Wait for the insert operation to complete, then allow reading to complete
            concurrent.futures.wait(insert_futures)
            for future in insert_futures:
                future.result()

        # Verify that both operations completed successfully
        final_count = self.query(f"SELECT count(*) as cntr FROM `{count_table}` WHERE cntr > 0;")
        assert len(final_count) > 0, "No count record found."

    def test_concurrent_upsert_and_count(self):
        """
        Tests concurrent batch upserts and count operations.
        Creates two tables and runs parallel operations:
        - Multiple threads performing batch upserts of 100k records into lineitem
        - Single thread continuously counting and storing results
        Combines multiple upsert statements into single batch query for better performance.
        Verifies data consistency between concurrent operations.
        """
        lineitem_table = f"{self.tpch_default_path()}/lineitem"
        count_table = f"{self.tpch_default_path()}/lineitem_count"

        # Ensure the count table is clean and set up
        self.query(f"""
            CREATE TABLE IF NOT EXISTS `{count_table}` (
                id Int64 NOT NULL,
                cntr Int64 NOT NULL,
                PRIMARY KEY(id)
            )
            PARTITION BY HASH(id)
            WITH(STORE=COLUMN)
        """)

        # One worker thread required be
        num_threads = 10

        # total records to UPSERT
        total_records = 100_000

        def insert_data():
            data = []
            # Insert N rows into lineitem
            for _ in range(total_records//num_threads):
                lineitem = self.create_lineitem({
                    "l_orderkey": random.randint(1, self.tpch_est_records_count()),
                    "l_linenumber": random.randint(1, self.tpch_est_records_count())
                    })

                data.append(lineitem)

            # Split all data to chunks and UPSERT them
            for chunk in split_data_into_fixed_size_chunks(data, 100):
                query_texts = []
                for lineitem in chunk:
                    query_texts.append(self.build_lineitem_upsert_query(lineitem_table, lineitem))

                if not count_future.done():
                    self.query(";".join(query_texts))

        def count_data():
            round = 0
            # Repeatedly insert count from lineitem into another table
            while not all(f.done() for f in insert_futures):
                count_query = f"""
                UPSERT INTO `{count_table}`(id, cntr)
                SELECT {round} as id, COALESCE(SUM(l_suppkey), 0) as cntr FROM `{lineitem_table}`;
                """
                self.query(count_query)
                round += 1

        # Use ThreadPoolExecutor to manage threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads+1) as executor:
            insert_futures = [executor.submit(insert_data) for _ in range(num_threads)]
            count_future = executor.submit(count_data)

            # Allow reading thread to fully process last count
            count_future.result()

            # Wait for the insert operation to complete, then allow reading to complete
            concurrent.futures.wait(insert_futures)
            for future in insert_futures:
                future.result()

        # Verify that both operations completed successfully
        final_count = self.query(f"SELECT count(*) as cntr FROM `{count_table}` WHERE cntr > 0;")
        assert len(final_count) > 0, "No count record found."
