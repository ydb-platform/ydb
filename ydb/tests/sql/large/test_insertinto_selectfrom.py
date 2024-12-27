import concurrent.futures
import random
from ydb.tests.sql.lib.test_base import TpchTestBaseH1, TestBase


class TestConcurrentInsertAndCount(TpchTestBaseH1):

    # Function to perform bulk upserts
    def bulk_upsert_operation(self, table_name, data_slice):
        self.driver.table_client.bulk_upsert(
            f"{self.database}/{table_name}",
            data_slice,
            self.tpch_bulk_upsert_col_types()
        )

    def test_concurrent_bulkinsert_and_count(self):
        """
        Test concurrent operations: inserting into lineitem and counting records from lineitem.
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

        def insert_data():
            data = []
            # Insert 100,000 rows into lineitem
            for _ in range(100_000):
                lineitem = self.create_lineitem({
                    "l_orderkey": random.randint(1, self.tcph_est_records_count())
                    })

                data.append(lineitem)

            for chunk in TestBase.split_data_into_fixed_size_chunks(data, 1000):
                self.bulk_upsert_operation(lineitem_table, chunk)

        def count_data():
            round = 0
            # Repeatedly insert count from lineitem into another table
            while not insert_future.done():
                count_query = f"""
                UPSERT INTO `{count_table}`(id, cntr)
                SELECT {round} as id, COUNT(*) as cntr FROM `{lineitem_table}`;
                """
                self.query(count_query)
                round += 1

        # Use ThreadPoolExecutor to manage threads
        with concurrent.futures.ThreadPoolExecutor() as executor:
            insert_future = executor.submit(insert_data)
            count_future = executor.submit(count_data)

            # Allow reading thread to fully process last count
            count_future.result()

            # Wait for the insert operation to complete, then allow reading to complete
            insert_future.result()

        # Verify that both operations completed successfully
        final_count = self.query(f"SELECT count(*) as cntr FROM `{count_table}`;")
        assert len(final_count) > 0, "No count record found."

    def test_concurrent_upsert_and_count(self):
        """
        Test concurrent operations: inserting into lineitem and counting records from lineitem.
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
                    "l_orderkey": random.randint(1, self.tcph_est_records_count())
                    })

                data.append(lineitem)

            # Function to perform bulk upserts
            def upsert_data(session, chunk):
                tx = session.transaction().begin()

                try:
                    # print(self.build_lineitem_upsert_query(lineitem_table, lineitem))
                    for lineitem in chunk:
                        self.query(self.build_lineitem_upsert_query(lineitem_table, lineitem), tx)

                    tx.commit()
                except Exception:
                    tx.rollback()
                    raise

            # Split all data to chunks and UPSERT them
            for chunk in TestBase.split_data_into_fixed_size_chunks(data, 1000):
                if not count_future.done():
                    self.transactional(lambda session: upsert_data(session, chunk))

        def count_data():
            round = 0
            # Repeatedly insert count from lineitem into another table
            while not all(f.done() for f in insert_futures):
                count_query = f"""
                UPSERT INTO `{count_table}`(id, cntr)
                SELECT {round} as id, COUNT(*) as cntr FROM `{lineitem_table}`;
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

        # Verify that both operations completed successfully
        final_count = self.query(f"SELECT count(*) as cntr FROM `{count_table}`;")
        assert len(final_count) > 0, "No count record found."
