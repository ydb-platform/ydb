import concurrent.futures
import random
from ydb.tests.sql.lib.test_base import TpchTestBaseH1


class TestConcurrentInsertDeleteAndRead(TpchTestBaseH1):

    def test_upsert_delete_and_read_tpch(self):
        """
        Test concurrent operations of inserting and deleting a row with l_suppkey=9999,
        while checking if the rows with l_suppkey=9999 are always 0 or 1.
        """
        lineitem_table = f"{self.tpch_default_path()}/lineitem"

        # Common l_orderkey
        l_orderkey = random.randint(1, self.tpch_est_records_count())

        # total records to upsert/delete
        total_records = 100_000

        # One worker thread required be
        num_threads = 10

        def insert_and_delete():
            for _ in range(total_records // num_threads):
                # Generate random values for other columns
                lineitem = self.create_lineitem({
                    "l_orderkey": l_orderkey,
                    "l_linenumber": 1,
                    "l_suppkey": 9999
                    })

                # Insert a row
                self.query(self.build_lineitem_upsert_query(lineitem_table, lineitem))

                # Immediately delete the row
                delete_query = f"""
                DELETE FROM `{lineitem_table}` WHERE l_suppkey = 9999;
                """
                self.query(delete_query)

                if read_future.done():
                    break

        def check_data():
            read_query = f"SELECT COUNT(*) AS cnt FROM `{lineitem_table}` WHERE l_suppkey = 9999;"
            result = self.query(read_query)
            count = result[0]['cnt']
            assert count in {0, 1}, f"Unexpected count of rows with l_suppkey=9999: {count}"

        def read_data():
            while not all(f.done() for f in insert_futures):
                check_data()

        # Use ThreadPoolExecutor to manage threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads+1) as executor:
            # Start insert and delete operations
            insert_futures = [executor.submit(insert_and_delete) for _ in range(num_threads)]

            # Start reading operations
            read_future = executor.submit(read_data)

            # Ensure both operations complete
            read_future.result()
            concurrent.futures.wait(insert_futures)

        check_data()

    def test_upsert_delete_and_read_tpch_tx(self):
        """
        Test concurrent operations of inserting and deleting a row with l_suppkey=9999,
        while checking if the rows with l_suppkey=9999 are always 0.
        """
        lineitem_table = f"{self.tpch_default_path()}/lineitem"

        # Common l_orderkey
        l_orderkey = random.randint(1, self.tpch_est_records_count())

        # total records to upsert/delete
        total_records = 100_000

        # One worker thread required be
        num_threads = 10

        def insert_and_delete():
            for _ in range(total_records // num_threads):
                def transaction_upsert(session):
                    tx = session.transaction().begin()

                    try:
                        # Generate random values for other columns
                        lineitem = self.create_lineitem({
                            "l_orderkey": l_orderkey,
                            "l_linenumber": 1,
                            "l_suppkey": 9999
                            })

                        # Insert a row
                        self.query(self.build_lineitem_upsert_query(lineitem_table, lineitem), tx)

                        # Immediately delete the row
                        delete_query = f"""
                        DELETE FROM `{lineitem_table}` WHERE l_suppkey = 9999;
                        """
                        self.query(delete_query, tx)
                        tx.commit()
                    except Exception:
                        tx.rollback()
                        raise

                self.transactional(transaction_upsert)
                if read_future.done():
                    break

        def check_data():
            read_query = f"SELECT COUNT(*) AS cnt FROM `{lineitem_table}` WHERE l_suppkey = 9999;"
            result = self.query(read_query)
            count = result[0]['cnt']
            assert count == 0, f"Unexpected count of rows with l_suppkey=9999: {count}"

        def read_data():
            while not all(f.done() for f in insert_futures):
                check_data()

        # Use ThreadPoolExecutor to manage threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads+1) as executor:
            # Start insert and delete operations
            insert_futures = [executor.submit(insert_and_delete) for _ in range(num_threads)]

            # Start reading operations
            read_future = executor.submit(read_data)

            # Ensure both operations complete
            read_future.result()
            concurrent.futures.wait(insert_futures)

        check_data()

    def test_bulkupsert_delete_and_read_tpch(self):
        """
        Test concurrent operations of inserting and deleting a row with l_suppkey=9999,
        while checking if the rows with l_suppkey=9999 are always 0 or 1.
        """
        lineitem_table = f"{self.tpch_default_path()}/lineitem"

        # Common l_orderkey
        l_orderkey = random.randint(1, self.tpch_est_records_count())

        # total records to upsert/delete
        total_records = 100_000

        # One worker thread required be
        num_threads = 10

        def insert_and_delete():
            for _ in range(total_records // num_threads):
                # Generate random values for other columns
                lineitem = self.create_lineitem({
                    "l_orderkey": l_orderkey,
                    "l_linenumber": 1,
                    "l_suppkey": 9999
                    })

                # Insert a row
                self.bulk_upsert_operation(lineitem_table, [lineitem])

                # Immediately delete the row
                delete_query = f"""
                DELETE FROM `{lineitem_table}` WHERE l_suppkey = 9999;
                """
                self.query(delete_query)

                if read_future.done():
                    break

        def check_data():
            read_query = f"SELECT COUNT(*) AS cnt FROM `{lineitem_table}` WHERE l_suppkey = 9999;"
            result = self.query(read_query)
            count = result[0]['cnt']
            assert count in {0, 1}, f"Unexpected count of rows with l_suppkey=9999: {count}"

        def read_data():
            while not all(f.done() for f in insert_futures):
                check_data()

        # Use ThreadPoolExecutor to manage threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads+1) as executor:
            # Start insert and delete operations
            insert_futures = [executor.submit(insert_and_delete) for _ in range(num_threads)]

            # Start reading operations
            read_future = executor.submit(read_data)

            # Ensure both operations complete
            read_future.result()
            concurrent.futures.wait(insert_futures)

        check_data()

    def test_insert_delete_and_read_simpletable(self):
        """
        Test concurrent operations of inserting and deleting a row to simple CS table,
        while checking if the rows count is always 0 or 1.
        """
        table_name = f"{self.table_path}"

        self.query(
            f"""
            CREATE TABLE `{table_name}` (
                id Int64 NOT NULL,
                value Utf8 NOT NULL,
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH(STORE=COLUMN)
            """
        )

        # total records to upsert/delete
        total_records = 100_000

        # One worker thread required be
        num_threads = 10

        def insert_and_delete():
            for _ in range(total_records//num_threads):

                # Insert a row
                self.query(
                    f"""
                    UPSERT INTO `{table_name}` (id, value) VALUES (1, '9999');
                    """
                    )

                # Immediately delete the row
                delete_query = f"""
                DELETE FROM `{table_name}` WHERE value = '9999';
                """
                self.query(delete_query)

                if read_future.done():
                    break

        def check_data():
            read_query = f"SELECT COUNT(*) AS cnt FROM `{table_name}` WHERE value = '9999';"
            result = self.query(read_query)
            count = result[0]['cnt']
            assert count in {0, 1}, f"Unexpected count of rows with value=9999: {count}"

        def read_data():
            while not all(f.done() for f in insert_futures):
                check_data()

        # Use ThreadPoolExecutor to manage threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads+1) as executor:
            # Start insert and delete operations
            insert_futures = [executor.submit(insert_and_delete) for _ in range(num_threads)]

            # Start reading operations
            read_future = executor.submit(read_data)

            # Ensure both operations complete
            read_future.result()
            concurrent.futures.wait(insert_futures)

        check_data()

    def test_insert_delete_and_read_simple_tx(self):
        """
        Test concurrent operations of inserting and deleting a row to simple CS table in tx,
        while checking if the rows count is always 0.
        """
        table_name = f"{self.table_path}"

        self.query(
            f"""
            CREATE TABLE `{table_name}` (
                id Int64 NOT NULL,
                value Utf8 NOT NULL,
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH(STORE=COLUMN)
            """
        )

        # number of worker threads
        num_threads = 10

        # total records to upsert/delete
        total_records = 100_000

        def insert_and_delete():
            for _ in range(total_records // num_threads):

                def transaction_upsert(session):
                    tx = session.transaction().begin()

                    try:
                        # Insert a row
                        self.query(
                            f"""
                            UPSERT INTO `{table_name}` (id, value) VALUES (1, '9999');
                            """, tx
                            )

                        # Immediately delete the row
                        delete_query = f"""
                        DELETE FROM `{table_name}` WHERE value = '9999';
                        """
                        self.query(delete_query, tx)
                        tx.commit()
                    except Exception:
                        tx.rollback()
                        raise

                self.transactional(transaction_upsert)

                if read_future.done():
                    break

        def check_data():
            read_query = f"SELECT COUNT(*) AS cnt FROM `{table_name}` WHERE value = '9999';"
            result = self.query(read_query)
            count = result[0]['cnt']
            assert count == 0, f"Unexpected count of rows with value=9999: {count}"

        def read_data():
            while not all(f.done() for f in insert_futures):
                check_data()

        # Use ThreadPoolExecutor to manage threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads+1) as executor:
            # Start insert and delete operations
            insert_futures = [executor.submit(insert_and_delete) for _ in range(num_threads)]

            # Start reading operations
            read_future = executor.submit(read_data)

            # Ensure both operations complete
            read_future.result()
            concurrent.futures.wait(insert_futures)

        check_data()
