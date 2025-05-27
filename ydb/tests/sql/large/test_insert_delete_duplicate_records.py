import concurrent.futures
import random
import ydb
from ydb.tests.sql.lib.test_base import TpchTestBaseH1


class TestConcurrentInsertDeleteAndRead(TpchTestBaseH1):

    def test_upsert_delete_and_read_tpch(self):
        """
        Tests concurrent upsert/delete operations with parallel reads.
        Verifies ACID properties by ensuring count of specific records (l_suppkey=9999)
        is always 0 or 1 during concurrent modifications:
        - Multiple threads performing cycles of upserts and immediate delete
        - Uses larger dataset (100k records) with batch size of 100
        - Single thread continuously checking record count
        - Validates consistency of concurrent operations
        """
        lineitem_table = f"{self.tpch_default_path()}/lineitem"

        # Delete all records with l_suppkey = 9999
        self.query(f"DELETE FROM `{lineitem_table}` WHERE l_suppkey = 9999")

        # Common l_orderkey
        l_orderkey = random.randint(1, self.tpch_est_records_count())

        # total records to upsert/delete
        total_records = 100_000

        # One worker thread required be
        num_threads = 10

        batch_size = 100

        def insert_and_delete():
            for _ in range(total_records // num_threads // batch_size):
                try:
                    queries = []
                    for _ in range(batch_size):
                        # Generate random values for other columns
                        lineitem = self.create_lineitem({
                            "l_orderkey": l_orderkey,
                            "l_linenumber": 1,
                            "l_suppkey": 9999
                            })
                        query = self.build_lineitem_upsert_query(lineitem_table, lineitem)
                        queries.append(query)

                    # Insert rows
                    self.query(";".join(queries))

                    # Immediately delete the row
                    delete_query = f"""
                    DELETE FROM `{lineitem_table}` WHERE l_suppkey = 9999;
                    """
                    self.query(delete_query)
                except ydb.issues.Aborted:
                    continue

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
            for future in insert_futures:
                future.result()

        check_data()

    def test_upsert_delete_and_read_tpch_tx(self):
        """
        Tests concurrent transactional upsert/delete operations with parallel reads.
        Performs batched operations within transactions:
        - Multiple threads doing batches of upserts followed by delete in single transaction
        - Single thread continuously verifying count is always 0
        - Uses larger dataset (100k records) with batch size of 100
        - Validates transaction isolation and consistency
        """
        lineitem_table = f"{self.tpch_default_path()}/lineitem"

        # Delete all records with l_suppkey = 9999
        self.query(f"DELETE FROM `{lineitem_table}` WHERE l_suppkey = 9999")

        # Common l_orderkey
        l_orderkey = random.randint(1, self.tpch_est_records_count())

        # total records to upsert/delete
        total_records = 100_000

        # One worker thread required be
        num_threads = 10

        # Batch size for upsert
        batch_size = 100

        def insert_and_delete():
            for _ in range(total_records // num_threads // batch_size):
                def transaction_upsert(session):
                    tx = session.transaction().begin()

                    try:
                        for _ in range(batch_size):
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

                try:
                    self.transactional(transaction_upsert)
                except ydb.issues.Aborted:
                    continue  # just skip TLI exception

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
            for future in insert_futures:
                future.result()

        check_data()

    def test_bulkupsert_delete_and_read_tpch(self):
        """
        Tests concurrent bulk upsert/delete operations with parallel reads.
        Performs batched bulk operations:
        - Multiple threads doing bulk upserts of 100 records followed by delete
        - Single thread continuously verifying count is between 0 and batch size
        - Uses larger dataset (100k records) processed in batches
        - Validates consistency during bulk operations
        """
        lineitem_table = f"{self.tpch_default_path()}/lineitem"

        # Delete all records with l_suppkey = 9999
        self.query(f"DELETE FROM `{lineitem_table}` WHERE l_suppkey = 9999")

        # Common l_orderkey
        l_orderkey = random.randint(1, self.tpch_est_records_count())

        # total records to upsert/delete
        total_records = 100_000

        # One worker thread required be
        num_threads = 10

        # Batch size for upsert
        batch_size = 100

        def insert_and_delete():
            for _ in range(total_records // num_threads // batch_size):
                try:
                    rows = []
                    for _ in range(batch_size):
                        # Generate random values for other columns
                        lineitem = self.create_lineitem({
                            "l_orderkey": l_orderkey,
                            "l_linenumber": 1,
                            "l_suppkey": 9999
                            })
                        rows.append(lineitem)

                    # Insert a row
                    self.bulk_upsert_operation(lineitem_table, rows)

                    # Immediately delete the row
                    delete_query = f"""
                    DELETE FROM `{lineitem_table}` WHERE l_suppkey = 9999;
                    """
                    self.query(delete_query)
                except ydb.issues.Aborted:
                    continue  # just skip TLI exception

                if read_future.done():
                    break

        def check_data():
            read_query = f"SELECT COUNT(*) AS cnt FROM `{lineitem_table}` WHERE l_suppkey = 9999;"
            result = self.query(read_query)
            count = result[0]['cnt']
            assert 0 <= count and count <= batch_size, f"Unexpected count of rows with l_suppkey=9999: {count}"

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
            for future in insert_futures:
                future.result()

        check_data()

    def test_insert_delete_and_read_simpletable(self):
        """
        Tests concurrent operations on simple table with two columns.
        Simulates basic concurrency scenario:
        - Multiple threads alternating between insert and delete of same record
        - Single thread continuously verifying count is 0 or 1
        - Uses simple schema (id, value) with primary key
        - Validates ACID properties on basic operations
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
        total_records = 10_000

        # One worker thread required be
        num_threads = 10

        batch_size = 10

        def insert_and_delete():
            for _ in range(total_records//num_threads//batch_size):
                try:
                    queries = []
                    for _ in range(batch_size):
                        # Insert a row
                        query = f"""
                            UPSERT INTO `{table_name}` (id, value) VALUES (1, '9999');
                            """
                        queries.append(query)

                    self.query(";".join(queries))

                    # Immediately delete the row
                    delete_query = f"""
                    DELETE FROM `{table_name}` WHERE value = '9999';
                    """
                    self.query(delete_query)
                except ydb.issues.Aborted:
                    continue

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
            for future in insert_futures:
                future.result()

        check_data()

    def test_insert_delete_and_read_simple_tx(self):
        """
        Tests concurrent transactional operations on simple table.
        Simulates transactional scenario:
        - Multiple threads performing insert+delete in single transaction
        - Single thread verifying count is always 0
        - Uses simple schema (id, value) with primary key
        - Validates transaction isolation and atomicity
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
        total_records = 1_000

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

                try:
                    self.transactional(transaction_upsert)
                except (ydb.issues.Aborted, RuntimeError):
                    continue  # just skip TLI exception

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
            for future in insert_futures:
                future.result()

        check_data()
