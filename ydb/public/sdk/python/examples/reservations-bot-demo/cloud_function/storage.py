import datetime
import typing
import ydb
from utils import session_pool_context, make_driver_config
from config import Config


class Storage(object):
    def __init__(self, *, endpoint: str, database: str, path: str):
        self._database = database
        self._driver_config = make_driver_config(endpoint, database, path)

    def list_table_ids(self, *, cnt: int = 0) -> typing.List[int]:
        query = f"""PRAGMA TablePathPrefix("{self._database}");
        DECLARE $cnt as Uint64;
        SELECT table_id FROM tables WHERE cnt >= $cnt;
        """

        def transaction(session):
            tx = session.transaction(ydb.SerializableReadWrite()).begin()
            prepared_query = session.prepare(query)
            rs = tx.execute(prepared_query, parameters={"$cnt": cnt}, commit_tx=True)
            return rs[0].rows

        with session_pool_context(self._driver_config) as session_pool:
            tables = session_pool.retry_operation_sync(transaction)
            return list(map(lambda x: getattr(x, "table_id"), tables))

    def find_reserved_table_ids(
        self, *, cnt: int, dt: datetime.datetime
    ) -> typing.List[int]:
        query = f"""PRAGMA TablePathPrefix("{self._database}");
        DECLARE $dt AS DateTime;
        DECLARE $reservation_period_minutes AS Int32;
        DECLARE $cnt AS Uint64;
        SELECT DISTINCT(table_id) FROM reservations
        WHERE dt <= $dt
        AND dt >= $dt - DateTime::IntervalFromMinutes($reservation_period_minutes)
        AND cnt >= $cnt;
        """

        def transaction(session):
            tx = session.transaction(ydb.SerializableReadWrite()).begin()
            prepared_query = session.prepare(query)
            rs = tx.execute(
                prepared_query,
                parameters={
                    "$cnt": cnt,
                    "$dt": int(dt.timestamp()),
                    "$reservation_period_minutes": Config.reservation_period_minutes(),
                },
                commit_tx=True,
            )
            if len(rs[0].rows) > 0:
                return rs[0].rows
            return []

        with session_pool_context(self._driver_config) as session_pool:
            tables = session_pool.retry_operation_sync(transaction)
            return list(map(lambda x: getattr(x, "table_id"), tables))

    def save_reservation(
        self,
        *,
        dt: datetime.datetime,
        table_id: int,
        cnt: int,
        description: str = "",
        phone: str,
    ) -> None:
        query = f"""PRAGMA TablePathPrefix("{self._database}");
        DECLARE $dt AS DateTime;
        DECLARE $cnt AS Uint64;
        DECLARE $phone AS String;
        DECLARE $description AS Utf8;
        DECLARE $table_id AS Uint64;
        INSERT INTO reservations (table_id, dt, phone, cnt, description)
        VALUES ($table_id, $dt, $phone, $cnt, $description);
        """

        def transaction(session):
            tx = session.transaction(ydb.SerializableReadWrite()).begin()
            prepared_query = session.prepare(query)
            tx.execute(
                prepared_query,
                parameters={
                    "$dt": int(dt.timestamp()),
                    "$cnt": cnt,
                    "$table_id": table_id,
                    "$description": "" if description is None else description,
                    "$phone": phone.encode(),
                },
                commit_tx=True,
            )

        with session_pool_context(self._driver_config) as session_pool:
            session_pool.retry_operation_sync(transaction)

    def delete_reservation(self, *, phone: str, dt: datetime.datetime) -> None:
        query = f"""PRAGMA TablePathPrefix("{self._database}");
        DECLARE $dt AS DateTime;
        DECLARE $phone AS String;
        DELETE FROM reservations WHERE dt = $dt AND phone = $phone;
        """

        def transaction(session):
            tx = session.transaction(ydb.SerializableReadWrite()).begin()
            prepared_query = session.prepare(query)
            tx.execute(
                prepared_query,
                parameters={"$dt": dt, "$phone": phone.encode()},
                commit_tx=True,
            )

        with session_pool_context(self._driver_config) as session_pool:
            session_pool.retry_operation_sync(transaction)
