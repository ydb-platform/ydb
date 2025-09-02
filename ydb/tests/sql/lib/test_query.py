import ydb
from typing import Callable, Any, List, Self, Optional


class Query(object):
    def create(database, endpoint, user=None, password=None) -> Self:
        query = Query()
        driver_config = ydb.DriverConfig(
            database=database,
            endpoint=endpoint
            )

        credentials = None
        if user is not None:
            credentials = ydb.StaticCredentials(
                driver_config=driver_config,
                user=user,
                password=password
                )

        query.driver = ydb.Driver(driver_config=driver_config, credentials=credentials)

        query.driver.wait(timeout=20)
        query.pool = ydb.QuerySessionPool(query.driver)
        return query

    def query(self, text,
              tx: ydb.QueryTxContext | None = None,
              stats: bool | None = None,
              parameters: Optional[dict] = None,
              retry_settings=None) -> List[Any]:
        results = []
        if tx is None:
            if not stats:
                result_sets = self.pool.execute_with_retries(text, parameters=parameters, retry_settings=retry_settings)
                for result_set in result_sets:
                    results.extend(result_set.rows)
            else:
                settings = ydb.ScanQuerySettings()
                settings = settings.with_collect_stats(ydb.QueryStatsCollectionMode.FULL)
                for response in self.driver.table_client.scan_query(text,
                                                                    settings=settings,
                                                                    parameters=parameters,
                                                                    retry_settings=retry_settings):
                    last_response = response
                    for row in response.result_set.rows:
                        results.append(row)

                return (results, last_response.query_stats)
        else:
            with tx.execute(text) as result_sets:
                for result_set in result_sets:
                    results.extend(result_set.rows)

        return results

    def transactional(self, fn: Callable[[ydb.QuerySession], List[Any]]):
        return self.pool.retry_operation_sync(lambda session: fn(session))
