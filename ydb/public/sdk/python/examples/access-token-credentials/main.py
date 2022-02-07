import os
import ydb
import ydb.iam


def execute_query(session):
    # Create the transaction and execute the `select 1` query.
    # All transactions must be committed using the `commit_tx` flag in the last
    # statement. The either way to commit transaction is using `commit` method of `TxContext` object, which is
    # not recommended.
    return session.transaction().execute(
        "select 1 as cnt;",
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2),
    )


def main():
    # Example demonstrates how to initializate driver instance
    # using the access token credentials provider.
    # This method is not recommended to be used for PRODUCTION applications,
    # because the specified access token can expire. For example,
    # Yandex.Cloud IAM token lifetime is limited by 12 hours.
    driver = ydb.Driver(
        endpoint=os.getenv("YDB_ENDPOINT"),
        database=os.getenv("YDB_DATABASE"),
        credentials=ydb.AccessTokenCredentials(
            os.getenv("YDB_ACCESS_TOKEN_CREDENTIALS")
        ),
    )

    # Start driver context manager.
    # The recommended way of using Driver object is using `with`
    # clause, because the context manager automatically stops the driver.
    with driver:
        # wait until driver become initialized
        driver.wait(fail_fast=True, timeout=5)

        # Initialize the session pool instance and enter the context manager.
        # The context manager automatically stops the session pool.
        # On the session pool termination all YDB sessions are closed.
        with ydb.SessionPool(driver) as pool:

            # Execute the query with the `retry_operation_helper` the.
            # The `retry_operation_sync` helper used to help developers
            # to retry YDB specific errors like locks invalidation.
            # The first argument of the `retry_operation_sync` is a function to retry.
            # This function must have session as the first argument.
            result = pool.retry_operation_sync(execute_query)
            assert result[0].rows[0].cnt == 1


main()
