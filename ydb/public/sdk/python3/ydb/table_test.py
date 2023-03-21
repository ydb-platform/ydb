from unittest import mock
from . import (
    retry_operation_impl,
    YdbRetryOperationFinalResult,
    issues,
    YdbRetryOperationSleepOpt,
    RetrySettings,
)


def test_retry_operation_impl(monkeypatch):
    monkeypatch.setattr("random.random", lambda: 0.5)
    monkeypatch.setattr(
        issues.Error,
        "__eq__",
        lambda self, other: type(self) == type(other) and self.message == other.message,
    )

    retry_once_settings = RetrySettings(
        max_retries=1,
        on_ydb_error_callback=mock.Mock(),
    )
    retry_once_settings.unknown_error_handler = mock.Mock()

    def get_results(callee):
        res_generator = retry_operation_impl(callee, retry_settings=retry_once_settings)
        results = []
        exc = None
        try:
            for res in res_generator:
                results.append(res)
                if isinstance(res, YdbRetryOperationFinalResult):
                    break
        except Exception as e:
            exc = e

        return results, exc

    class TestException(Exception):
        def __init__(self, message):
            super(TestException, self).__init__(message)
            self.message = message

        def __eq__(self, other):
            return type(self) == type(other) and self.message == other.message

    def check_unretriable_error(err_type, call_ydb_handler):
        retry_once_settings.on_ydb_error_callback.reset_mock()
        retry_once_settings.unknown_error_handler.reset_mock()

        results = get_results(
            mock.Mock(side_effect=[err_type("test1"), err_type("test2")])
        )
        yields = results[0]
        exc = results[1]

        assert yields == []
        assert exc == err_type("test1")

        if call_ydb_handler:
            assert retry_once_settings.on_ydb_error_callback.call_count == 1
            retry_once_settings.on_ydb_error_callback.assert_called_with(
                err_type("test1")
            )

            assert retry_once_settings.unknown_error_handler.call_count == 0
        else:
            assert retry_once_settings.on_ydb_error_callback.call_count == 0

            assert retry_once_settings.unknown_error_handler.call_count == 1
            retry_once_settings.unknown_error_handler.assert_called_with(
                err_type("test1")
            )

    def check_retriable_error(err_type, backoff):
        retry_once_settings.on_ydb_error_callback.reset_mock()

        results = get_results(
            mock.Mock(side_effect=[err_type("test1"), err_type("test2")])
        )
        yields = results[0]
        exc = results[1]

        if backoff:
            assert [
                YdbRetryOperationSleepOpt(backoff.calc_timeout(0)),
                YdbRetryOperationSleepOpt(backoff.calc_timeout(1)),
            ] == yields
        else:
            assert [] == yields

        assert exc == err_type("test2")

        assert retry_once_settings.on_ydb_error_callback.call_count == 2
        retry_once_settings.on_ydb_error_callback.assert_any_call(err_type("test1"))
        retry_once_settings.on_ydb_error_callback.assert_called_with(err_type("test2"))

        assert retry_once_settings.unknown_error_handler.call_count == 0

    # check ok
    assert get_results(lambda: True) == ([YdbRetryOperationFinalResult(True)], None)

    # check retry error and return result
    assert get_results(mock.Mock(side_effect=[issues.Overloaded("test"), True])) == (
        [
            YdbRetryOperationSleepOpt(retry_once_settings.slow_backoff.calc_timeout(0)),
            YdbRetryOperationFinalResult(True),
        ],
        None,
    )

    # check errors
    check_retriable_error(issues.Aborted, None)
    check_retriable_error(issues.BadSession, None)

    check_retriable_error(issues.NotFound, None)
    with mock.patch.object(retry_once_settings, "retry_not_found", False):
        check_unretriable_error(issues.NotFound, True)

    check_retriable_error(issues.InternalError, None)
    with mock.patch.object(retry_once_settings, "retry_internal_error", False):
        check_unretriable_error(issues.InternalError, True)

    check_retriable_error(issues.Overloaded, retry_once_settings.slow_backoff)
    check_retriable_error(issues.SessionPoolEmpty, retry_once_settings.slow_backoff)
    check_retriable_error(issues.ConnectionError, retry_once_settings.slow_backoff)

    check_retriable_error(issues.Unavailable, retry_once_settings.fast_backoff)

    check_unretriable_error(issues.Undetermined, True)
    with mock.patch.object(retry_once_settings, "idempotent", True):
        check_retriable_error(issues.Unavailable, retry_once_settings.fast_backoff)

    check_unretriable_error(issues.Error, True)
    with mock.patch.object(retry_once_settings, "idempotent", True):
        check_unretriable_error(issues.Error, True)

    check_unretriable_error(TestException, False)
    with mock.patch.object(retry_once_settings, "idempotent", True):
        check_unretriable_error(TestException, False)
