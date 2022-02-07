# -*- coding: utf-8 -*-
try:
    import tornado.concurrent
    import tornado.ioloop
    import tornado.gen
    from tornado.concurrent import TracebackFuture
except ImportError:
    tornado = None

from ydb.table import retry_operation_impl, YdbRetryOperationSleepOpt


def as_tornado_future(foreign_future, timeout=None):
    """
    Return tornado.concurrent.Future wrapped python concurrent.future (foreign_future).
    Cancel execution original future after given timeout
    """
    result_future = tornado.concurrent.Future()
    timeout_timer = set()
    if timeout:

        def on_timeout():
            timeout_timer.clear()
            foreign_future.cancel()

        timeout_timer.add(
            tornado.ioloop.IOLoop.current().call_later(timeout, on_timeout)
        )

    def copy_to_result_future(foreign_future):
        try:
            to_remove = timeout_timer.pop()
            tornado.ioloop.IOLoop.current().remove_timeout(to_remove)
        except KeyError:
            pass

        if result_future.done():
            return

        if (
            isinstance(foreign_future, TracebackFuture)
            and isinstance(result_future, TracebackFuture)
            and result_future.exc_info() is not None
        ):
            result_future.set_exc_info(foreign_future.exc_info())
        elif foreign_future.cancelled():
            result_future.set_exception(tornado.gen.TimeoutError())
        elif foreign_future.exception() is not None:
            result_future.set_exception(foreign_future.exception())
        else:
            result_future.set_result(foreign_future.result())

    tornado.ioloop.IOLoop.current().add_future(foreign_future, copy_to_result_future)
    return result_future


async def retry_operation(callee, retry_settings=None, *args, **kwargs):
    opt_generator = retry_operation_impl(callee, retry_settings, *args, **kwargs)

    for next_opt in opt_generator:
        if isinstance(next_opt, YdbRetryOperationSleepOpt):
            await tornado.gen.sleep(next_opt.timeout)
        else:
            try:
                return await next_opt.result
            except Exception as e:
                next_opt.set_exception(e)
