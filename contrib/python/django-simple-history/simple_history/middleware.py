from contextlib import contextmanager

from asgiref.sync import iscoroutinefunction
from django.utils.decorators import sync_and_async_middleware

from .models import HistoricalRecords


@contextmanager
def _context_manager(request):
    HistoricalRecords.context.request = request

    try:
        yield None
    finally:
        try:
            del HistoricalRecords.context.request
        except AttributeError:
            pass


@sync_and_async_middleware
def HistoryRequestMiddleware(get_response):
    """Expose request to HistoricalRecords.

    This middleware sets request as a local context/thread variable, making it
    available to the model-level utilities to allow tracking of the authenticated user
    making a change.
    """

    if iscoroutinefunction(get_response):

        async def middleware(request):
            with _context_manager(request):
                return await get_response(request)

    else:

        def middleware(request):
            with _context_manager(request):
                return get_response(request)

    return middleware
