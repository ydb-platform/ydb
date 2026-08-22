import asyncio
import threading
from concurrent.futures import Future


class BackgroundEventLoop:
    """
    A wrapper for running asyncio coroutines in a background thread.

    This class creates a dedicated event loop that runs in a daemon thread,
    allowing synchronous code to submit and execute asyncio coroutines.
    Useful for safety/liveness checks that need to run async operations
    from a synchronous context.
    """

    def __init__(self):
        """Initialize the background event loop and start the daemon thread."""
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever,
            daemon=True  # will terminate together with the main program
        )
        self._thread.start()

    def submit(self, coro) -> Future:
        """
        Submit a coroutine to the background event loop.

        Args:
            coro: The coroutine to execute in the background loop

        Returns:
            Future: A Future object that can be used to track the coroutine's completion
                   and retrieve its result
        """
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def stop(self):
        """Stop the background event loop and wait for the thread to complete."""
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
