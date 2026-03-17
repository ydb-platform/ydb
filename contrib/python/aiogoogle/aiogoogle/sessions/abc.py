__all__ = ["AbstractSession"]


from abc import ABC, abstractmethod
import inspect


class AbstractSession(ABC):
    """
    Should represent an HTTP session that has an asynchronous context manager and a send method

    Argumetnts:

        Should return an instance without needing to pass it any args or kwargs
    """

    def __new__(cls, *args, **kwargs):
        # Get all coros of this the abstract class
        parent_abstract_coros = inspect.getmembers(
            AbstractSession, predicate=inspect.iscoroutinefunction
        )

        # Ensure all relevant child methods are implemented as coros
        for coro in parent_abstract_coros:
            coro_name = coro[0]
            child_method = getattr(cls, coro_name)
            if not inspect.iscoroutinefunction(child_method):
                raise RuntimeError(f"{child_method} must be a coroutine")

        # Resume with normal behavior of a Python constructor
        return super(AbstractSession, cls).__new__(cls)

    @abstractmethod
    async def send(self, *requests, timeout=None, full_res=False, session_factory=None):
        """
        Takes requests, sends them, returns contents of responses or full http responses.

        Note:

            Given more than one request this method should return responses as a list. However if only given one, it will return a single response object

        Arguments:

            requests (aiogoogle.models.Request):

                Request objects from aiogoogle.models

            timeout (int):

                Total timeout for *requests

            full_res (bool):

                Return full HTTP response with headers, status code

            raise_for_status (bool):

                * Whether or not to raise an HTTP error if status code is >= 400

                * Default: True

            session_factory (aiogoogle.sessions.abs.AbstractSession):

                * A callable implementation of aiogoogle's session interface

                * Session factory that creates a session that will handle pagination, resumable uploads etc.

                * Defaults to ``self.__class__``

        Returns:

            aiogoogle.models.Response

        Raises:

            aiogoogle.excs.HTTPError

        """
        raise NotImplementedError
