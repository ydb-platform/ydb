import logging

from django.core.exceptions import ImproperlyConfigured

from idempotency_key import status, utils
from idempotency_key.exceptions import (
    DecoratorsMutuallyExclusiveError,
    bad_request,
    resource_locked,
)

logger = logging.getLogger("django-idempotency-key.idempotency_key.middleware")


class IdempotencyKeyMiddleware:
    """
    This middleware class assumes that all non-safe HTTP methods will require an
    idempotency key to be specified in the header.
    View functions can opt-out using the @idempotency_key_exempt decorator
    """

    def __init__(self, get_response=None):
        self.get_response = get_response
        self.storage = utils.get_storage_class()()
        self.encoder = utils.get_encoder_class()()
        self.storage_lock = utils.get_lock_class()()

    def __call__(self, request):
        self.process_request(request)
        response = self.get_response(request)
        response = self.process_response(request, response)
        return response

    @staticmethod
    def _reject(request, reason):
        response = bad_request(request, None)
        logger.debug(
            "Bad Request (%s): %s",
            reason,
            request.path,
            extra={"status_code": 400, "request": request},
        )
        return response

    def _set_flags_from_callback(self, request, callback):
        # If there is an actions attribute then the function is wrapped in a DRF viewset
        func_name = callback.__name__
        if hasattr(callback, "actions"):
            func_name = callback.actions[request.method.lower()]
            # get a reference to the function to access any attributes we might be
            # interested in.
            callback = getattr(callback.cls, func_name, callback)

        idempotency_key = getattr(callback, "idempotency_key", None)
        idempotency_key_optional = getattr(callback, "idempotency_key_optional", False)
        idempotency_key_exempt = getattr(callback, "idempotency_key_exempt", False)
        idempotency_key_manual = getattr(callback, "idempotency_key_manual", False)
        idempotency_key_cache_name = getattr(
            callback, "idempotency_key_cache_name", utils.get_storage_cache_name()
        )

        if idempotency_key and idempotency_key_exempt:
            raise DecoratorsMutuallyExclusiveError(
                "@idempotency_key and @idempotency_key_exempt decorators are mutually "
                'exclusive for function "{}"'.format(func_name)
            )

        if idempotency_key_manual and idempotency_key_exempt:
            raise DecoratorsMutuallyExclusiveError(
                "@idempotency_key_manual and @idempotency_key_exempt decorators are "
                'mutually exclusive for function "{}"'.format(func_name)
            )

        request.idempotency_key_optional = idempotency_key_optional
        request.idempotency_key_exempt = idempotency_key_exempt
        request.idempotency_key_manual = idempotency_key_manual
        request.idempotency_key_cache_name = idempotency_key_cache_name

    def perform_generate_response(self, request, encoded_key):
        # Check if a response already exists for the encoded key
        key_exists, response = self.storage.retrieve_data(
            request.idempotency_key_cache_name, encoded_key
        )

        # add the key exists result and the original request if it exists
        request.idempotency_key_exists = key_exists
        request.idempotency_key_response = response

        # If not manual override and the key already exists
        if not request.idempotency_key_manual and key_exists:
            # Get the required return status code from settings
            status_code = utils.get_conflict_code()
            # if None then return whatever the status code was originally otherwise use
            # the specified status code
            if status_code is not None:
                response.status_code = status_code
            return response

        return None

    def generate_response(self, request, encoded_key, lock=None):
        if lock is None:
            lock = utils.get_lock_enable()

        if not lock:
            return self.perform_generate_response(request, encoded_key)

        # If there was a timeout for a lock on the storage object then return a
        # HTTP_423_LOCKED
        if not self.storage_lock.acquire():
            return resource_locked(request, None)

        try:
            return self.perform_generate_response(request, encoded_key)
        finally:
            self.storage_lock.release()

    def process_request(self, request):
        key = request.META.get(utils.get_header_name())
        if key is not None:
            request.META["IDEMPOTENCY_KEY"] = key

        # Use this attribute to check that process_view has been called.
        request.idempotency_key_done = False

    def process_view(self, request, callback, _callback_args, _callback_kwargs):
        self._set_flags_from_callback(request, callback)

        # signal the process_view has been called
        request.idempotency_key_done = True

        # Assume that anything defined as 'safe' by RFC7231 is exempt or if exempt is
        # specified directly
        if request.idempotency_key_exempt or request.method in (
            "GET",
            "HEAD",
            "OPTIONS",
            "TRACE",
        ):
            request.idempotency_key_exempt = True
            return None

        # At this point the view function is not exempt so mark it as such
        request.idempotency_key_exempt = False

        key = request.META.get("IDEMPOTENCY_KEY")
        if key is None:
            if request.idempotency_key_optional:
                request.idempotency_key_exempt = True
                return None
            return self._reject(
                request,
                "Idempotency key is required and was not specified in the header.",
            )

        # encode the key and add it to the request
        encoded_key = request.idempotency_key_encoded_key = self.encoder.encode_key(
            request, key
        )

        # Generate the response
        return self.generate_response(request, encoded_key)

    def process_response(self, request, response):
        # If the response is not in the 20X range then return the response because at
        # this point protecting it with an idempotency key is meaningless.
        if response and response.status_code not in [
            status.HTTP_200_OK,
            status.HTTP_201_CREATED,
            status.HTTP_202_ACCEPTED,
            status.HTTP_203_NON_AUTHORITATIVE_INFORMATION,
            status.HTTP_204_NO_CONTENT,
            status.HTTP_205_RESET_CONTENT,
            status.HTTP_206_PARTIAL_CONTENT,
            status.HTTP_207_MULTI_STATUS,
        ]:
            return response

        # Make sure that process_view is called otherwise the use of idempotency keys
        # will be overridden without us knowing about it.
        if not getattr(request, "idempotency_key_done", False):
            raise ImproperlyConfigured(
                "Idempotency key middleware's 'process_view' function was not called! "
                "There maybe another middleware stopping this from happening which "
                "means your functions will not be properly protected with idempotency "
                "keys."
            )

        if getattr(request, "idempotency_key_exempt", True):
            return response

        if request.method not in ("GET", "HEAD", "OPTIONS", "TRACE"):
            # If the response matches that given by the store_on_statuses function then
            # store the data
            if response.status_code in utils.get_storage_store_on_statuses():
                self.storage.store_data(
                    request.idempotency_key_cache_name,
                    request.idempotency_key_encoded_key,
                    response,
                )

        return response


class ExemptIdempotencyKeyMiddleware(IdempotencyKeyMiddleware):
    """
    This middleware class assume all requests are exempt and do not require an
    idempotency key to be specified.
    View functions opt-in using the @idempotency_key or @idempotency_key_manual
    decorators.
    """

    def _set_flags_from_callback(self, request, callback):
        func_name = callback.__name__
        # If there is an actions attribute then the function is wrapped in a DRF viewset
        if hasattr(callback, "actions"):
            actual_func_name = callback.actions.get(request.method.lower())
            # if for some reason the method is not available in the viewset then just
            # proceed as normal and let the framework handle the problem.
            if actual_func_name is not None:
                func_name = actual_func_name

                # get a reference to the function to access any attributes we might be
                # interested in.
                callback = getattr(callback.cls, func_name, callback)

        idempotency_key = getattr(callback, "idempotency_key", False)
        idempotency_key_optional = getattr(callback, "idempotency_key_optional", False)
        idempotency_key_exempt = getattr(callback, "idempotency_key_exempt", None)
        idempotency_key_manual = getattr(callback, "idempotency_key_manual", False)
        idempotency_key_cache_name = getattr(
            callback, "idempotency_key_cache_name", utils.get_storage_cache_name()
        )

        if idempotency_key and idempotency_key_exempt:
            raise DecoratorsMutuallyExclusiveError(
                "@idempotency_key and @idempotency_key_exempt decorators are mutually "
                'exclusive for function "{}"'.format(func_name)
            )

        if idempotency_key_manual and idempotency_key_exempt:
            raise DecoratorsMutuallyExclusiveError(
                "@idempotency_key_manual and @idempotency_key_exempt decorators are "
                'mutually exclusive for function "{}"'.format(func_name)
            )

        request.idempotency_key_optional = idempotency_key_optional
        request.idempotency_key_exempt = idempotency_key_exempt or (
            idempotency_key_exempt is None
            and not idempotency_key_manual
            and not idempotency_key
        )
        request.idempotency_key_manual = idempotency_key_manual
        request.idempotency_key_cache_name = idempotency_key_cache_name
