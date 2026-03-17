import abc
import pickle
from collections import defaultdict
from typing import Tuple

from django.core.cache import caches


class IdempotencyKeyStorage(object):
    @abc.abstractmethod
    def store_data(self, cache_name: str, encoded_key: str, response: object) -> None:
        """
        called when data should be stored in the storage medium
        :param cache_name: The name of the cache to use defined in settings under CACHES
        :param encoded_key: the key used to store the response data under
        :param response: The response data to store
        :return: None
        """
        raise NotImplementedError

    @abc.abstractmethod
    def retrieve_data(self, cache_name: str, encoded_key: str) -> Tuple[bool, object]:
        """
        Retrieve data from the sore using the specified key.
        :param cache_name: The name of the cache to use defined in settings under CACHES
        :param encoded_key: The key that was used to store the response data
        :return: the response data
        """
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def validate_storage(name: str):
        """
        Validate that the storage name exists. If the class is using django `CACHES`
        setting then this function ensures that the cache is setup correctly in the
        settings file and will cause a failure at startup if it is not.
        This function should raise an exception if the storage name cannot be validated.
        :param name: The name of the storage.
        """
        raise NotImplementedError


class MemoryKeyStorage(IdempotencyKeyStorage):
    def __init__(self):
        self.idempotency_key_cache_data = defaultdict(dict)

    def store_data(self, cache_name: str, encoded_key: str, response: object) -> None:
        self.idempotency_key_cache_data[cache_name][encoded_key] = response

    def retrieve_data(self, cache_name: str, encoded_key: str) -> Tuple[bool, object]:
        the_cache = self.idempotency_key_cache_data.get(cache_name)
        if the_cache and encoded_key in the_cache.keys():
            return True, the_cache[encoded_key]

        return False, None

    @staticmethod
    def validate_storage(name: str):
        pass


class CacheKeyStorage(IdempotencyKeyStorage):
    def store_data(self, cache_name: str, encoded_key: str, response: object) -> None:
        str_response = pickle.dumps(response)
        caches[cache_name].set(encoded_key, str_response)

    def retrieve_data(self, cache_name: str, encoded_key: str) -> Tuple[bool, object]:
        if encoded_key in caches[cache_name]:
            str_response = caches[cache_name].get(encoded_key)
            return True, pickle.loads(str_response)

        return False, None

    @staticmethod
    def validate_storage(name: str):
        # Check that the cache exists. If the cache is not found then an
        # InvalidCacheBackendError is raised. Note that there is no get function on the
        # caches object, so we cannot perform a normal check.
        caches[name]
