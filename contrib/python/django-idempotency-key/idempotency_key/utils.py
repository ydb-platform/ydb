from django.conf import settings
from django.utils import module_loading

from idempotency_key import status


def idempotency_key_exists(request):
    return getattr(request, "idempotency_key_exists", False)


def idempotency_key_response(request):
    return getattr(request, "idempotency_key_response", None)


def get_idempotency_key_settings():
    return getattr(settings, "IDEMPOTENCY_KEY", dict())


def get_encoder_class():
    return module_loading.import_string(
        get_idempotency_key_settings().get(
            "ENCODER_CLASS", "idempotency_key.encoders.BasicKeyEncoder"
        )
    )


def get_conflict_code():
    return get_idempotency_key_settings().get(
        "CONFLICT_STATUS_CODE", status.HTTP_409_CONFLICT
    )


def get_storage_settings():
    return get_idempotency_key_settings().get("STORAGE", dict())


def get_storage_class():
    return module_loading.import_string(
        get_storage_settings().get("CLASS", "idempotency_key.storage.MemoryKeyStorage")
    )


def get_storage_cache_name():
    return get_storage_settings().get("CACHE_NAME", "default")


def get_storage_store_on_statuses():
    return get_storage_settings().get(
        "STORE_ON_STATUSES",
        [
            status.HTTP_200_OK,
            status.HTTP_201_CREATED,
            status.HTTP_202_ACCEPTED,
            status.HTTP_203_NON_AUTHORITATIVE_INFORMATION,
            status.HTTP_204_NO_CONTENT,
            status.HTTP_205_RESET_CONTENT,
            status.HTTP_206_PARTIAL_CONTENT,
            status.HTTP_207_MULTI_STATUS,
        ],
    )


def get_lock_settings():
    return get_idempotency_key_settings().get("LOCK", dict())


def get_lock_class():
    return module_loading.import_string(
        get_lock_settings().get("CLASS", "idempotency_key.locks.basic.ThreadLock")
    )


def get_lock_location():
    return get_lock_settings().get("LOCATION", "redis://localhost:6379/1")


def get_lock_timeout():
    return get_lock_settings().get("TIMEOUT", 0.1)  # default to 100ms


def get_lock_enable():
    return get_lock_settings().get("ENABLE", True)


def get_lock_time_to_live():
    return get_lock_settings().get("TTL", 300)  # default to 5 minutes


def get_lock_name():
    return get_lock_settings().get("NAME", "MyLock")


def get_header_name():
    return get_idempotency_key_settings().get("HEADER", "HTTP_IDEMPOTENCY_KEY")
