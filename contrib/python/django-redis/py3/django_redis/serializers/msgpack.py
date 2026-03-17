from typing import Any

import msgpack

from django_redis.serializers.base import BaseSerializer


class MSGPackSerializer(BaseSerializer):
    def dumps(self, value: Any) -> bytes:
        return msgpack.dumps(value)

    def loads(self, value: bytes) -> Any:
        return msgpack.loads(value, raw=False)
