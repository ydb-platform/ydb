import itertools
import math
import random
from typing import Callable, List, Tuple, Any, Optional

from fakeredis import _msgs as msgs
from fakeredis._command_args_parsing import extract_args
from fakeredis._commands import command, Key, Int, Float, CommandItem
from fakeredis._helpers import SimpleError, OK, casematch, SimpleString
from fakeredis._helpers import current_time
from fakeredis.model import Hash


class HashCommandsMixin:
    _encodeint: Callable[
        [
            int,
        ],
        bytes,
    ]
    _encodefloat: Callable[[float, bool], bytes]
    _scan: Callable[[CommandItem, int, bytes, bytes], Tuple[int, List[bytes]]]

    def _hset(self, key: CommandItem, *args: bytes) -> int:
        h = key.value
        keys_count = len(h.keys())
        h.update(dict(zip(*[iter(args)] * 2)))  # type: ignore  # https://stackoverflow.com/a/12739974/1056460
        created = len(h.keys()) - keys_count

        key.updated()
        return created

    @command((Key(Hash), bytes), (bytes,))
    def hdel(self, key: CommandItem, *fields: bytes) -> int:
        h = key.value
        rem = 0
        for field in fields:
            if field in h:
                del h[field]
                key.updated()
                rem += 1
        return rem

    @command((Key(Hash), bytes))
    def hexists(self, key: CommandItem, field: bytes) -> int:
        return int(field in key.value)

    @command((Key(Hash), bytes))
    def hget(self, key: CommandItem, field: bytes) -> Any:
        return key.value.get(field)

    @command((Key(Hash),))
    def hgetall(self, key: CommandItem) -> List[bytes]:
        return list(itertools.chain(*key.value.items()))

    @command(fixed=(Key(Hash), bytes, bytes))
    def hincrby(self, key: CommandItem, field: bytes, amount_bytes: bytes) -> int:
        amount = Int.decode(amount_bytes)
        field_value = Int.decode(key.value.get(field, b"0"), decode_error=msgs.INVALID_HASH_MSG)
        c = field_value + amount
        key.value[field] = self._encodeint(c)
        key.updated()
        return c

    @command((Key(Hash), bytes, bytes))
    def hincrbyfloat(self, key: CommandItem, field: bytes, amount: bytes) -> bytes:
        c = Float.decode(key.value.get(field, b"0")) + Float.decode(amount)
        if not math.isfinite(c):
            raise SimpleError(msgs.NONFINITE_MSG)
        encoded = self._encodefloat(c, True)
        key.value[field] = encoded
        key.updated()
        return encoded

    @command((Key(Hash),))
    def hkeys(self, key: CommandItem) -> List[bytes]:
        return list(key.value.keys())

    @command((Key(Hash),))
    def hlen(self, key: CommandItem) -> int:
        return len(key.value)

    @command((Key(Hash), bytes), (bytes,))
    def hmget(self, key: CommandItem, *fields: bytes) -> List[bytes]:
        return [key.value.get(field) for field in fields]

    @command((Key(Hash), bytes, bytes), (bytes, bytes))
    def hmset(self, key: CommandItem, *args: bytes) -> SimpleString:
        self.hset(key, *args)
        return OK

    @command((Key(Hash), Int), (bytes, bytes))
    def hscan(self, key: CommandItem, cursor: int, *args: bytes) -> List[Any]:
        cursor, keys = self._scan(key.value, cursor, *args)
        items = []
        for k in keys:
            items.append(k)
            items.append(key.value[k])
        return [cursor, items]

    @command((Key(Hash), bytes, bytes), (bytes, bytes))
    def hset(self, key: CommandItem, *args: bytes) -> int:
        return self._hset(key, *args)

    @command((Key(Hash), bytes, bytes))
    def hsetnx(self, key: CommandItem, field: bytes, value: bytes) -> int:
        if field in key.value:
            return 0
        return self._hset(key, field, value)

    @command((Key(Hash), bytes))
    def hstrlen(self, key: CommandItem, field: bytes) -> int:
        return len(key.value.get(field, b""))

    @command((Key(Hash),))
    def hvals(self, key: CommandItem) -> List[bytes]:
        return list(key.value.values())

    @command(name="HRANDFIELD", fixed=(Key(Hash),), repeat=(bytes,))
    def hrandfield(self, key: CommandItem, *args: bytes) -> Optional[List[bytes]]:
        if len(args) > 2:
            raise SimpleError(msgs.SYNTAX_ERROR_MSG)
        if key.value is None or len(key.value) == 0:
            return None
        count = min(Int.decode(args[0]) if len(args) >= 1 else 1, len(key.value))
        withvalues = casematch(args[1], b"withvalues") if len(args) >= 2 else False
        if count == 0:
            return list()

        if count < 0:  # Allow repetitions
            res = random.choices(sorted(key.value.items()), k=-count)
        else:  # Unique values from hash
            res = random.sample(sorted(key.value.items()), count)

        if withvalues:
            res = [item for t in res for item in t]
        else:
            res = [t[0] for t in res]
        return res

    def _hexpire(self, key: CommandItem, when_ms: int, *args: bytes) -> List[int]:
        # Deal with input arguments
        (nx, xx, gt, lt), left_args = extract_args(
            args, ("nx", "xx", "gt", "lt"), left_from_first_unexpected=True, error_on_unexpected=False
        )
        if (nx, xx, gt, lt).count(True) > 1:
            raise SimpleError(msgs.NX_XX_GT_LT_ERROR_MSG)
        if len(left_args) < 3 or not casematch(left_args[0], b"fields"):
            raise SimpleError(msgs.WRONG_ARGS_MSG6.format("HEXPIRE"))
        num_fields = Int.decode(left_args[1])
        if num_fields != len(left_args) - 2:
            raise SimpleError(msgs.HEXPIRE_NUMFIELDS_DIFFERENT)
        hash_val: Hash = key.value
        if hash_val is None:
            return [-2] * num_fields
        fields = left_args[2:]
        # process command
        res = []
        for field in fields:
            if field not in hash_val:
                res.append(-2)
                continue
            current_expiration = hash_val.get_key_expireat(field)
            if (
                (nx and current_expiration is not None)
                or (xx and current_expiration is None)
                or (gt and (current_expiration is None or when_ms <= current_expiration))
                or (lt and current_expiration is not None and when_ms >= current_expiration)
            ):
                res.append(0)
                continue
            res.append(hash_val.set_key_expireat(field, when_ms))
        return res

    def _get_expireat(self, command: bytes, key: CommandItem, *args: bytes) -> List[int]:
        if len(args) < 3 or not casematch(args[0], b"fields"):
            raise SimpleError(msgs.WRONG_ARGS_MSG6.format(command))
        num_fields = Int.decode(args[1])
        if num_fields != len(args) - 2:
            raise SimpleError(msgs.HEXPIRE_NUMFIELDS_DIFFERENT)
        hash_val: Hash = key.value
        if hash_val is None:
            return [-2] * num_fields
        fields = args[2:]
        res = list()
        for field in fields:
            if field not in hash_val:
                res.append(-2)
                continue
            when_ms = hash_val.get_key_expireat(field)
            if when_ms is None:
                res.append(-1)
            else:
                res.append(when_ms)
        return res

    @command(name="HEXPIRE", fixed=(Key(Hash), Int), repeat=(bytes,))
    def hexpire(self, key: CommandItem, seconds: int, *args: bytes) -> List[int]:
        when_ms = current_time() + seconds * 1000
        return self._hexpire(key, when_ms, *args)

    @command(name="HPEXPIRE", fixed=(Key(Hash), Int), repeat=(bytes,))
    def hpexpire(self, key: CommandItem, milliseconds: int, *args: bytes) -> List[int]:
        when_ms = current_time() + milliseconds
        return self._hexpire(key, when_ms, *args)

    @command(name="HEXPIREAT", fixed=(Key(Hash), Int), repeat=(bytes,))
    def hexpireat(self, key: CommandItem, unix_time_seconds: int, *args: bytes) -> List[int]:
        when_ms = unix_time_seconds * 1000
        return self._hexpire(key, when_ms, *args)

    @command(name="HPEXPIREAT", fixed=(Key(Hash), Int), repeat=(bytes,))
    def hpexpireat(self, key: CommandItem, unix_time_ms: int, *args: bytes) -> List[int]:
        return self._hexpire(key, unix_time_ms, *args)

    @command(name="HPERSIST", fixed=(Key(Hash),), repeat=(bytes,))
    def hpersist(self, key: CommandItem, *args: bytes) -> List[int]:
        if len(args) < 3 or not casematch(args[0], b"fields"):
            raise SimpleError(msgs.WRONG_ARGS_MSG6.format("HEXPIRE"))
        num_fields = Int.decode(args[1])
        if num_fields != len(args) - 2:
            raise SimpleError(msgs.HEXPIRE_NUMFIELDS_DIFFERENT)
        fields = args[2:]
        hash_val: Hash = key.value
        res = list()
        for field in fields:
            if field not in hash_val:
                res.append(-2)
                continue
            if hash_val.clear_key_expireat(field):
                res.append(1)
            else:
                res.append(-1)
        return res

    @command(
        name="HEXPIRETIME", fixed=(Key(Hash),), repeat=(bytes,), flags=msgs.FLAG_DO_NOT_CREATE, server_types=("redis",)
    )
    def hexpiretime(self, key: CommandItem, *args: bytes) -> List[int]:
        res = self._get_expireat(b"HEXPIRETIME", key, *args)
        return [(i // 1000 if i > 0 else i) for i in res]

    @command(name="HPEXPIRETIME", fixed=(Key(Hash),), repeat=(bytes,), server_types=("redis",))
    def hpexpiretime(self, key: CommandItem, *args: bytes) -> List[float]:
        res = self._get_expireat(b"HEXPIRETIME", key, *args)
        return res

    @command(name="HTTL", fixed=(Key(Hash),), repeat=(bytes,), server_types=("redis",))
    def httl(self, key: CommandItem, *args: bytes) -> List[int]:
        curr_expireat_ms = self._get_expireat(b"HEXPIRETIME", key, *args)
        curr_time_ms = current_time()
        return [((i - curr_time_ms) // 1000) if i > 0 else i for i in curr_expireat_ms]

    @command(name="HPTTL", fixed=(Key(Hash),), repeat=(bytes,), server_types=("redis",))
    def hpttl(self, key: CommandItem, *args: bytes) -> List[int]:
        curr_expireat_ms = self._get_expireat(b"HEXPIRETIME", key, *args)
        curr_time_ms = current_time()
        return [(i - curr_time_ms) if i > 0 else i for i in curr_expireat_ms]
