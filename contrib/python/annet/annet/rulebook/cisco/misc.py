import re
from collections import OrderedDict
from typing import Any

from annet.annlib.types import Op
from annet.rulebook import common


def ssh_key(rule, key, diff, hw, **_):
    """
    При включении ssh надо еще сгенерировать ключ. По конфигу никак не понять есть ли ключ на свитче или нет.
    """
    if diff[Op.ADDED]:
        added = sorted([x["row"] for x in diff[Op.ADDED]])
        if added == ["ip ssh version 2"]:
            # Отсыпаем mpdaemon-у подсказок для дополнительной команды при наливке
            comment = rule["comment"]
            rule["comment"] = ["!!suppress_errors!!", "!!timeout=240!!"]
            if hw.Cisco.C2960:
                yield (False, "crypto key generate rsa modulus 2048", None)
            else:
                yield (False, "crypto key generate rsa general-keys modulus 2048", None)
            rule["comment"] = comment
    yield from common.default(rule, key, diff)


def no_ipv6_nd_suppress_ra(rule, key, diff, **_):
    """
    При конфигурации ipv6 nd на нексусах нужно добавлять
    no ipv6 nd suppress-ra
    иначе RA не будет включен.
    К сожалению данной команды не видно в running-config.
    Поэтому подмешиваем ее в патч вместо генератора
    """
    if diff[Op.ADDED]:
        yield (False, "no ipv6 nd suppress-ra", None)
    yield from common.default(rule, key, diff)


def no_ntp_distribute(rule, key, diff, **_):
    """
    Для того, чтобы удалить NTP из CFS, сначала нужно сбросить активные
    NTP сессии.
    """
    if diff[Op.REMOVED]:
        yield (False, "clear ntp session", None)
    yield from common.default(rule, key, diff)


def banner_any(rule, key, diff, **_):
    if diff[Op.ADDED]:
        # Убираем дополнительный экранирующий сиимвол
        banner = re.sub(r"\^C", "^", diff[Op.ADDED][0]["row"])
        yield (False, banner, None)
    elif diff[Op.REMOVED]:
        yield (False, f"no banner {key[0]}", None)
    else:
        yield from common.default(rule, key, diff)
