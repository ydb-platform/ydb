import socket

from annet.annlib.types import Op
from annet.rulebook import common


def undo_commit(rule, key, diff, **_):
    # Huawei не даёт снести конфигурацию bgp и написать заново одним коммитом. Говорит:
    #    Invalid configuration. BGP is under undo.
    # при попытке создать новую после удаления
    if diff[Op.REMOVED]:
        rule["force_commit"] = True
        yield (False, rule["reverse"], None)
    # commit нужен под undo bgp
    rule["force_commit"] = False
    yield from common.default(rule, key, diff)


def peer(rule, key, diff, **_):  # pylint: disable=unused-argument
    """
    Особенность peer-команд в том, что
        peer IP as-number N
    является основной командой, и отменить её можно только через
        undo peer IP
    , то есть полностью удалив все настройки пира.

    При этом, as-number может выставляться и для группы:
        group SPINES
        peer SPINES as-number 13238
    в таком случае игнорим, позволяем удалить эту настройку поскольку она не дефайнит группу
        undo peer SPINES as-number
    """

    assert not diff[Op.AFFECTED], "Peer commands could not contain subcommands"
    for action in sorted(diff[Op.REMOVED], key=lambda act: "as-number" not in act["row"].split()):
        tokens = action["row"].split()
        (_, addr_or_group_name, param, *__) = tokens
        if param == "as-number":
            if _is_ip_addr(addr_or_group_name):
                yield (False, "undo peer {}".format(*key), None)
            else:
                # мы не можем делать common.default потому что правило определено как peer * а не peer * *
                # таким образом дефолтное поведение тут будет "undo peer PEERGROUP" что не то что мы хотим
                yield (False, "undo peer {} as-number".format(*key), None)
            break

        if param in ["connect-interface", "ebgp-max-hop", "local-as", "substitute-as", "password", "preferred-value"]:
            yield (False, "undo " + " ".join(tokens[:3]), None)
        else:
            yield (False, "undo " + action["row"], None)

    for action in sorted(diff[Op.ADDED], key=lambda act: "as-number" not in act["row"]):
        yield (True, action["row"], None)


def bfd(rule, key, diff, **_):
    """
    [*vla-1x1-bgp]undo peer SPINE1 bfd min-tx-interval 500 min-rx-interval 500 detect-multiplier 4
    │Error: Unrecognized command found at '^' position.

    [*vla-1x1-bgp]undo peer SPINE1 bfd min-rx-interval
    [~vla-1x1-bgp]undo peer SPINE1 bfd min-tx-interval
    [*vla-1x1-bgp]undo peer SPINE1 bfd detect-multiplier
    """
    if diff[Op.REMOVED]:
        assert len(diff[Op.REMOVED]) <= 1 and len(diff[Op.ADDED]) <= 1
        new_params = set()
        if diff[Op.ADDED]:
            new_params = set(_bfd_params_used(diff[Op.ADDED][0]["row"]))
        for token in _bfd_params_used(diff[Op.REMOVED][0]["row"]):
            if token not in new_params:
                yield (False, rule["reverse"].format(*key) + " " + token, None)
        diff[Op.REMOVED] = []
    if diff[Op.ADDED]:
        yield from common.default(rule, key, diff, **_)


def _is_ip_addr(addr_or_string):
    ret = None
    for af in (socket.AF_INET6, socket.AF_INET):
        try:
            ret = socket.inet_pton(af, addr_or_string)
        except OSError:
            pass
        else:
            break
    return bool(ret)


def _bfd_params_used(row):
    prev = None
    for token in row.split():
        if prev and token.isnumeric():
            if prev and token.isnumeric():
                yield prev
        prev = token
