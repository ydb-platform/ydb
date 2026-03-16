import socket

from annet.annlib.types import Op
from annet.rulebook import common


def peer(rule, key, diff, **_):  # pylint: disable=unused-argument
    """
    The peculiarity of peer commands is that
        peer IP as-number N
    is the main command, and it can only be removed with
        undo peer IP
    which completely deletes all settings of the peer.
    At the same time, the as-number can also be set for a group:
        group SPINES
        peer SPINES as-number 13238
    In this case, we ignore it and allow this setting to be deleted, since it does not define the group itself:
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
                # We can’t use common.default because the rule is defined as "peer *" and not "peer * *".
                # Therefore, the default behavior here would be "undo peer PEERGROUP", which is not what we want.
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
