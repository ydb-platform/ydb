from annet.annlib.types import Op
from annet.rulebook import common


def undo_peer_group(rule, key, diff, **_):
    """Correctly removes neighbors that were added to the peer-group."""
    for action in diff[Op.ADDED]:
        yield (True, action["row"], None)

    if diff[Op.REMOVED]:
        is_group = any(action["row"].endswith(" peer-group") for action in diff[Op.REMOVED])

        group_name = ""
        for action in diff[Op.REMOVED]:
            if " peer-group " in action["row"]:
                group_name = action["row"].split()[-1]
        is_remote_as = any(" remote-as " in action["row"] for action in diff[Op.REMOVED])

        is_group_in_removed = False
        if _["rule_pre"]["items"].get(tuple([group_name])):
            for action in _["rule_pre"]["items"][tuple([group_name])]["removed"]:
                if action["row"].endswith(f"{group_name} peer-group"):
                    is_group_in_removed = True
                    break

        for action in diff[Op.REMOVED]:
            row = action["row"]
            if is_group:
                if row.endswith(" peer-group"):
                    yield (False, "no " + row, None)
            elif is_group_in_removed:
                continue
            elif is_remote_as or group_name:
                if " peer-group " in row:
                    yield (False, "no " + row, None)
            else:
                yield (False, "no " + row, None)


def undo_peer(rule, key, diff, **kwargs):
    """
    If we remove a neighbor, we just remove configuration with remote-as command.
    But if we remove specific neighbor's options, without neighbor deletion
    we need check if neighbor * remote-as not exists in Op.REMOVED rule_pre items.
    """
    if diff[Op.REMOVED]:
        if "remote-as" in diff[Op.REMOVED][0]["row"]:
            yield (False, "no " + diff[Op.REMOVED][0]["row"], None)
        else:
            is_neighbor_removing = False
            for item in kwargs["rule_pre"]["items"].values():
                if item[Op.REMOVED]:
                    if f"neighbor {key[0].split()[0]} remote-as" in item[Op.REMOVED][0]["row"]:
                        is_neighbor_removing = True
                        break
            if not is_neighbor_removing:
                yield from common.default(rule, key, diff, **kwargs)
    else:
        yield from common.default(rule, key, diff, **kwargs)
