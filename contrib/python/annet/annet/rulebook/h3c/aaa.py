from annet.annlib.types import Op


def user(key, diff, **_):
    check_for_remove = True
    added = []
    for add in diff[Op.ADDED]:
        added.append((True, add["row"], None))
        if add["row"].startswith("local-user %s password" % key[0]):
            check_for_remove = False
    if check_for_remove:
        for rem in diff[Op.REMOVED]:
            # we abe able to overwrite new command without undo
            if rem["row"].startswith("local-user %s password" % key[0]):
                yield (False, "undo local-user %s" % key[0], None)
                return
            if rem["row"].startswith("local-user %s privilege" % key[0]) and not _added_contains(
                diff[Op.ADDED], "local-user %s privilege" % key[0]
            ):
                yield (False, "undo local-user %s" % key[0], None)
                return
    yield from added


def _added_contains(array: list[dict], lookup_string: str) -> bool:
    for item in array:
        if item["row"].startswith(lookup_string):
            return True
    return False
