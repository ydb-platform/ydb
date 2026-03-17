from annet.annlib.types import Op
from annet.rulebook import common


def remove_last_param(rule, key, diff, **_):
    if diff[Op.REMOVED]:
        for rem in diff[Op.REMOVED]:
            # Обрабатывать удаление последнего параметра команды
            cmd_parts = rem["row"].split(" ")
            cmd_parts.remove(cmd_parts[len(cmd_parts) - 1])
            yield False, "undo %s" % " ".join(cmd_parts), None
    else:
        yield from common.default(rule, key, diff)
