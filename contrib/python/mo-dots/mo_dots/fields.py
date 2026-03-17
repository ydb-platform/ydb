import re
import sys

from mo_future import generator_types, flatten
from mo_imports import expect

from mo_dots.utils import get_logger, CLASS, is_missing, is_null

_module_type = type(sys.modules[__name__])
_builtin_zip = zip
_get = object.__getattribute__


ESCAPE_DOTS1 = re.compile(r"(^\.|\.$)")  # DOTS AT START/END
ESCAPE_DOTS2 = re.compile(r"(?<!^)\.(?!$)")  # INTERNAL DOTS
ILLEGAL_DOTS = re.compile(r"[^.]\.(?:\.\.)+")  # ODD DOTS ARE NOT ALLOWED
SPLIT_DOTS = re.compile(r"(?<!\.)\.(?!\.)")  # SINGLE DOTS
UNESCAPE_DOTS = re.compile(r"\x08|(?:\.\.)")  # ENCODED DOTS


def literal_field(field):
    """
    RETURN SAME WITH DOTS (`.`) ESCAPED
    """
    try:
        return ESCAPE_DOTS2.sub("..", ESCAPE_DOTS1.sub("\b", field))
    except Exception as e:
        get_logger().error("bad literal", e)


def unliteral_field(field):
    """
    DUE TO PATHOLOGY IN MY CODE WE HAVE A path WITH ESCAPED DOTS BUT WE WANT OT USE IT ON A dict, NOT A Data
    a = dict()
    b = Data(a)
    a[unliteral_field(k)]==b[k] (for all k)

    :param field: THE STRING TO DE-literal IZE
    :return: SIMPLER STRING
    """
    return UNESCAPE_DOTS.sub(".", field)


def tail_field(field):
    """
    RETURN THE FIRST STEP IN PATH, ALONG WITH THE REMAINING TAIL
    IN (first, rest) PAIR
    """
    if field == "." or is_missing(field):
        return ".", "."
    elif "." in field:
        path = split_field(field)
        if path[0].startswith("."):
            return path[0], join_field(path[1:])
        return literal_field(path[0]), join_field(path[1:])
    else:
        return field, "."


def split_field(field):
    """
    RETURN field AS ARRAY OF DOT-SEPARATED FIELDS
    """
    if ILLEGAL_DOTS.search(field):
        get_logger().error("Odd number of dots is not allowed")
    if field.startswith(".."):
        remainder = field.lstrip(".")
        back = len(field) - len(remainder) - 1
        return [".."] * back + [UNESCAPE_DOTS.sub(".", k) for k in SPLIT_DOTS.split(remainder) if k]
    else:
        return [UNESCAPE_DOTS.sub(".", k) for k in SPLIT_DOTS.split(field) if k]


def join_field(path):
    """
    RETURN field SEQUENCE AS STRING
    """
    if _get(path, CLASS) in generator_types:
        path = list(path)

    if not path:
        return "."

    prefix = ""
    while True:
        try:
            i = path.index("..")
            if i == 0:
                prefix += "."
                path = path[1:]
            else:
                path = path[: i - 1] + path[i + 1 :]
        except ValueError:
            return ("." if prefix else "") + prefix + ".".join(literal_field(f) for f in path)


def concat_field(*fields):
    return join_field(flatten(split_field(f) for f in fields))


def startswith_field(field, prefix):
    """
    RETURN True IF field PATH STRING STARTS WITH prefix PATH STRING
    """
    if prefix == None:
        return False
    if prefix.startswith("."):
        return len(prefix) == 1

    if field.startswith(prefix):
        lp = len(prefix)
        if len(field) == len(prefix) or field[lp] in (".", "\b") and field[lp + 1] not in (".", "\b"):
            return True
    return False


def endswith_field(field, suffix):
    """
    RETURN True IF field PATH STRING ENDS WITH suffix PATH STRING
    """
    if is_null(suffix):
        return False
    if suffix == ".":
        return True

    if field.endswith(suffix):
        ls = len(suffix)
        if len(field) == ls or field[-ls - 1] in (".", "\b") and field[-ls - 2] not in (".", "\b"):
            return True
    return False


def relative_field(field, parent):
    """
    RETURN field PATH WITH RESPECT TO parent
    """
    if parent == ".":
        return field

    field_path = split_field(field)
    parent_path = split_field(parent)
    common = 0
    for f, p in _builtin_zip(field_path, parent_path):
        if f != p:
            break
        common += 1

    tail = join_field(field_path[common:])
    if len(parent_path) <= common:
        return join_field(field_path[common:])

    dots = "." * (len(parent_path) - common)
    if tail == ".":
        return "." + dots
    else:
        return "." + dots + tail
