from __future__ import annotations

import ast
import contextlib
import dataclasses
import math
import re
import types
from collections import namedtuple
from functools import reduce
from typing import Any, Callable

from .logging import logger

# Variable info with formatting metadata
VarInfo = namedtuple("VarInfo", ["name", "typename", "value", "format_hint"])


def _format_scalar(v: Any) -> str:
    """Format a single numeric value intelligently."""
    # Integer types (including numpy integers) - display without decimals
    if isinstance(v, int) or (hasattr(v, "dtype") and "int" in str(v.dtype)):
        return str(int(v))
    # Float types
    if isinstance(v, float) or (hasattr(v, "dtype") and "float" in str(v.dtype)):
        v = float(v)
        if v != v:  # NaN
            return "NaN"
        if abs(v) == float("inf"):
            return "∞" if v > 0 else "-∞"
        if v == 0:
            return "0"
        if v == int(v) and abs(v) < 1e15:
            return str(int(v))
        # Compact fixed-point: strip trailing zeros
        return f"{v:.6f}".rstrip("0").rstrip(".")
    return str(v)


# Superscript digits for formatting powers of 10
_SUPERSCRIPT_DIGITS = str.maketrans("-0123456789", "⁻⁰¹²³⁴⁵⁶⁷⁸⁹")


def _get_flat(arr: Any) -> list[Any]:
    """Get a flat/1D view of an array, supporting numpy and torch."""
    # Try numpy-style .flat first
    if hasattr(arr, "flat"):
        return arr.flat
    # PyTorch tensors use .flatten()
    if hasattr(arr, "flatten"):
        return arr.flatten()
    # Fallback - just return the array itself
    return arr


def _array_formatter(arr: Any) -> tuple[Callable[[Any], str], str]:
    """
    Create an optimal formatter for displaying array values consistently.

    For integers: display as integers without decimals.
    For floats: determine scale from max(abs(values)), apply SI-style
    scaling (×10⁶, ×10⁻³, etc.), and display with consistent fixed precision.
    Returns (formatter_func, scale_suffix) where scale_suffix may be empty.
    """
    try:
        dtype_str = str(arr.dtype)
    except AttributeError:
        dtype_str = ""

    # Integer arrays - display as integers, no scaling
    if "int" in dtype_str or "bool" in dtype_str:
        return lambda v: str(int(v)), ""

    # For float arrays, analyze the values to determine optimal formatting
    if "float" in dtype_str or "complex" in dtype_str:
        flat = _get_flat(arr)
        # Sample values for analysis
        n = len(flat)
        if n <= 200:
            sample = [float(v) for v in flat]
        else:
            sample = [float(flat[i]) for i in range(100)]
            sample += [float(flat[n - 100 + i]) for i in range(100)]

        # Filter out non-finite values for scale analysis
        finite = [abs(v) for v in sample if v == v and abs(v) != float("inf")]
        if not finite:
            # All NaN/Inf
            return lambda v: ("NaN" if v != v else ("∞" if v > 0 else "-∞")), ""

        max_abs = max(finite) if finite else 0
        log_max = math.log10(max_abs) if max_abs > 0 else 0
        scale_power = int(log_max // 3) * 3
        if scale_power in (-3, 0, 3):
            scale_power = 0  # No scientific notation for average scales
        scale_suffix = (
            f"×10{str(scale_power).translate(_SUPERSCRIPT_DIGITS)}"
            if scale_power
            else ""
        )
        scale_factor = 10.0 ** (-scale_power) if scale_power else 1.0
        log_scaled = log_max - scale_power if scale_power else log_max
        decimals = max(0, 2 - math.floor(log_scaled)) if max_abs > 0 else 0

        def fmt(v: Any, sf: float = scale_factor, d: int = decimals) -> str:
            if v != v:
                return "NaN"
            if v == float("inf"):
                return "∞"
            if v == float("-inf"):
                return "-∞"
            scaled = v * sf
            if scaled == 0:
                return "0"
            return f"{scaled:.{d}f}"

        return fmt, scale_suffix

    # Fallback for other types
    return (lambda v: f"{v}"), ""


blacklist_names = {"_", "In", "Out"}
blacklist_types = (
    type,
    types.ModuleType,
    types.FunctionType,
    types.MethodType,
    types.BuiltinFunctionType,
)
no_str_conv = re.compile(r"<.* object at 0x[0-9a-fA-F]{5,}>")


def _extract_identifiers_ast(sourcecode: str) -> set[str] | None:
    """
    Extract variable identifiers from source code using AST.

    Returns a set of variable names (including attribute access like "obj.attr"),
    or None if AST parsing fails.
    """
    # Try to parse as an expression first (most common case for error lines)
    for wrapper in ("({})", "{}"):
        try:
            code = wrapper.format(sourcecode)
            tree = ast.parse(code, mode="eval")
            break
        except SyntaxError:
            continue
    else:
        # Try parsing as statements
        try:
            tree = ast.parse(sourcecode, mode="exec")
        except SyntaxError:
            return None

    identifiers: set[str] = set()

    class IdentifierVisitor(ast.NodeVisitor):
        def visit_Name(self, node: ast.Name) -> None:
            identifiers.add(node.id)
            self.generic_visit(node)

        def visit_Attribute(self, node: ast.Attribute) -> None:
            # Build the full dotted name for attribute access
            parts = [node.attr]
            current = node.value
            while isinstance(current, ast.Attribute):
                parts.append(current.attr)
                current = current.value
            if isinstance(current, ast.Name):
                parts.append(current.id)
                # Add the full dotted name (e.g., "obj.attr")
                identifiers.add(".".join(reversed(parts)))
                # Also add intermediate names (e.g., "obj" for "obj.attr.sub")
                for i in range(len(parts)):
                    identifiers.add(".".join(reversed(parts[i:])))
            self.generic_visit(node)

    IdentifierVisitor().visit(tree)
    return identifiers


def _extract_identifiers_regex(sourcecode: str) -> set[str]:
    """
    Extract variable identifiers from source code using regex (fallback).

    This is less accurate than AST as it can match names in strings/comments,
    but works when AST parsing fails.
    """
    return {
        m.group(0) for p in (r"\w+", r"\w+\.\w+") for m in re.finditer(p, sourcecode)
    }


def extract_variables(variables: dict[str, Any], sourcecode: str) -> list[VarInfo]:
    # Try AST-based extraction first, fall back to regex
    identifiers = _extract_identifiers_ast(sourcecode)
    if identifiers is None:
        identifiers = _extract_identifiers_regex(sourcecode)
    rows = []
    for name, value in variables.items():
        if name in blacklist_names or isinstance(value, blacklist_types):
            continue
        try:
            typename = type(value).__name__
            if name not in identifiers:
                continue
            try:
                strvalue = str(value)
                reprvalue = repr(value)
            except Exception:
                continue  # Skip variables failing str() or repr()
            # Using repr is better for empty strings and some other cases
            if not strvalue and reprvalue:
                strvalue = reprvalue
            # Try to print members of objects that don't have proper __str__
            elif no_str_conv.fullmatch(strvalue):
                found = False
                try:
                    members = safe_vars(value).items()
                except Exception:
                    members = []
                for n, v in members:
                    mname = f"{name}.{n}"
                    if sourcecode and mname not in identifiers:
                        continue
                    tname = type(v).__name__
                    if isinstance(v, blacklist_types):
                        continue
                    # Check if the member also has a poor representation
                    try:
                        member_str = str(v)
                        if no_str_conv.fullmatch(member_str):
                            continue  # Skip members with poor representations
                    except Exception:
                        continue  # Skip members that fail str()
                    tname += f" in {typename}"
                    val_str, val_fmt = prettyvalue(v)
                    rows += (VarInfo(mname, tname, val_str, val_fmt),)
                    found = True
                if found:
                    continue
                value = "⋯"
            # Full types for Numpy-like arrays, PyTorch tensors, etc.
            try:
                dtype = str(object.__getattribute__(value, "dtype")).rsplit(".", 1)[-1]
                if typename == dtype:
                    raise AttributeError  # Numpy scalars need no further info
                shape = object.__getattribute__(value, "shape")
                dims = "×".join(str(d + 0) for d in shape) + " " if shape else ""
                try:
                    dev = object.__getattribute__(value, "device")
                    dev = f"@{dev}" if dev and dev.type != "cpu" else ""
                except AttributeError:
                    dev = ""
                typename += f" of {dims}{dtype}{dev}"
            except AttributeError:
                pass
            val_str, val_fmt = prettyvalue(value)
            # Don't show type for None values
            if typename == "NoneType":
                typename = ""
            rows += (VarInfo(name, typename, val_str, val_fmt),)
        except Exception:
            logger.exception("Variable inspector failed (please report a bug)")
    return rows


def safe_vars(obj: Any) -> dict[str, Any]:
    """Like vars(), but also supports objects with slots."""
    ret = {}
    for attr in dir(obj):
        with contextlib.suppress(AttributeError):
            ret[attr] = object.__getattribute__(obj, attr)
    return ret


def prettyvalue(val: Any) -> tuple[Any, str]:
    """
    Format a value for display in the inspector.

    Returns:
        tuple: (formatted_value, format_hint) where format_hint is one of:
               'block' - left-aligned block format (for multi-line strings)
               'inline' - inline right-aligned format (default)
    """
    # Handle namedtuple formatting before regular tuple check
    # namedtuples have _fields attribute which is a tuple of field names
    if (
        isinstance(val, tuple)
        and hasattr(val, "_fields")
        and isinstance(val._fields, tuple)
    ):
        fields = val._fields
        if not fields:
            return (f"{type(val).__name__}()", "inline")
        # For small namedtuples, return as structured table
        if len(fields) <= 10:
            rows = []
            for name in fields:
                key_str = name if len(name) <= 40 else name[:37] + "…"
                field_val = getattr(val, name)
                val_str = (
                    f"{field_val!s}"
                    if len(f"{field_val!s}") <= 60
                    else f"{field_val!s}"[:57] + "…"
                )
                rows.append([key_str, val_str])
            return ({"type": "keyvalue", "rows": rows}, "inline")
        # For larger namedtuples, show summary
        return (f"({len(fields)} fields)", "inline")
    if isinstance(val, (list, tuple)):
        if not 0 < len(val) <= 10:
            return (f"({len(val)} items)", "inline")
        return (", ".join(repr(v)[:80] for v in val), "inline")
    if isinstance(val, dict):
        # Handle dict formatting specially
        if not val:
            return ("{}", "inline")
        # For small dicts, return as structured table
        if len(val) <= 10:
            # Return list of [key, value] pairs for structured rendering
            rows = []
            for k, v in val.items():
                key_str = f"{k!s}" if len(f"{k!s}") <= 40 else f"{k!s}"[:37] + "…"
                val_str = f"{v!s}" if len(f"{v!s}") <= 60 else f"{v!s}"[:57] + "…"
                rows.append([key_str, val_str])
            return ({"type": "keyvalue", "rows": rows}, "inline")
        # For larger dicts, show summary
        return (f"({len(val)} items)", "inline")
    if dataclasses.is_dataclass(val) and not isinstance(val, type):
        # Handle dataclass formatting similar to dict
        fields = dataclasses.fields(val)
        if not fields:
            return (f"{type(val).__name__}()", "inline")
        # For small dataclasses, return as structured table
        if len(fields) <= 10:
            rows = []
            for field in fields:
                key_str = field.name if len(field.name) <= 40 else field.name[:37] + "…"
                field_val = object.__getattribute__(val, field.name)
                val_str = (
                    f"{field_val!s}"
                    if len(f"{field_val!s}") <= 60
                    else f"{field_val!s}"[:57] + "…"
                )
                rows.append([key_str, val_str])
            return ({"type": "keyvalue", "rows": rows}, "inline")
        # For larger dataclasses, show summary
        return (f"({len(fields)} fields)", "inline")
    # msgspec Struct and Pydantic BaseModel support (without importing either)
    # msgspec uses __struct_fields__ (tuple), Pydantic v2 uses model_fields (dict)
    struct_fields = None
    if isinstance(fields := getattr(type(val), "__struct_fields__", None), tuple):
        struct_fields = fields
    elif isinstance(fields := getattr(type(val), "model_fields", None), dict):
        struct_fields = tuple(fields.keys())
    if struct_fields is not None:
        if not struct_fields:
            return (f"{type(val).__name__}()", "inline")
        if len(struct_fields) <= 10:
            rows = []
            for name in struct_fields:
                key_str = name if len(name) <= 40 else name[:37] + "…"
                field_val = object.__getattribute__(val, name)
                val_str = (
                    f"{field_val!s}"
                    if len(f"{field_val!s}") <= 60
                    else f"{field_val!s}"[:57] + "…"
                )
                rows.append([key_str, val_str])
            return ({"type": "keyvalue", "rows": rows}, "inline")
        return (f"({len(struct_fields)} fields)", "inline")
    if isinstance(val, type):
        return (f"{val.__module__}.{val.__name__}", "inline")
    try:
        # This only works for Numpy-like arrays, and should cause exceptions otherwise
        shape = object.__getattribute__(val, "shape")
        if isinstance(shape, tuple) and val.shape:
            numelem = reduce(lambda x, y: x * y, shape)
            if numelem <= 1:
                flat = _get_flat(val)
                return (_format_scalar(flat[0]), "inline")
            # 1D arrays
            if len(shape) == 1:
                fmt, suffix = _array_formatter(val)
                if shape[0] <= 100:
                    result = ", ".join(fmt(v) for v in val)
                else:
                    formatted = [fmt(v) for v in (*val[:3], *val[-3:])]
                    result = ", ".join([*formatted[:3], "…", *formatted[-3:]])
                if suffix:
                    result = f"{result} {suffix}"
                return (result, "inline")
            # 2D arrays
            if len(shape) == 2 and shape[0] <= 10 and shape[1] <= 10:
                fmt, suffix = _array_formatter(val)
                table = [[fmt(v) for v in row] for row in val]
                if suffix:
                    return (
                        {"type": "array", "rows": table, "suffix": suffix},
                        "inline",
                    )
                return (table, "inline")
    except (AttributeError, ValueError):
        pass
    except Exception:
        logger.exception(
            "Pretty-printing in variable inspector failed (please report a bug)"
        )

    try:
        # Handle numpy scalars and plain floats/ints (but not arrays)
        dtype_str = str(getattr(val, "dtype", ""))
        # Check it's not an array (no shape, or empty shape)
        shape = getattr(val, "shape", ())
        is_scalar = not shape or (isinstance(shape, tuple) and len(shape) == 0)
        is_numeric = isinstance(val, (int, float)) or (dtype_str and is_scalar)
        if is_numeric and not isinstance(val, bool):
            return (_format_scalar(val), "inline")
    except (AttributeError, TypeError, ValueError):
        pass

    # Determine format hint based on content
    format_hint = "inline"

    # Format exceptions using str() for cleaner display
    if isinstance(val, BaseException):
        ret = str(val)
    elif isinstance(val, str):
        ret = str(val)
        # Multi-line strings should be displayed as blocks
        if "\n" in ret.rstrip():
            format_hint = "block"
    else:
        ret = repr(val)

    # For inline format, collapse newlines to avoid display issues
    if format_hint == "inline":
        if "\n" in ret:
            ret = " ".join(line.strip() for line in ret.split("\n") if line.strip())
        # Only truncate inline values
        if len(ret) > 120:
            ret = ret[:30] + " … " + ret[-30:]
    # For block format, don't truncate but limit line count if needed
    else:
        lines = ret.split("\n")
        if len(lines) > 20:
            # Show first 10 and last 10 lines
            ret = "\n".join(lines[:10] + ["⋯"] + lines[-10:])

    return (ret, format_hint)
