import dataclasses
from os.path import commonprefix
from typing import Any, Final

from django.db.models import ManyToManyField, Model
from django.utils.html import conditional_escape
from django.utils.safestring import SafeString, mark_safe
from django.utils.text import capfirst

from .models import HistoricalChanges, ModelChange, ModelChangeValue, ModelDelta
from .utils import get_m2m_reverse_field_name


def conditional_str(obj: Any) -> str:
    """
    Converts ``obj`` to a string, unless it's already one.
    """
    if isinstance(obj, str):
        return obj
    return str(obj)


def is_safe_str(s: Any) -> bool:
    """
    Returns whether ``s`` is a (presumably) pre-escaped string or not.

    This relies on the same ``__html__`` convention as Django's ``conditional_escape``
    does.
    """
    return hasattr(s, "__html__")


class HistoricalRecordContextHelper:
    """
    Class containing various utilities for formatting the template context for
    a historical record.
    """

    DEFAULT_MAX_DISPLAYED_DELTA_CHANGE_CHARS: Final = 100

    def __init__(
        self,
        model: type[Model],
        historical_record: HistoricalChanges,
        *,
        max_displayed_delta_change_chars=DEFAULT_MAX_DISPLAYED_DELTA_CHANGE_CHARS,
    ):
        self.model = model
        self.record = historical_record

        self.max_displayed_delta_change_chars = max_displayed_delta_change_chars

    def context_for_delta_changes(self, delta: ModelDelta) -> list[dict[str, Any]]:
        """
        Return the template context for ``delta.changes``.
        By default, this is a list of dicts with the keys ``"field"``,
        ``"old"`` and ``"new"`` -- corresponding to the fields of ``ModelChange``.

        :param delta: The result from calling ``diff_against()`` with another historical
               record. Its ``old_record`` or ``new_record`` field should have been
               assigned to ``self.record``.
        """
        context_list = []
        for change in delta.changes:
            formatted_change = self.format_delta_change(change)
            context_list.append(
                {
                    "field": formatted_change.field,
                    "old": formatted_change.old,
                    "new": formatted_change.new,
                }
            )
        return context_list

    def format_delta_change(self, change: ModelChange) -> ModelChange:
        """
        Return a ``ModelChange`` object with fields formatted for being used as
        template context.
        """
        old = self.prepare_delta_change_value(change, change.old)
        new = self.prepare_delta_change_value(change, change.new)

        old, new = self.stringify_delta_change_values(change, old, new)

        field_meta = self.model._meta.get_field(change.field)
        return dataclasses.replace(
            change,
            field=capfirst(field_meta.verbose_name),
            old=old,
            new=new,
        )

    def prepare_delta_change_value(
        self,
        change: ModelChange,
        value: ModelChangeValue,
    ) -> Any:
        """
        Return the prepared value for the ``old`` and ``new`` fields of ``change``,
        before it's passed through ``stringify_delta_change_values()`` (in
        ``format_delta_change()``).

        For example, if ``value`` is a list of M2M related objects, it could be
        "prepared" by replacing the related objects with custom string representations.

        :param change:
        :param value: Either ``change.old`` or ``change.new``.
        """
        field_meta = self.model._meta.get_field(change.field)
        if isinstance(field_meta, ManyToManyField):
            reverse_field_name = get_m2m_reverse_field_name(field_meta)
            # Display a list of only the instances of the M2M field's related model
            display_value = [
                obj_values_dict[reverse_field_name] for obj_values_dict in value
            ]
        else:
            display_value = value
        return display_value

    def stringify_delta_change_values(
        self, change: ModelChange, old: Any, new: Any
    ) -> tuple[SafeString, SafeString]:
        """
        Called by ``format_delta_change()`` after ``old`` and ``new`` have been
        prepared by ``prepare_delta_change_value()``.

        Return a tuple -- ``(old, new)`` -- where each element has been
        escaped/sanitized and turned into strings, ready to be displayed in a template.
        These can be HTML strings (remember to pass them through ``mark_safe()`` *after*
        escaping).

        If ``old`` or ``new`` are instances of ``list``, the default implementation will
        use each list element's ``__str__()`` method, and also reapply ``mark_safe()``
        if all the passed elements are safe strings.
        """

        def stringify_value(value: Any) -> str | SafeString:
            # If `value` is a list, stringify each element using `str()` instead of
            # `repr()` (the latter is the default when calling `list.__str__()`)
            if isinstance(value, list):
                string = f"[{', '.join(map(conditional_str, value))}]"
                # If all elements are safe strings, reapply `mark_safe()`
                if all(map(is_safe_str, value)):
                    string = mark_safe(string)  # nosec
            else:
                string = conditional_str(value)
            return string

        old_str, new_str = stringify_value(old), stringify_value(new)
        diff_display = self.get_obj_diff_display()
        old_short, new_short = diff_display.common_shorten_repr(old_str, new_str)
        # Escape *after* shortening, as any shortened, previously safe HTML strings have
        # likely been mangled. Other strings that have not been shortened, should have
        # their "safeness" unchanged.
        return conditional_escape(old_short), conditional_escape(new_short)

    def get_obj_diff_display(self) -> "ObjDiffDisplay":
        """
        Return an instance of ``ObjDiffDisplay`` that will be used in
        ``stringify_delta_change_values()`` to display the difference between
        the old and new values of a ``ModelChange``.
        """
        return ObjDiffDisplay(max_length=self.max_displayed_delta_change_chars)


class ObjDiffDisplay:
    """
    A class grouping functions and settings related to displaying the textual
    difference between two (or more) objects.
    ``common_shorten_repr()`` is the main method for this.

    The code is based on
    https://github.com/python/cpython/blob/v3.12.0/Lib/unittest/util.py#L8-L52.
    """

    def __init__(
        self,
        *,
        max_length=80,
        placeholder_len=12,
        min_begin_len=5,
        min_end_len=5,
        min_common_len=5,
    ):
        self.max_length = max_length
        self.placeholder_len = placeholder_len
        self.min_begin_len = min_begin_len
        self.min_end_len = min_end_len
        self.min_common_len = min_common_len
        self.min_diff_len = max_length - (
            min_begin_len
            + placeholder_len
            + min_common_len
            + placeholder_len
            + min_end_len
        )
        assert self.min_diff_len >= 0  # nosec

    def common_shorten_repr(self, *args: Any) -> tuple[str, ...]:
        """
        Returns ``args`` with each element converted into a string representation.
        If any of the strings are longer than ``self.max_length``, they're all shortened
        so that the first differences between the strings (after a potential common
        prefix in all of them) are lined up.
        """
        args = tuple(map(conditional_str, args))
        max_len = max(map(len, args))
        if max_len <= self.max_length:
            return args

        prefix = commonprefix(args)
        prefix_len = len(prefix)

        common_len = self.max_length - (
            max_len - prefix_len + self.min_begin_len + self.placeholder_len
        )
        if common_len > self.min_common_len:
            assert (
                self.min_begin_len
                + self.placeholder_len
                + self.min_common_len
                + (max_len - prefix_len)
                < self.max_length
            )  # nosec
            prefix = self.shorten(prefix, self.min_begin_len, common_len)
            return tuple(f"{prefix}{s[prefix_len:]}" for s in args)

        prefix = self.shorten(prefix, self.min_begin_len, self.min_common_len)
        return tuple(
            prefix + self.shorten(s[prefix_len:], self.min_diff_len, self.min_end_len)
            for s in args
        )

    def shorten(self, s: str, prefix_len: int, suffix_len: int) -> str:
        skip = len(s) - prefix_len - suffix_len
        if skip > self.placeholder_len:
            suffix_index = len(s) - suffix_len
            s = self.shortened_str(s[:prefix_len], skip, s[suffix_index:])
        return s

    def shortened_str(self, prefix: str, num_skipped_chars: int, suffix: str) -> str:
        """
        Return a shortened version of the string representation of one of the args
        passed to ``common_shorten_repr()``.
        This should be in the format ``f"{prefix}{skip_str}{suffix}"``, where
        ``skip_str`` is a string indicating how many characters (``num_skipped_chars``)
        of the string representation were skipped between ``prefix`` and ``suffix``.
        """
        return f"{prefix}[{num_skipped_chars:d} chars]{suffix}"
