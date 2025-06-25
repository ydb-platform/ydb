import re

import _common


def get_coverage_filter_regexp(pattern, cache={}):
    return cache[pattern] if pattern in cache else cache.setdefault(pattern, re.compile(pattern))


def should_be_covered(unit, filters):
    if unit.get("FORCE_COVERAGE_DISABLED") == "yes":
        return False
    if unit.get("FORCE_COVERAGE_ENABLED") == "yes":
        return True
    unit_path = _common.get_norm_unit_path(unit)
    return not any(pred(unit_path) for pred in filters)


def get_coverage_filters(unit, filters=[]):
    # don`t calculate filters if it already was calculated
    if filters:
        return filters

    coverage_target_regexp = unit.get("COVERAGE_TARGET_REGEXP") or None
    coverage_exclude_regexp = unit.get("COVERAGE_EXCLUDE_REGEXP") or None
    if coverage_target_regexp:
        cov_re = get_coverage_filter_regexp(coverage_target_regexp)
        filters.append(lambda x: re.match(cov_re, x) is None)
    if coverage_exclude_regexp:
        cov_exclude_re = get_coverage_filter_regexp(coverage_exclude_regexp)
        filters.append(lambda x: re.match(cov_exclude_re, x) is not None)
    if unit.get("ENABLE_CONTRIB_COVERAGE") != "yes":
        paths_to_exclude = ("contrib",)
        filters.append(lambda x: x.startswith(paths_to_exclude))
    return filters


def onset_cpp_coverage_flags(unit):
    if unit.get("CLANG_COVERAGE") != "yes":
        return
    filters = get_coverage_filters(unit)
    if should_be_covered(unit, filters):
        unit.on_setup_clang_coverage()


def on_filter_py_coverage(unit):
    if unit.get("PYTHON_COVERAGE") != "yes":
        return

    if unit.get("COVERAGE_FILTER_PROGRAMS") != "yes" or unit.get("CYTHON_COVERAGE") == "yes":
        unit.on_enable_python_coverage()
        return

    filters = get_coverage_filters(unit)
    if should_be_covered(unit, filters):
        unit.on_enable_python_coverage()
