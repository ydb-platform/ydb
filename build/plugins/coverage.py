import re

import _common
import lib.test_const as consts


def get_coverage_filter_regexp(pattern, cache={}):
    return cache[pattern] if pattern in cache else cache.setdefault(pattern, re.compile(pattern))


def should_be_covered(unit, filters):
    if unit.get("FORCE_COVERAGE_DISABLED") == "yes":
        return False
    if unit.get("FORCE_COVERAGE_ENABLED") == "yes":
        return True
    unit_path = _common.get_norm_unit_path(unit)
    return not any(pred(unit_path) for pred in filters)


def get_cpp_coverage_filters(unit, filters=[]):
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


def add_cpp_coverage_ldflags(unit):
    ldflags = unit.get("LDFLAGS")
    changed = False
    for flag in consts.COVERAGE_LDFLAGS:
        if flag not in ldflags:
            ldflags = ldflags + ' ' + flag
            changed = True
    if changed:
        unit.set(["LDFLAGS", ldflags])


def add_cpp_coverage_cflags(unit):
    cflags = unit.get("CFLAGS")
    changed = False
    for flag in consts.COVERAGE_CFLAGS:
        if flag not in cflags:
            cflags = cflags + ' ' + flag
            changed = True
    if changed:
        unit.set(["CFLAGS", cflags])


def onset_cpp_coverage_flags(unit):
    if unit.get("CLANG_COVERAGE") == "no":
        return
    filters = get_cpp_coverage_filters(unit)
    if should_be_covered(unit, filters):
        add_cpp_coverage_cflags(unit)
        add_cpp_coverage_ldflags(unit)
