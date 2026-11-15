import re

import _common
from ymake import macro, Unit


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
        if unit.get("ENABLE_CONTRIB_YDB_COVERAGE") == "yes":
            pattern = r'^contrib/(?!ydb)'
            filters.append(lambda x: re.match(pattern, x) is not None)
        else:
            paths_to_exclude = ("contrib",)
            filters.append(lambda x: x.startswith(paths_to_exclude))
    return filters


@macro
def SET_CPP_COVERAGE_FLAGS(unit: Unit):
    if unit.get("CLANG_COVERAGE") != "yes":
        return
    filters = get_coverage_filters(unit)
    if should_be_covered(unit, filters):
        unit.on_setup_clang_coverage()


@macro
def _FILTER_PY_COVERAGE(unit: Unit):
    if unit.get("PYTHON_COVERAGE") != "yes":
        return

    if unit.get("COVERAGE_FILTER_PROGRAMS") != "yes" or unit.get("CYTHON_COVERAGE") == "yes":
        unit.on_enable_python_coverage()
        return

    filters = get_coverage_filters(unit)
    if should_be_covered(unit, filters):
        unit.on_enable_python_coverage()


@macro
def _FILTER_JAVA_COVERAGE(unit: Unit):
    """
    Decide what a jar module emits for Java coverage.
    Without JAVA_COVERAGE_SOURCE_FILTER every module emits the .cpsf source list and every class is
    covered. With the filter only the modules whose path matches it emit .cpsf and, in addition, the
    .covcls class list that narrows the jacoco agent and the coverage report down to their classes;
    the other modules keep the regular build command, so their outputs stay cache-compatible with
    a build without coverage.
    The filter is separate from COVERAGE_TARGET_REGEXP, which for Java is also used as a package
    prefix of the report, so a path cannot be told from a package there.
    """
    if unit.get("JAVA_COVERAGE") != "yes":
        return
    source_filter = unit.get("JAVA_COVERAGE_SOURCE_FILTER")
    if not source_filter:
        unit.on_enable_java_coverage_srclist()
    elif re.match(get_coverage_filter_regexp(source_filter), _common.get_norm_unit_path(unit)):
        unit.on_enable_java_coverage_srclist()
        unit.on_enable_java_coverage_classlist()
