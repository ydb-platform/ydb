import pytest
from itertools import repeat
from allure_commons.utils import SafeFormatter, md5
from allure_commons.utils import format_exception, format_traceback
from allure_commons.model2 import Status
from allure_commons.model2 import StatusDetails
from allure_commons.types import LabelType
from allure_pytest.stash import stashed

ALLURE_DESCRIPTION_MARK = 'allure_description'
ALLURE_DESCRIPTION_HTML_MARK = 'allure_description_html'
ALLURE_LABEL_MARK = 'allure_label'
ALLURE_LINK_MARK = 'allure_link'
ALLURE_UNIQUE_LABELS = [
    LabelType.SEVERITY,
    LabelType.FRAMEWORK,
    LabelType.HOST,
    LabelType.SUITE,
    LabelType.PARENT_SUITE,
    LabelType.SUB_SUITE
]

MARK_NAMES_TO_IGNORE = {
    "usefixtures",
    "filterwarnings",
    "skip",
    "skipif",
    "xfail",
    "parametrize",
}


class ParsedPytestNodeId:
    def __init__(self, nodeid):
        filepath, *class_names, function_segment = ensure_len(nodeid.split("::"), 2)
        self.filepath = filepath
        self.path_segments = filepath.split('/')
        *parent_dirs, filename = ensure_len(self.path_segments, 1)
        self.parent_package = '.'.join(parent_dirs)
        self.module = filename.rsplit(".", 1)[0]
        self.package = '.'.join(filter(None, [self.parent_package, self.module]))
        self.class_names = class_names
        self.test_function = function_segment.split("[", 1)[0]


@stashed
def parse_nodeid(item):
    return ParsedPytestNodeId(item.nodeid)


def get_marker_value(item, keyword):
    marker = item.get_closest_marker(keyword)
    return marker.args[0] if marker and marker.args else None


def allure_title(item):
    return getattr(
        getattr(item, "obj", None),
        "__allure_display_name__",
        None
    )


def allure_description(item):
    description = get_marker_value(item, ALLURE_DESCRIPTION_MARK)
    if description:
        return description
    elif hasattr(item, 'function'):
        return item.function.__doc__


def allure_description_html(item):
    return get_marker_value(item, ALLURE_DESCRIPTION_HTML_MARK)


def allure_label(item, label):
    labels = []
    for mark in item.iter_markers(name=ALLURE_LABEL_MARK):
        if mark.kwargs.get("label_type") == label:
            labels.extend(mark.args)
    return labels


def allure_labels(item):
    unique_labels = dict()
    labels = set()
    for mark in item.iter_markers(name=ALLURE_LABEL_MARK):
        label_type = mark.kwargs["label_type"]
        if label_type in ALLURE_UNIQUE_LABELS:
            if label_type not in unique_labels.keys():
                unique_labels[label_type] = mark.args[0]
        else:
            for arg in mark.args:
                labels.add((label_type, arg))
    for k, v in unique_labels.items():
        labels.add((k, v))
    return labels


def allure_links(item):
    for mark in item.iter_markers(name=ALLURE_LINK_MARK):
        yield (mark.kwargs["link_type"], mark.args[0], mark.kwargs["name"])


def format_allure_link(config, url, link_type):
    pattern = dict(config.option.allure_link_pattern).get(link_type, '{}')
    return pattern.format(url)


def pytest_markers(item):
    for mark in item.iter_markers():
        if should_convert_mark_to_tag(mark):
            yield mark.name


def should_convert_mark_to_tag(mark):
    return mark.name not in MARK_NAMES_TO_IGNORE and \
        not mark.args and not mark.kwargs


def allure_package(item):
    return parse_nodeid(item).package


def allure_name(item, parameters, param_id=None):
    name = item.name
    title = allure_title(item)
    param_id_kwargs = {}
    if param_id:
        # if param_id is an ASCII string, it could have been encoded by pytest (_pytest.compat.ascii_escaped)
        if param_id.isascii():
            param_id = param_id.encode().decode("unicode-escape")
        param_id_kwargs["param_id"] = param_id
    return SafeFormatter().format(
        title,
        **{**param_id_kwargs, **parameters, **item.funcargs}
    ) if title else name


def allure_full_name(item: pytest.Item):
    nodeid = parse_nodeid(item)
    class_part = ("." + ".".join(nodeid.class_names)) if nodeid.class_names else ""
    test = item.originalname if isinstance(item, pytest.Function) else nodeid.test_function
    full_name = f"{nodeid.package}{class_part}#{test}"
    return full_name


def allure_title_path(item):
    nodeid = parse_nodeid(item)
    return list(
        filter(None, [*nodeid.path_segments, *nodeid.class_names]),
    )


def ensure_len(value, min_length, fill_value=None):
    yield from value
    yield from repeat(fill_value, min_length - len(value))


def allure_suite_labels(item):
    nodeid = parse_nodeid(item)

    default_suite_labels = {
        LabelType.PARENT_SUITE: nodeid.parent_package,
        LabelType.SUITE: nodeid.module,
        LabelType.SUB_SUITE: " > ".join(nodeid.class_names),
    }

    existing_labels = dict(allure_labels(item))
    resolved_default_suite_labels = []
    for label, value in default_suite_labels.items():
        if label not in existing_labels and value:
            resolved_default_suite_labels.append((label, value))

    return resolved_default_suite_labels


def get_outcome_status(outcome):
    _, exception, _ = outcome.excinfo or (None, None, None)
    return get_status(exception)


def get_outcome_status_details(outcome):
    exception_type, exception, exception_traceback = outcome.excinfo or (None, None, None)
    return get_status_details(exception_type, exception, exception_traceback)


def get_status(exception):
    if exception:
        if isinstance(exception, AssertionError) or isinstance(exception, pytest.fail.Exception):
            return Status.FAILED
        elif isinstance(exception, pytest.skip.Exception):
            return Status.SKIPPED
        return Status.BROKEN
    else:
        return Status.PASSED


def get_status_details(exception_type, exception, exception_traceback):
    message = format_exception(exception_type, exception)
    trace = format_traceback(exception_traceback)
    return StatusDetails(message=message, trace=trace) if message or trace else None


def get_pytest_report_status(pytest_report):
    pytest_statuses = ('failed', 'passed', 'skipped')
    statuses = (Status.FAILED, Status.PASSED, Status.SKIPPED)
    for pytest_status, status in zip(pytest_statuses, statuses):
        if getattr(pytest_report, pytest_status):
            return status


def get_history_id(full_name, parameters, original_values):
    return md5(
        full_name,
        *(original_values.get(p.name, p.value) for p in sorted(
            filter(
                lambda p: not p.excluded,
                parameters
            ),
            key=lambda p: p.name
        ))
    )
