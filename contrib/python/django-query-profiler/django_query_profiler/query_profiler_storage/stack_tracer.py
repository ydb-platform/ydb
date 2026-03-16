"""
This module contains one public function - for finding the stack_trace partitioned into a tuple of two items:
1. app_stack_trace
2. django_stack_trace
"""

import inspect
from collections import defaultdict
from typing import Dict, List, Tuple

from . import StackTraceElement

'''
These following are 2-D maps.  The first entry is for the app_module_names_to_exclude/django_module_names_to_include
and the second key in the map is the moduleName.  True means that we should include it.  The reason for having a
2-D matrix is to allow passing different variants of app_module_names_to_exclude, django_module_names_to_include
list in the same run
'''
MEMO_DJANGO_MODULE_NAME_DECISION: Dict[Tuple[str], Dict[str, bool]] = defaultdict(dict)
MEMO_APP_MODULE_NAME_DECISION: Dict[Tuple[str], Dict[str, bool]] = defaultdict(dict)


def find_stack_trace(app_module_names_to_exclude: Tuple[str], django_module_names_to_include: Tuple[str],
                     max_depth: int) -> Tuple[Tuple[StackTraceElement], Tuple[StackTraceElement]]:
    """
    This function finds the stack trace from the frame and returns a tuple of application & django stack-trace
    The exclusion and inclusion list should be mutually exclusive (they would be in practice), but if they are not -
    the inclusion list takes precedence & it would be included in django_stack_trace
    """
    app_stack_trace: List[StackTraceElement] = []
    django_stack_trace: List[StackTraceElement] = []
    if max_depth <= 0:
        return tuple(app_stack_trace), tuple(django_stack_trace)

    current_iteration: int = 0
    current_frame = inspect.currentframe()
    try:
        while current_iteration < max_depth and current_frame is not None:
            module_name = _module_name_from_frame(current_frame)
            '''
            We would fist check if the stack_trace can be included in django stack trace because that list is the
            include list.  If it can be included in django one, we would not add it to app stack_trace list
            If not, we would check if it can be included in the app list based on the passed exclusion list
            '''

            # Checking django
            is_django_stack_trace = MEMO_DJANGO_MODULE_NAME_DECISION[django_module_names_to_include].get(module_name)
            if is_django_stack_trace is None:
                is_django_stack_trace = any(module_name.startswith(app) for app in django_module_names_to_include)
            MEMO_DJANGO_MODULE_NAME_DECISION[django_module_names_to_include][module_name] = is_django_stack_trace

            # Checking app
            if is_django_stack_trace:
                is_app_stack_trace = False
            else:
                is_app_stack_trace = MEMO_APP_MODULE_NAME_DECISION[app_module_names_to_exclude].get(module_name)
                if is_app_stack_trace is None:
                    is_app_stack_trace = not (any(module_name.startswith(app) for app in app_module_names_to_exclude))
            MEMO_APP_MODULE_NAME_DECISION[app_module_names_to_exclude][module_name] = is_app_stack_trace

            if is_app_stack_trace or is_django_stack_trace:
                function_name = _function_name_from_frame(current_frame)
                if is_app_stack_trace:
                    stack_trace = StackTraceElement.app_stacktrace_element(
                        module_name=module_name,
                        function_name=function_name,
                        line_number=_line_number_from_frame(current_frame))
                    app_stack_trace.append(stack_trace)
                elif is_django_stack_trace:
                    stack_trace = StackTraceElement.django_stacktrace_element(
                        module_name=module_name,
                        function_name=function_name)
                    django_stack_trace.append(stack_trace)

            current_frame = current_frame.f_back
            current_iteration += 1
    finally:
        del current_frame
    return tuple(app_stack_trace), tuple(django_stack_trace)


def _module_name_from_frame(frame):
    return frame.f_globals['__name__']


def _function_name_from_frame(frame):
    return frame.f_code.co_name


def _line_number_from_frame(frame):
    return frame.f_lineno
