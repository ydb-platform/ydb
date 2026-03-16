# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

import re

from .base import _ErrorOverride
from .platform import ResourceDoesNotExist

_ALL_OVERRIDES = [
    _ErrorOverride(
        debug_name="Clusters InvalidParameterValue=>ResourceDoesNotExist",
        path_regex=re.compile(r"^/api/2\.\d/clusters/get"),
        verb="GET",
        status_code_matcher=re.compile(r"^400$"),
        error_code_matcher=re.compile(r"INVALID_PARAMETER_VALUE"),
        message_matcher=re.compile(r"Cluster .* does not exist"),
        custom_error=ResourceDoesNotExist,
    ),
    _ErrorOverride(
        debug_name="Jobs InvalidParameterValue=>ResourceDoesNotExist",
        path_regex=re.compile(r"^/api/2\.\d/jobs/get"),
        verb="GET",
        status_code_matcher=re.compile(r"^400$"),
        error_code_matcher=re.compile(r"INVALID_PARAMETER_VALUE"),
        message_matcher=re.compile(r"Job .* does not exist"),
        custom_error=ResourceDoesNotExist,
    ),
    _ErrorOverride(
        debug_name="Job Runs InvalidParameterValue=>ResourceDoesNotExist",
        path_regex=re.compile(r"^/api/2\.\d/jobs/runs/get"),
        verb="GET",
        status_code_matcher=re.compile(r"^400$"),
        error_code_matcher=re.compile(r"INVALID_PARAMETER_VALUE"),
        message_matcher=re.compile(r"(Run .* does not exist|Run: .* in job: .* doesn\'t exist)"),
        custom_error=ResourceDoesNotExist,
    ),
]
