# Copyright 2023-2025 Buf Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from protovalidate import config, validator

Config = config.Config
Validator = validator.Validator
CompilationError = validator.CompilationError
ValidationError = validator.ValidationError
Violations = validator.Violations

_default_validator = Validator()
validate = _default_validator.validate
collect_violations = _default_validator.collect_violations

__all__ = ["CompilationError", "Config", "ValidationError", "Validator", "Violations", "collect_violations", "validate"]
