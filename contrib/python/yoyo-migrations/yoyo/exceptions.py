# Copyright 2015 Oliver Cope
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

DatabaseErrors = []


def register(exception_class):
    DatabaseErrors.append(exception_class)


class BadMigration(Exception):
    """
    The migration file could not be compiled
    """


class MigrationConflict(Exception):
    """
    The migration id conflicts with another migration
    """


class LockTimeout(Exception):
    """
    Timeout was reached while acquiring the migration lock
    """
