# Copyright 2024 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class DeletedRecords:
    """
    DeletedRecords
    Represents information about deleted records.

    Parameters
    ----------
    low_watermark: int
        The "low watermark" for the topic partition on which the deletion was executed.
    """
    def __init__(self, low_watermark):
        self.low_watermark = low_watermark
