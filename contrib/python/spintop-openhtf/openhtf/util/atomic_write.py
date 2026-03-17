# Copyright 2016 Google Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utilities for automic_write a new file."""

import os
import tempfile
from contextlib import contextmanager

@contextmanager
def atomic_write(filename, filesync=False):
  """ atomically write a file (using a temporary file).

  filename: the file to be written
  filesync: flush the file to disk
  """

  tmpf = tempfile.NamedTemporaryFile(delete=False)
  try:
    with open(tmpf.name, 'w') as curfile:
      yield curfile
      if filesync:
        curfile.flush()
        os.fsync(curfile.fileno())
    os.rename(tmpf.name, filename)
  finally:
    try:
      os.remove(tmpf.name)
    except (IOError, OSError):
      pass
