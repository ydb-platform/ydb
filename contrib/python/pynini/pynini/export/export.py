# Copyright 2016-2020 Google LLC
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
#

"""Helper classes for generating FAR files with Pynini."""

import os
from typing import Union

import logging

import pynini

_Filename = Union[str, os.PathLike]


class Exporter:
  """Helper class to collect and export FSTs into an FAR archive file.

  For example, the following code collects 2 FSTs and writes them upon
  destruction of the involved exporter instance.

    exporter = grm.Exporter(filename)
    exporter.export("FST1", fst1)
    exporter.export("FST2", fst2)

  Typically, instead of explicitly creating an Exporter, a client will use the
  exporter provided as the argument to the generator_main function.
  """

  def __init__(self,
               filename: _Filename,
               arc_type: str = "standard",
               far_type: pynini.FarType = "default") -> None:
    """Creates an exporter that writes a FAR archive file upon destruction.

    Args:
      filename: A string with the filename.
      arc_type: A string with the arc type; one of: "standard", "log", "log64".
      far_type: A string with the file type; one of: "default", "sstable",
          "sttable", "stlist".
    """
    logging.info("Setting up exporter for %r", filename)
    self._fsts = {}
    self._filename = os.fspath(filename)
    self._arc_type = arc_type
    self._far_type = far_type
    self._is_open = True

  def __setitem__(self, name: str, fst: pynini.Fst) -> None:
    """Register an FST under a given name to be saved into the FST archive.

    Args:
      name: A string with the name of the fst.
      fst: An FST to be stored.
    """
    assert self._is_open
    logging.info("Adding FST %r to archive %r", name, self._filename)
    self._fsts[name] = fst

  def close(self) -> None:
    """Writes the registered FSTs into the given file and closes it."""
    assert self._is_open
    logging.info("Writing FSTs into %r", self._filename)
    # TODO(b/123775699): Currently pytype is unable to resolve
    # the usage of typing.Literal for pynini.Far.__init__'s far_type, producing
    # the error:
    #
    #  Expected: (self, filename, mode, arc_type, far_type: Literal[str] = ...)
    #  Actually passed: (self, filename, mode, arc_type, far_type: Literal[str])
    #
    # Once typing.Literal support no longer makes this error, drop
    # the below pytype disable comment.
    with pynini.Far(
        self._filename, "w", arc_type=self._arc_type,
        far_type=self._far_type) as sink:  # pytype: disable=wrong-arg-types
      for name in sorted(self._fsts):
        logging.info("Writing FST %r to %r", name, self._filename)
        sink[name] = self._fsts[name]
    logging.info("Writing FSTs to %r complete", self._filename)
    self._is_open = False

