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

"""Interface for generating multiple FAR files from a Pynini file.

Given a number of output FAR files (FST archive) via --output in the form
designator1=file1,designator2=file2,... a Pynini file produces the FAR files
file1, file2,...  by exporting a number of FSTs into these FAR files. To this
end, a Pynini file creates a generator_main file with the signature:

  def generator_main(exporter_map: multi_grm.ExporterMapping):

which can contain lines of the form

  my_exporter = exporter_map[designator]
  my_exporter[fst_name] = fst

to export a given FST under a corresponding name into the file identified by the
given designator. The generator main function should be have a:

  if __name__ == '__main__':
    multi_grm.run(generator_main)

For an example, see multi_grm_example.py.
"""

from typing import Callable, Mapping

from absl import app
from absl import flags
import logging

from pynini.export import export


flags.DEFINE_string('outputs', None,
                    ('The output FAR files in the form '
                     'designator1=file1,designator2=file2,...'))
FLAGS = flags.FLAGS

ExporterMapping = Mapping[str, export.Exporter]


def _get_target_file_map(target_file_pairs: str) -> Mapping[str, str]:
  """Generates a map from a list designator1=file1,designator2=file2,...

  Args:
    target_file_pairs: A string of the form
                       designator1=file1,designator2=file2,...
  Returns:
    A map from designators to file names.
  Raises:
    app.UsageError: If there is a parsing error. Does not check whether the
      files can be written.
  """
  result = {}
  for target_file_pair in target_file_pairs.split(','):
    target_file_list = target_file_pair.split('=')
    if len(target_file_list) != 2:
      raise app.UsageError('--outputs must be of form '
                           'designator1=file1,designator2=file2,...')
    result[target_file_list[0]] = target_file_list[1]
    logging.info('Setting up target designator [%s] with file [%s].',
                 target_file_list[0], target_file_list[1])
  return result


def run(generator_main: Callable[[ExporterMapping], None]) -> None:
  """Executes the multi_grm FAR export program to export a number of FARs.

  Args:
    generator_main: The FAR generator_main function to execute. It takes a
      single argument "exporter_map", which is a map from string designators to
      export.Exporter objects with a __setitem__ method.
  """

  def main(unused_argv):
    try:
      unused_argv.pop(0)
      if unused_argv:
        raise app.UsageError(
            f'Unexpected command line arguments: {unused_argv}')
      target_file_pair = _get_target_file_map(FLAGS.outputs)
      if not target_file_pair:
        raise app.UsageError(
            '--outputs must specify at least one name=file pair.')
      exporter_map = {
          designator: export.Exporter(filename)
          for designator, filename in target_file_pair.items()
      }
      generator_main(exporter_map)
      for ex in exporter_map.values():
        ex.close()
    except:
      FLAGS.stderrthreshold = 'fatal'
      raise

  flags.mark_flag_as_required('outputs')
  FLAGS.stderrthreshold = 'warning'
  app.run(main)

