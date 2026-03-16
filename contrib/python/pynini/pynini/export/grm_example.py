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

"""Test pynini file using the grm."""

import pynini
from pynini.export import grm


def generator_main(exporter: grm.Exporter):
  exporter['FST1'] = pynini.accep('1234')
  exporter['FST2'] = pynini.accep('4321')
  exporter['FST3'] = pynini.accep('ABCD')


if __name__ == '__main__':
  grm.run(generator_main)

