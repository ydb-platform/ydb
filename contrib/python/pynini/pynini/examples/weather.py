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

# For general information on the Pynini grammar compilation library, see
# pynini.opengrm.org.
"""Grammar for generating simple weather expressions given tabular input."""

from typing import Dict, NamedTuple

import pynini
from pynini.lib import byte
from pynini.lib import rewrite


class WeatherTuple(NamedTuple):
  """Stores single entry in weather table."""
  state: str
  temperature: int
  wind_direction: str
  wind_speed: int


class WeatherTable:
  """Simple object for holding a set of weather data.

  The table is indexed by city and for each city there is data on temperature,
  wind speed, wind direction, current state of the weather (raining, snowing,
  clear, partly cloudy, etc.).
  """

  _sigma_star = pynini.closure(byte.BYTE).optimize()
  _punct = pynini.union("*", ",", ":", "?", "!").optimize()
  _singularization = pynini.string_map([("degrees", "degree"),
                                        ("kilometers", "kilometer")])
  _singularize = pynini.cdrewrite(_singularization,
                                  pynini.union("[BOS]", " ").concat("1 "),
                                  pynini.union(" ", "[EOS]", _punct),
                                  _sigma_star).optimize()
  _template = ("In $CITY, it is $TEMPERATURE degrees and $STATE, "
               "with winds out of the $WIND_DIRECTION "
               "at $WIND_SPEED kilometers per hour.")

  _table: Dict[str, WeatherTuple]

  def __init__(self) -> None:
    self._table = {}

  def add_city(self, city: str, temperature: int, wind_speed: int,
               wind_direction: str, state: str) -> None:
    self._table[city] = WeatherTuple(state, temperature, wind_direction,
                                     wind_speed)

  @staticmethod
  def sigma_pad(*args: pynini.FstLike) -> pynini.Fst:
    """Helper function that pads a series of arguments with _sigma_star.

    Args:
      args: strings or FSTs.

    Returns:
      _sigma_star + args0 + _sigma_star ... + argsN + _sigma_star
    """
    val = WeatherTable._sigma_star.copy()
    for arg in args:
      val += arg + WeatherTable._sigma_star
    return val.optimize()

  def generate_report(self, city: str) -> str:
    """Generates weather report for the given city.

    Args:
      city: a city string.

    Returns:
      Weather report for the city.
    """
    data = self._table[city]
    populate = WeatherTable.sigma_pad(
        pynini.cross("$CITY", city),
        pynini.cross("$TEMPERATURE", str(data.temperature)),
        pynini.cross("$STATE", data.state),
        pynini.cross("$WIND_DIRECTION", data.wind_direction),
        pynini.cross("$WIND_SPEED", str(data.wind_speed)))
    return rewrite.one_top_rewrite(self._template, populate @ self._singularize)

