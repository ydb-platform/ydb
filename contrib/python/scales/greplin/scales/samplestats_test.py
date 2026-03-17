# Copyright 2011 The scales Authors.
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

"""Sample statistics tests."""

from greplin.scales.samplestats import UniformSample, ExponentiallyDecayingReservoir
import random
import unittest

class UniformSampleTest(unittest.TestCase):
  """Test cases for uniform sample stats."""

  def testGaussian(self):
    """Test with gaussian random numbers."""
    random.seed(42)

    us = UniformSample()
    for _ in range(300):
      us.update(random.gauss(42.0, 13.0))
    self.assertAlmostEqual(us.mean, 43.143067271195235, places=5)
    self.assertAlmostEqual(us.stddev, 13.008553229943168, places=5)

    us.clear()
    for _ in range(30000):
      us.update(random.gauss(0.0012, 0.00005))
    self.assertAlmostEqual(us.mean, 0.0012015284549517493, places=5)
    self.assertAlmostEqual(us.stddev, 4.9776450250869146e-05, places=5)


class ExponentiallyDecayingReservoirTest(unittest.TestCase):
  """Test cases for exponentially decaying reservoir sample stats."""

  def testGaussian(self):
    """Test with gaussian random numbers."""
    random.seed(42)

    sample = ExponentiallyDecayingReservoir()
    for _ in range(300):
      sample.update(random.gauss(42.0, 13.0))
    self.assertAlmostEqual(sample.mean, 41.974069434931714, places=5)
    self.assertAlmostEqual(sample.stddev, 12.982363860393766, places=5)


  def testWithRescale(self):
    """Excercise rescaling."""
    # Not a good test, but at least we cover a little more of the code.
    random.seed(42)

    sample = ExponentiallyDecayingReservoir(rescale_threshold=-1)
    sample.update(random.gauss(42.0, 13.0))
    self.assertAlmostEqual(sample.mean, 40.12682571548693, places=5)





if __name__ == '__main__':
  unittest.main()
