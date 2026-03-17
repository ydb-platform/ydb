# Copyright 2022 The ml_dtypes Authors.
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

from absl.testing import absltest
from absl.testing import parameterized
import ml_dtypes
import numpy as np

ALL_DTYPES = [
    ml_dtypes.bfloat16,
    ml_dtypes.float8_e4m3b11fnuz,
    ml_dtypes.float8_e4m3fn,
    ml_dtypes.float8_e4m3fnuz,
    ml_dtypes.float8_e5m2,
    ml_dtypes.float8_e5m2fnuz,
]

DTYPES_WITH_NO_INFINITY = [
    ml_dtypes.float8_e4m3b11fnuz,
    ml_dtypes.float8_e4m3fn,
    ml_dtypes.float8_e4m3fnuz,
    ml_dtypes.float8_e5m2fnuz,
]

UINT_TYPES = {
    8: np.uint8,
    16: np.uint16,
}


class FinfoTest(parameterized.TestCase):

  def assertNanEqual(self, x, y):
    if np.isnan(x) and np.isnan(y):
      return
    self.assertEqual(x, y)

  @parameterized.named_parameters(
      {"testcase_name": f"_{dtype.__name__}", "dtype": np.dtype(dtype)}
      for dtype in ALL_DTYPES
  )
  def testFInfo(self, dtype):
    info = ml_dtypes.finfo(dtype)

    assert ml_dtypes.finfo(dtype.name) is info
    assert ml_dtypes.finfo(dtype.type) is info

    _ = str(info)  # doesn't crash

    def make_val(val):
      return np.array(val, dtype=dtype)

    def assert_representable(val):
      self.assertEqual(make_val(val).item(), val)

    def assert_infinite(val):
      val = make_val(val)
      if dtype in DTYPES_WITH_NO_INFINITY:
        self.assertTrue(np.isnan(val), f"expected NaN, got {val}")
      else:
        self.assertTrue(np.isposinf(val), f"expected inf, got {val}")

    def assert_zero(val):
      self.assertEqual(make_val(val), make_val(0))

    self.assertEqual(np.array(0, dtype).dtype, dtype)
    self.assertIs(info.dtype, dtype)

    self.assertEqual(info.bits, np.array(0, dtype).itemsize * 8)
    self.assertEqual(info.nmant + info.nexp + 1, info.bits)

    assert_representable(info.tiny)

    assert_representable(info.max)
    assert_infinite(np.spacing(info.max))

    assert_representable(info.min)
    assert_infinite(-np.spacing(info.min))

    assert_representable(2.0 ** (info.maxexp - 1))
    assert_infinite(2.0**info.maxexp)

    assert_representable(info.smallest_subnormal)
    assert_zero(info.smallest_subnormal * 0.5)
    self.assertEqual(info.tiny, info.smallest_normal)

    # Identities according to the documentation:
    np.testing.assert_allclose(info.resolution, make_val(10**-info.precision))
    self.assertEqual(info.epsneg, make_val(2**info.negep))
    self.assertEqual(info.eps, make_val(2**info.machep))
    self.assertEqual(info.iexp, info.nexp)

    # Check that minexp is consistent with nmant
    self.assertEqual(
        make_val(2**info.minexp).view(UINT_TYPES[info.bits]),
        2**info.nmant,
    )


if __name__ == "__main__":
  absltest.main()
