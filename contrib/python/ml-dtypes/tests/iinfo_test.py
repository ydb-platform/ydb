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


class IinfoTest(parameterized.TestCase):

  def testIinfoInt4(self):
    info = ml_dtypes.iinfo(ml_dtypes.int4)
    self.assertEqual(info.dtype, ml_dtypes.iinfo("int4").dtype)
    self.assertEqual(info.dtype, ml_dtypes.iinfo(np.dtype("int4")).dtype)
    self.assertEqual(info.min, -8)
    self.assertEqual(info.max, 7)
    self.assertEqual(info.dtype, np.dtype(ml_dtypes.int4))
    self.assertEqual(info.bits, 4)
    self.assertEqual(info.kind, "i")
    self.assertEqual(str(info), "iinfo(min=-8, max=7, dtype=int4)")

  def testIInfoUint4(self):
    info = ml_dtypes.iinfo(ml_dtypes.uint4)
    self.assertEqual(info.dtype, ml_dtypes.iinfo("uint4").dtype)
    self.assertEqual(info.dtype, ml_dtypes.iinfo(np.dtype("uint4")).dtype)
    self.assertEqual(info.min, 0)
    self.assertEqual(info.max, 15)
    self.assertEqual(info.dtype, np.dtype(ml_dtypes.uint4))
    self.assertEqual(info.bits, 4)
    self.assertEqual(info.kind, "u")
    self.assertEqual(str(info), "iinfo(min=0, max=15, dtype=uint4)")

  def testIinfoInt8(self):
    # Checks iinfo succeeds for a built-in NumPy type.
    info = ml_dtypes.iinfo(np.int8)
    self.assertEqual(info.min, -128)
    self.assertEqual(info.max, 127)

  def testIinfoNonInteger(self):
    with self.assertRaises(ValueError):
      ml_dtypes.iinfo(np.float32)
    with self.assertRaises(ValueError):
      ml_dtypes.iinfo(np.complex128)
    with self.assertRaises(ValueError):
      ml_dtypes.iinfo(bool)


if __name__ == "__main__":
  absltest.main()
