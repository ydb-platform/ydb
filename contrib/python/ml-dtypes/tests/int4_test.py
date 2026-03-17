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

"""Test cases for int4 types."""

import contextlib
import copy
import operator
import pickle
import warnings

from absl.testing import absltest
from absl.testing import parameterized

import ml_dtypes

import numpy as np

int4 = ml_dtypes.int4
uint4 = ml_dtypes.uint4

INT4_TYPES = [int4, uint4]

VALUES = {
    int4: list(range(-8, 8)),
    uint4: list(range(0, 16)),
}


@contextlib.contextmanager
def ignore_warning(**kw):
  with warnings.catch_warnings():
    warnings.filterwarnings("ignore", **kw)
    yield


# Tests for the Python scalar type
class ScalarTest(parameterized.TestCase):

  @parameterized.product(scalar_type=INT4_TYPES)
  def testModuleName(self, scalar_type):
    self.assertEqual(scalar_type.__module__, "ml_dtypes")

  @parameterized.product(scalar_type=INT4_TYPES)
  def testPickleable(self, scalar_type):
    # https://github.com/google/jax/discussions/8505
    x = np.arange(10, dtype=scalar_type)
    serialized = pickle.dumps(x)
    x_out = pickle.loads(serialized)
    self.assertEqual(x_out.dtype, x.dtype)
    np.testing.assert_array_equal(x_out.astype(int), x.astype(int))

  @parameterized.product(
      scalar_type=INT4_TYPES,
      python_scalar=[int, float, np.float16, np.longdouble],
  )
  def testRoundTripToPythonScalar(self, scalar_type, python_scalar):
    for v in VALUES[scalar_type]:
      self.assertEqual(v, scalar_type(v))
      self.assertEqual(python_scalar(v), python_scalar(scalar_type(v)))
      self.assertEqual(
          scalar_type(v), scalar_type(python_scalar(scalar_type(v)))
      )

  @parameterized.product(scalar_type=INT4_TYPES)
  def testRoundTripNumpyTypes(self, scalar_type):
    for dtype in [np.int8, np.int32]:
      for f in VALUES[scalar_type]:
        self.assertEqual(dtype(f), dtype(scalar_type(dtype(f))))
        self.assertEqual(int(dtype(f)), int(scalar_type(dtype(f))))
        self.assertEqual(dtype(f), dtype(scalar_type(np.array(f, dtype))))

      np.testing.assert_equal(
          dtype(np.array(VALUES[scalar_type], scalar_type)),
          np.array(VALUES[scalar_type], dtype),
      )

  @parameterized.product(scalar_type=INT4_TYPES)
  def testStr(self, scalar_type):
    for value in VALUES[scalar_type]:
      self.assertEqual(str(value), str(scalar_type(value)))

  @parameterized.product(scalar_type=INT4_TYPES)
  def testRepr(self, scalar_type):
    for value in VALUES[scalar_type]:
      self.assertEqual(str(value), str(scalar_type(value)))

  @parameterized.product(scalar_type=INT4_TYPES)
  def testItem(self, scalar_type):
    self.assertIsInstance(scalar_type(3).item(), int)
    self.assertEqual(scalar_type(3).item(), 3)

  @parameterized.product(scalar_type=INT4_TYPES)
  def testHash(self, scalar_type):
    for v in VALUES[scalar_type]:
      self.assertEqual(hash(v), hash(scalar_type(v)), msg=v)

  @parameterized.product(
      scalar_type=INT4_TYPES,
      op=[
          operator.le,
          operator.lt,
          operator.eq,
          operator.ne,
          operator.ge,
          operator.gt,
      ],
  )
  def testComparison(self, scalar_type, op):
    for v in VALUES[scalar_type]:
      for w in VALUES[scalar_type]:
        result = op(scalar_type(v), scalar_type(w))
        self.assertEqual(op(v, w), result)
        self.assertIsInstance(result, np.bool_)

  @parameterized.product(
      scalar_type=INT4_TYPES,
      op=[
          operator.neg,
          operator.pos,
      ],
  )
  def testUnop(self, scalar_type, op):
    for v in VALUES[scalar_type]:
      out = op(scalar_type(v))
      self.assertIsInstance(out, scalar_type)
      self.assertEqual(scalar_type(op(v)), out, msg=v)

  @parameterized.product(
      scalar_type=INT4_TYPES,
      op=[
          operator.add,
          operator.sub,
          operator.mul,
          operator.floordiv,
          operator.mod,
      ],
  )
  def testBinop(self, scalar_type, op):
    for v in VALUES[scalar_type]:
      for w in VALUES[scalar_type]:
        if w == 0 and op in [operator.floordiv, operator.mod]:
          with self.assertRaises(ZeroDivisionError):
            op(scalar_type(v), scalar_type(w))
        else:
          out = op(scalar_type(v), scalar_type(w))
          self.assertIsInstance(out, scalar_type)
          self.assertEqual(scalar_type(op(v, w)), out, msg=(v, w))

  @parameterized.product(
      scalar_type=INT4_TYPES,
      dtype=[
          np.float16,
          np.float32,
          np.float64,
          np.int8,
          np.int16,
          np.int32,
          np.int64,
          np.complex64,
          np.complex128,
          np.uint8,
          np.uint16,
          np.uint32,
          np.uint64,
          np.intc,
          np.int_,
          np.longlong,
          np.uintc,
          np.ulonglong,
      ],
  )
  def testCanCast(self, scalar_type, dtype):
    allowed_casts = [
        (np.bool_, int4),
        (int4, np.int8),
        (int4, np.int16),
        (int4, np.int32),
        (int4, np.int64),
        (int4, np.float16),
        (int4, np.float32),
        (int4, np.float64),
        (int4, np.complex64),
        (int4, np.complex128),
        (np.bool_, uint4),
        (uint4, np.int8),
        (uint4, np.int16),
        (uint4, np.int32),
        (uint4, np.int64),
        (uint4, np.uint8),
        (uint4, np.uint16),
        (uint4, np.uint32),
        (uint4, np.uint64),
        (uint4, np.float16),
        (uint4, np.float32),
        (uint4, np.float64),
        (uint4, np.complex64),
        (uint4, np.complex128),
    ]

    assert ((scalar_type, dtype) in allowed_casts) == np.can_cast(
        scalar_type, dtype
    )
    assert ((dtype, scalar_type) in allowed_casts) == np.can_cast(
        dtype, scalar_type
    )


# Tests for the Python scalar type
class ArrayTest(parameterized.TestCase):

  @parameterized.product(scalar_type=INT4_TYPES)
  def testDtype(self, scalar_type):
    self.assertEqual(scalar_type, np.dtype(scalar_type))

  @parameterized.product(scalar_type=INT4_TYPES)
  def testHash(self, scalar_type):
    h = hash(np.dtype(scalar_type))
    self.assertEqual(h, hash(np.dtype(scalar_type.dtype)))
    self.assertEqual(h, hash(np.dtype(scalar_type.__name__)))

  @parameterized.product(scalar_type=INT4_TYPES)
  def testDeepCopyDoesNotAlterHash(self, scalar_type):
    # For context, see https://github.com/google/jax/issues/4651. If the hash
    # value of the type descriptor is not initialized correctly, a deep copy
    # can change the type hash.
    dtype = np.dtype(scalar_type)
    h = hash(dtype)
    _ = copy.deepcopy(dtype)
    self.assertEqual(h, hash(dtype))

  @parameterized.product(scalar_type=INT4_TYPES)
  def testArray(self, scalar_type):
    x = np.array([[1, 2, 3]], dtype=scalar_type)
    self.assertEqual(scalar_type, x.dtype)
    self.assertEqual("[[1 2 3]]", str(x))
    np.testing.assert_array_equal(x, x)
    self.assertTrue((x == x).all())  # pylint: disable=comparison-with-itself

  @parameterized.product(
      scalar_type=INT4_TYPES,
      ufunc=[np.nonzero, np.logical_not, np.argmax, np.argmin],
  )
  def testUnaryPredicateUfunc(self, scalar_type, ufunc):
    x = np.array(VALUES[scalar_type])
    y = np.array(VALUES[scalar_type], dtype=scalar_type)
    # Compute `ufunc(y)` first so we don't get lucky by reusing memory
    # initialized by `ufunc(x)`.
    y_result = ufunc(y)
    x_result = ufunc(x)
    np.testing.assert_array_equal(x_result, y_result)

  @parameterized.product(
      scalar_type=INT4_TYPES,
      ufunc=[
          np.less,
          np.less_equal,
          np.greater,
          np.greater_equal,
          np.equal,
          np.not_equal,
          np.logical_and,
          np.logical_or,
          np.logical_xor,
      ],
  )
  def testPredicateUfuncs(self, scalar_type, ufunc):
    x = np.array(VALUES[scalar_type])
    y = np.array(VALUES[scalar_type], dtype=scalar_type)
    np.testing.assert_array_equal(
        ufunc(x[:, None], x[None, :]),
        ufunc(y[:, None], y[None, :]),
    )

  @parameterized.product(
      scalar_type=INT4_TYPES,
      dtype=[
          np.float16,
          np.float32,
          np.float64,
          np.longdouble,
          np.int8,
          np.int16,
          np.int32,
          np.int64,
          np.complex64,
          np.complex128,
          np.clongdouble,
          np.uint8,
          np.uint16,
          np.uint32,
          np.uint64,
          np.intc,
          np.int_,
          np.longlong,
          np.uintc,
          np.ulonglong,
      ],
  )
  def testCasts(self, scalar_type, dtype):
    x_orig = np.array(VALUES[scalar_type])
    x = np.array(VALUES[scalar_type]).astype(dtype)
    x = np.where(x == x_orig, x, np.zeros_like(x))
    y = x.astype(scalar_type)
    z = y.astype(dtype)
    self.assertTrue(np.all(x == y), msg=(x, y))
    self.assertEqual(scalar_type, y.dtype)
    self.assertTrue(np.all(x == z))
    self.assertEqual(dtype, z.dtype)

  @parameterized.product(
      scalar_type=INT4_TYPES,
      ufunc=[
          np.add,
          np.subtract,
          np.multiply,
          np.floor_divide,
          np.remainder,
      ],
  )
  @ignore_warning(category=RuntimeWarning, message="divide by zero encountered")
  def testBinaryUfuncs(self, scalar_type, ufunc):
    x = np.array(VALUES[scalar_type])
    y = np.array(VALUES[scalar_type], dtype=scalar_type)
    np.testing.assert_array_equal(
        ufunc(x[:, None], x[None, :]).astype(scalar_type),
        ufunc(y[:, None], y[None, :]),
    )


if __name__ == "__main__":
  absltest.main()
