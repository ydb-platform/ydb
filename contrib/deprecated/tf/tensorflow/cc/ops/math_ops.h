// This file is MACHINE GENERATED! Do not edit.

#ifndef TENSORFLOW_CC_OPS_MATH_OPS_H_
#define TENSORFLOW_CC_OPS_MATH_OPS_H_

// This file is MACHINE GENERATED! Do not edit.

#include "tensorflow/cc/framework/ops.h"
#include "tensorflow/cc/framework/scope.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/lib/gtl/array_slice.h"

namespace tensorflow {
namespace ops {

/// @defgroup math_ops Math Ops
/// @{

/// Computes the absolute value of a tensor.
///
/// Given a tensor `x`, this operation returns a tensor containing the absolute
/// value of each element in `x`. For example, if x is an input element and y is
/// an output element, this operation computes \\(y = |x|\\).
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Abs {
 public:
  Abs(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Returns the element-wise sum of a list of tensors.
///
/// `tf.accumulate_n_v2` performs the same operation as `tf.add_n`, but does not
/// wait for all of its inputs to be ready before beginning to sum. This can
/// save memory if inputs are ready at different times, since minimum temporary
/// storage is proportional to the output size rather than the inputs size.
///
/// Unlike the original `accumulate_n`, `accumulate_n_v2` is differentiable.
///
/// Returns a `Tensor` of same shape and type as the elements of `inputs`.
///
/// Arguments:
/// * scope: A Scope object
/// * inputs: A list of `Tensor` objects, each with same shape and type.
/// * shape: Shape of elements of `inputs`.
///
/// Returns:
/// * `Output`: The sum tensor.
class AccumulateNV2 {
 public:
  AccumulateNV2(const ::tensorflow::Scope& scope, ::tensorflow::InputList inputs,
              PartialTensorShape shape);
  operator ::tensorflow::Output() const { return sum; }
  operator ::tensorflow::Input() const { return sum; }
  ::tensorflow::Node* node() const { return sum.node(); }

  ::tensorflow::Output sum;
};

/// Computes acos of x element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Acos {
 public:
  Acos(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Computes inverse hyperbolic cosine of x element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Acosh {
 public:
  Acosh(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Returns x + y element-wise.
///
/// *NOTE*: `Add` supports broadcasting. `AddN` does not. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class Add {
 public:
  Add(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
    ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Add all input tensors element wise.
///
/// Arguments:
/// * scope: A Scope object
/// * inputs: Must all be the same size and shape.
///
/// Returns:
/// * `Output`: The sum tensor.
class AddN {
 public:
  AddN(const ::tensorflow::Scope& scope, ::tensorflow::InputList inputs);
  operator ::tensorflow::Output() const { return sum; }
  operator ::tensorflow::Input() const { return sum; }
  ::tensorflow::Node* node() const { return sum.node(); }

  ::tensorflow::Output sum;
};

/// Returns x + y element-wise.
///
/// *NOTE*: `Add` supports broadcasting. `AddN` does not. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class AddV2 {
 public:
  AddV2(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
      ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Computes the "logical and" of elements across dimensions of a tensor.
///
/// Reduces `input` along the dimensions given in `axis`. Unless
/// `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
/// `axis`. If `keep_dims` is true, the reduced dimensions are
/// retained with length 1.
///
/// Arguments:
/// * scope: A Scope object
/// * input: The tensor to reduce.
/// * axis: The dimensions to reduce. Must be in the range
/// `[-rank(input), rank(input))`.
///
/// Optional attributes (see `Attrs`):
/// * keep_dims: If true, retain reduced dimensions with length 1.
///
/// Returns:
/// * `Output`: The reduced tensor.
///
/// Aliases:
/// * ReduceAll
class All {
 public:
  /// Optional attribute setters for All
  struct Attrs {
    /// If true, retain reduced dimensions with length 1.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs KeepDims(bool x) {
      Attrs ret = *this;
      ret.keep_dims_ = x;
      return ret;
    }

    bool keep_dims_ = false;
  };
  All(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
    ::tensorflow::Input axis);
  All(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
    ::tensorflow::Input axis, const All::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs KeepDims(bool x) {
    return Attrs().KeepDims(x);
  }

  ::tensorflow::Output output;
};
typedef All ReduceAll;

/// Returns the argument of a complex number.
///
/// Given a tensor `input` of complex numbers, this operation returns a tensor of
/// type `float` that is the argument of each element in `input`. All elements in
/// `input` must be complex numbers of the form \\(a + bj\\), where *a*
/// is the real part and *b* is the imaginary part.
///
/// The argument returned by this operation is of the form \\(atan2(b, a)\\).
///
/// For example:
///
/// ```
/// # tensor 'input' is [-2.25 + 4.75j, 3.25 + 5.75j]
/// tf.angle(input) ==> [2.0132, 1.056]
/// ```
///
/// @compatibility(numpy)
/// Equivalent to np.angle.
/// @end_compatibility
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The output tensor.
class Angle {
 public:
  /// Optional attribute setters for Angle
  struct Attrs {
    /// Defaults to DT_FLOAT
    TF_MUST_USE_RESULT Attrs Tout(DataType x) {
      Attrs ret = *this;
      ret.Tout_ = x;
      return ret;
    }

    DataType Tout_ = DT_FLOAT;
  };
  Angle(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  Angle(const ::tensorflow::Scope& scope, ::tensorflow::Input input, const
      Angle::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs Tout(DataType x) {
    return Attrs().Tout(x);
  }

  ::tensorflow::Output output;
};

/// Computes the "logical or" of elements across dimensions of a tensor.
///
/// Reduces `input` along the dimensions given in `axis`. Unless
/// `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
/// `axis`. If `keep_dims` is true, the reduced dimensions are
/// retained with length 1.
///
/// Arguments:
/// * scope: A Scope object
/// * input: The tensor to reduce.
/// * axis: The dimensions to reduce. Must be in the range
/// `[-rank(input), rank(input))`.
///
/// Optional attributes (see `Attrs`):
/// * keep_dims: If true, retain reduced dimensions with length 1.
///
/// Returns:
/// * `Output`: The reduced tensor.
///
/// Aliases:
/// * ReduceAny
class Any {
 public:
  /// Optional attribute setters for Any
  struct Attrs {
    /// If true, retain reduced dimensions with length 1.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs KeepDims(bool x) {
      Attrs ret = *this;
      ret.keep_dims_ = x;
      return ret;
    }

    bool keep_dims_ = false;
  };
  Any(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
    ::tensorflow::Input axis);
  Any(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
    ::tensorflow::Input axis, const Any::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs KeepDims(bool x) {
    return Attrs().KeepDims(x);
  }

  ::tensorflow::Output output;
};
typedef Any ReduceAny;

/// Returns the truth value of abs(x-y) < tolerance element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class ApproximateEqual {
 public:
  /// Optional attribute setters for ApproximateEqual
  struct Attrs {
    /// Defaults to 1e-05
    TF_MUST_USE_RESULT Attrs Tolerance(float x) {
      Attrs ret = *this;
      ret.tolerance_ = x;
      return ret;
    }

    float tolerance_ = 1e-05f;
  };
  ApproximateEqual(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                 ::tensorflow::Input y);
  ApproximateEqual(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                 ::tensorflow::Input y, const ApproximateEqual::Attrs& attrs);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  static Attrs Tolerance(float x) {
    return Attrs().Tolerance(x);
  }

  ::tensorflow::Output z;
};

/// Returns the index with the largest value across dimensions of a tensor.
///
/// Note that in case of ties the identity of the return value is not guaranteed.
///
/// Arguments:
/// * scope: A Scope object
/// * dimension: int32 or int64, must be in the range `[-rank(input), rank(input))`.
/// Describes which dimension of the input Tensor to reduce across. For vectors,
/// use dimension = 0.
///
/// Returns:
/// * `Output`: The output tensor.
class ArgMax {
 public:
  /// Optional attribute setters for ArgMax
  struct Attrs {
    /// Defaults to DT_INT64
    TF_MUST_USE_RESULT Attrs OutputType(DataType x) {
      Attrs ret = *this;
      ret.output_type_ = x;
      return ret;
    }

    DataType output_type_ = DT_INT64;
  };
  ArgMax(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
       ::tensorflow::Input dimension);
  ArgMax(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
       ::tensorflow::Input dimension, const ArgMax::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs OutputType(DataType x) {
    return Attrs().OutputType(x);
  }

  ::tensorflow::Output output;
};

/// Returns the index with the smallest value across dimensions of a tensor.
///
/// Note that in case of ties the identity of the return value is not guaranteed.
///
/// Arguments:
/// * scope: A Scope object
/// * dimension: int32 or int64, must be in the range `[-rank(input), rank(input))`.
/// Describes which dimension of the input Tensor to reduce across. For vectors,
/// use dimension = 0.
///
/// Returns:
/// * `Output`: The output tensor.
class ArgMin {
 public:
  /// Optional attribute setters for ArgMin
  struct Attrs {
    /// Defaults to DT_INT64
    TF_MUST_USE_RESULT Attrs OutputType(DataType x) {
      Attrs ret = *this;
      ret.output_type_ = x;
      return ret;
    }

    DataType output_type_ = DT_INT64;
  };
  ArgMin(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
       ::tensorflow::Input dimension);
  ArgMin(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
       ::tensorflow::Input dimension, const ArgMin::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs OutputType(DataType x) {
    return Attrs().OutputType(x);
  }

  ::tensorflow::Output output;
};

/// Computes asin of x element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Asin {
 public:
  Asin(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Computes inverse hyperbolic sine of x element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Asinh {
 public:
  Asinh(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Computes atan of x element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Atan {
 public:
  Atan(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Computes arctangent of `y/x` element-wise, respecting signs of the arguments.
///
/// This is the angle \( \theta \in [-\pi, \pi] \) such that
/// \[ x = r \cos(\theta) \]
/// and
/// \[ y = r \sin(\theta) \]
/// where \(r = \sqrt(x^2 + y^2) \).
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class Atan2 {
 public:
  Atan2(const ::tensorflow::Scope& scope, ::tensorflow::Input y,
      ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Computes inverse hyperbolic tangent of x element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Atanh {
 public:
  Atanh(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Multiplies slices of two tensors in batches.
///
/// Multiplies all slices of `Tensor` `x` and `y` (each slice can be
/// viewed as an element of a batch), and arranges the individual results
/// in a single output tensor of the same batch size. Each of the
/// individual slices can optionally be adjointed (to adjoint a matrix
/// means to transpose and conjugate it) before multiplication by setting
/// the `adj_x` or `adj_y` flag to `True`, which are by default `False`.
///
/// The input tensors `x` and `y` are 2-D or higher with shape `[..., r_x, c_x]`
/// and `[..., r_y, c_y]`.
///
/// The output tensor is 2-D or higher with shape `[..., r_o, c_o]`, where:
///
///     r_o = c_x if adj_x else r_x
///     c_o = r_y if adj_y else c_y
///
/// It is computed as:
///
///     output[..., :, :] = matrix(x[..., :, :]) * matrix(y[..., :, :])
///
/// Arguments:
/// * scope: A Scope object
/// * x: 2-D or higher with shape `[..., r_x, c_x]`.
/// * y: 2-D or higher with shape `[..., r_y, c_y]`.
///
/// Optional attributes (see `Attrs`):
/// * adj_x: If `True`, adjoint the slices of `x`. Defaults to `False`.
/// * adj_y: If `True`, adjoint the slices of `y`. Defaults to `False`.
///
/// Returns:
/// * `Output`: 3-D or higher with shape `[..., r_o, c_o]`
class BatchMatMul {
 public:
  /// Optional attribute setters for BatchMatMul
  struct Attrs {
    /// If `True`, adjoint the slices of `x`. Defaults to `False`.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs AdjX(bool x) {
      Attrs ret = *this;
      ret.adj_x_ = x;
      return ret;
    }

    /// If `True`, adjoint the slices of `y`. Defaults to `False`.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs AdjY(bool x) {
      Attrs ret = *this;
      ret.adj_y_ = x;
      return ret;
    }

    bool adj_x_ = false;
    bool adj_y_ = false;
  };
  BatchMatMul(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
            ::tensorflow::Input y);
  BatchMatMul(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
            ::tensorflow::Input y, const BatchMatMul::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs AdjX(bool x) {
    return Attrs().AdjX(x);
  }
  static Attrs AdjY(bool x) {
    return Attrs().AdjY(x);
  }

  ::tensorflow::Output output;
};

/// Computes the Bessel i0e function of `x` element-wise.
///
/// Exponentially scaled modified Bessel function of order 0 defined as
/// `bessel_i0e(x) = exp(-abs(x)) bessel_i0(x)`.
///
/// This function is faster and numerically stabler than `bessel_i0(x)`.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class BesselI0e {
 public:
  BesselI0e(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Computes the Bessel i1e function of `x` element-wise.
///
/// Exponentially scaled modified Bessel function of order 0 defined as
/// `bessel_i1e(x) = exp(-abs(x)) bessel_i1(x)`.
///
/// This function is faster and numerically stabler than `bessel_i1(x)`.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class BesselI1e {
 public:
  BesselI1e(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Compute the regularized incomplete beta integral \\(I_x(a, b)\\).
///
/// The regularized incomplete beta integral is defined as:
///
///
/// \\(I_x(a, b) = \frac{B(x; a, b)}{B(a, b)}\\)
///
/// where
///
///
/// \\(B(x; a, b) = \int_0^x t^{a-1} (1 - t)^{b-1} dt\\)
///
///
/// is the incomplete beta function and \\(B(a, b)\\) is the *complete*
/// beta function.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class Betainc {
 public:
  Betainc(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
        ::tensorflow::Input b, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Counts the number of occurrences of each value in an integer array.
///
/// Outputs a vector with length `size` and the same dtype as `weights`. If
/// `weights` are empty, then index `i` stores the number of times the value `i` is
/// counted in `arr`. If `weights` are non-empty, then index `i` stores the sum of
/// the value in `weights` at each index where the corresponding value in `arr` is
/// `i`.
///
/// Values in `arr` outside of the range [0, size) are ignored.
///
/// Arguments:
/// * scope: A Scope object
/// * arr: int32 `Tensor`.
/// * size: non-negative int32 scalar `Tensor`.
/// * weights: is an int32, int64, float32, or float64 `Tensor` with the same
/// shape as `arr`, or a length-0 `Tensor`, in which case it acts as all weights
/// equal to 1.
///
/// Returns:
/// * `Output`: 1D `Tensor` with length equal to `size`. The counts or summed weights for
/// each value in the range [0, size).
class Bincount {
 public:
  Bincount(const ::tensorflow::Scope& scope, ::tensorflow::Input arr,
         ::tensorflow::Input size, ::tensorflow::Input weights);
  operator ::tensorflow::Output() const { return bins; }
  operator ::tensorflow::Input() const { return bins; }
  ::tensorflow::Node* node() const { return bins.node(); }

  ::tensorflow::Output bins;
};

/// Bucketizes 'input' based on 'boundaries'.
///
/// For example, if the inputs are
///     boundaries = [0, 10, 100]
///     input = [[-5, 10000]
///              [150,   10]
///              [5,    100]]
///
/// then the output will be
///     output = [[0, 3]
///               [3, 2]
///               [1, 3]]
///
/// Arguments:
/// * scope: A Scope object
/// * input: Any shape of Tensor contains with int or float type.
/// * boundaries: A sorted list of floats gives the boundary of the buckets.
///
/// Returns:
/// * `Output`: Same shape with 'input', each value of input replaced with bucket index.
///
/// @compatibility(numpy)
/// Equivalent to np.digitize.
/// @end_compatibility
class Bucketize {
 public:
  Bucketize(const ::tensorflow::Scope& scope, ::tensorflow::Input input, const
          gtl::ArraySlice<float>& boundaries);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Cast x of type SrcT to y of DstT.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Cast {
 public:
  Cast(const ::tensorflow::Scope& scope, ::tensorflow::Input x, DataType DstT);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Returns element-wise smallest integer in not less than x.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Ceil {
 public:
  Ceil(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Clips tensor values to a specified min and max.
///
/// Given a tensor `t`, this operation returns a tensor of the same type and
/// shape as `t` with its values clipped to `clip_value_min` and `clip_value_max`.
/// Any values less than `clip_value_min` are set to `clip_value_min`. Any values
/// greater than `clip_value_max` are set to `clip_value_max`.
///
/// Arguments:
/// * scope: A Scope object
/// * t: A `Tensor`.
/// * clip_value_min: A 0-D (scalar) `Tensor`, or a `Tensor` with the same shape
/// as `t`. The minimum value to clip by.
/// * clip_value_max: A 0-D (scalar) `Tensor`, or a `Tensor` with the same shape
/// as `t`. The maximum value to clip by.
///
/// Returns:
/// * `Output`: A clipped `Tensor` with the same shape as input 't'.
class ClipByValue {
 public:
  ClipByValue(const ::tensorflow::Scope& scope, ::tensorflow::Input t,
            ::tensorflow::Input clip_value_min, ::tensorflow::Input
            clip_value_max);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Compare values of `input` to `threshold` and pack resulting bits into a `uint8`.
///
/// Each comparison returns a boolean `true` (if `input_value > threshold`)
/// or and `false` otherwise.
///
/// This operation is useful for Locality-Sensitive-Hashing (LSH) and other
/// algorithms that use hashing approximations of cosine and `L2` distances;
/// codes can be generated from an input via:
///
/// ```python
/// codebook_size = 50
/// codebook_bits = codebook_size * 32
/// codebook = tf.get_variable('codebook', [x.shape[-1].value, codebook_bits],
///                            dtype=x.dtype,
///                            initializer=tf.orthogonal_initializer())
/// codes = compare_and_threshold(tf.matmul(x, codebook), threshold=0.)
/// codes = tf.bitcast(codes, tf.int32)  # go from uint8 to int32
/// # now codes has shape x.shape[:-1] + [codebook_size]
/// ```
///
/// **NOTE**: Currently, the innermost dimension of the tensor must be divisible
/// by 8.
///
/// Given an `input` shaped `[s0, s1, ..., s_n]`, the output is
/// a `uint8` tensor shaped `[s0, s1, ..., s_n / 8]`.
///
/// Arguments:
/// * scope: A Scope object
/// * input: Values to compare against `threshold` and bitpack.
/// * threshold: Threshold to compare against.
///
/// Returns:
/// * `Output`: The bitpacked comparisons.
class CompareAndBitpack {
 public:
  CompareAndBitpack(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
                  ::tensorflow::Input threshold);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Converts two real numbers to a complex number.
///
/// Given a tensor `real` representing the real part of a complex number, and a
/// tensor `imag` representing the imaginary part of a complex number, this
/// operation returns complex numbers elementwise of the form \\(a + bj\\), where
/// *a* represents the `real` part and *b* represents the `imag` part.
///
/// The input tensors `real` and `imag` must have the same shape.
///
/// For example:
///
/// ```
/// # tensor 'real' is [2.25, 3.25]
/// # tensor `imag` is [4.75, 5.75]
/// tf.complex(real, imag) ==> [[2.25 + 4.75j], [3.25 + 5.75j]]
/// ```
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The out tensor.
class Complex {
 public:
  /// Optional attribute setters for Complex
  struct Attrs {
    /// Defaults to DT_COMPLEX64
    TF_MUST_USE_RESULT Attrs Tout(DataType x) {
      Attrs ret = *this;
      ret.Tout_ = x;
      return ret;
    }

    DataType Tout_ = DT_COMPLEX64;
  };
  Complex(const ::tensorflow::Scope& scope, ::tensorflow::Input real,
        ::tensorflow::Input imag);
  Complex(const ::tensorflow::Scope& scope, ::tensorflow::Input real,
        ::tensorflow::Input imag, const Complex::Attrs& attrs);
  operator ::tensorflow::Output() const { return out; }
  operator ::tensorflow::Input() const { return out; }
  ::tensorflow::Node* node() const { return out.node(); }

  static Attrs Tout(DataType x) {
    return Attrs().Tout(x);
  }

  ::tensorflow::Output out;
};

/// Computes the complex absolute value of a tensor.
///
/// Given a tensor `x` of complex numbers, this operation returns a tensor of type
/// `float` or `double` that is the absolute value of each element in `x`. All
/// elements in `x` must be complex numbers of the form \\(a + bj\\). The absolute
/// value is computed as \\( \sqrt{a^2 + b^2}\\).
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class ComplexAbs {
 public:
  /// Optional attribute setters for ComplexAbs
  struct Attrs {
    /// Defaults to DT_FLOAT
    TF_MUST_USE_RESULT Attrs Tout(DataType x) {
      Attrs ret = *this;
      ret.Tout_ = x;
      return ret;
    }

    DataType Tout_ = DT_FLOAT;
  };
  ComplexAbs(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  ComplexAbs(const ::tensorflow::Scope& scope, ::tensorflow::Input x, const
           ComplexAbs::Attrs& attrs);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  static Attrs Tout(DataType x) {
    return Attrs().Tout(x);
  }

  ::tensorflow::Output y;
};

/// Returns the complex conjugate of a complex number.
///
/// Given a tensor `input` of complex numbers, this operation returns a tensor of
/// complex numbers that are the complex conjugate of each element in `input`. The
/// complex numbers in `input` must be of the form \\(a + bj\\), where *a* is the
/// real part and *b* is the imaginary part.
///
/// The complex conjugate returned by this operation is of the form \\(a - bj\\).
///
/// For example:
///
/// ```
/// # tensor 'input' is [-2.25 + 4.75j, 3.25 + 5.75j]
/// tf.conj(input) ==> [-2.25 - 4.75j, 3.25 - 5.75j]
/// ```
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The output tensor.
class Conj {
 public:
  Conj(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes cos of x element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Cos {
 public:
  Cos(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Computes hyperbolic cosine of x element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Cosh {
 public:
  Cosh(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Compute the pairwise cross product.
///
/// `a` and `b` must be the same shape; they can either be simple 3-element vectors,
/// or any shape where the innermost dimension is 3. In the latter case, each pair
/// of corresponding 3-element vectors is cross-multiplied independently.
///
/// Arguments:
/// * scope: A Scope object
/// * a: A tensor containing 3-element vectors.
/// * b: Another tensor, of same type and shape as `a`.
///
/// Returns:
/// * `Output`: Pairwise cross product of the vectors in `a` and `b`.
class Cross {
 public:
  Cross(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
      ::tensorflow::Input b);
  operator ::tensorflow::Output() const { return product; }
  operator ::tensorflow::Input() const { return product; }
  ::tensorflow::Node* node() const { return product.node(); }

  ::tensorflow::Output product;
};

/// Compute the cumulative product of the tensor `x` along `axis`.
///
/// By default, this op performs an inclusive cumprod, which means that the first
/// element of the input is identical to the first element of the output:
///
/// ```python
/// tf.cumprod([a, b, c])  # => [a, a * b, a * b * c]
/// ```
///
/// By setting the `exclusive` kwarg to `True`, an exclusive cumprod is
/// performed instead:
///
/// ```python
/// tf.cumprod([a, b, c], exclusive=True)  # => [1, a, a * b]
/// ```
///
/// By setting the `reverse` kwarg to `True`, the cumprod is performed in the
/// opposite direction:
///
/// ```python
/// tf.cumprod([a, b, c], reverse=True)  # => [a * b * c, b * c, c]
/// ```
///
/// This is more efficient than using separate `tf.reverse` ops.
///
/// The `reverse` and `exclusive` kwargs can also be combined:
///
/// ```python
/// tf.cumprod([a, b, c], exclusive=True, reverse=True)  # => [b * c, c, 1]
/// ```
///
/// Arguments:
/// * scope: A Scope object
/// * x: A `Tensor`. Must be one of the following types: `float32`, `float64`,
/// `int64`, `int32`, `uint8`, `uint16`, `int16`, `int8`, `complex64`,
/// `complex128`, `qint8`, `quint8`, `qint32`, `half`.
/// * axis: A `Tensor` of type `int32` (default: 0). Must be in the range
/// `[-rank(x), rank(x))`.
///
/// Optional attributes (see `Attrs`):
/// * exclusive: If `True`, perform exclusive cumprod.
/// * reverse: A `bool` (default: False).
///
/// Returns:
/// * `Output`: The out tensor.
class Cumprod {
 public:
  /// Optional attribute setters for Cumprod
  struct Attrs {
    /// If `True`, perform exclusive cumprod.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs Exclusive(bool x) {
      Attrs ret = *this;
      ret.exclusive_ = x;
      return ret;
    }

    /// A `bool` (default: False).
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs Reverse(bool x) {
      Attrs ret = *this;
      ret.reverse_ = x;
      return ret;
    }

    bool exclusive_ = false;
    bool reverse_ = false;
  };
  Cumprod(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
        ::tensorflow::Input axis);
  Cumprod(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
        ::tensorflow::Input axis, const Cumprod::Attrs& attrs);
  operator ::tensorflow::Output() const { return out; }
  operator ::tensorflow::Input() const { return out; }
  ::tensorflow::Node* node() const { return out.node(); }

  static Attrs Exclusive(bool x) {
    return Attrs().Exclusive(x);
  }
  static Attrs Reverse(bool x) {
    return Attrs().Reverse(x);
  }

  ::tensorflow::Output out;
};

/// Compute the cumulative sum of the tensor `x` along `axis`.
///
/// By default, this op performs an inclusive cumsum, which means that the first
/// element of the input is identical to the first element of the output:
///
/// ```python
/// tf.cumsum([a, b, c])  # => [a, a + b, a + b + c]
/// ```
///
/// By setting the `exclusive` kwarg to `True`, an exclusive cumsum is
/// performed instead:
///
/// ```python
/// tf.cumsum([a, b, c], exclusive=True)  # => [0, a, a + b]
/// ```
///
/// By setting the `reverse` kwarg to `True`, the cumsum is performed in the
/// opposite direction:
///
/// ```python
/// tf.cumsum([a, b, c], reverse=True)  # => [a + b + c, b + c, c]
/// ```
///
/// This is more efficient than using separate `tf.reverse` ops.
///
/// The `reverse` and `exclusive` kwargs can also be combined:
///
/// ```python
/// tf.cumsum([a, b, c], exclusive=True, reverse=True)  # => [b + c, c, 0]
/// ```
///
/// Arguments:
/// * scope: A Scope object
/// * x: A `Tensor`. Must be one of the following types: `float32`, `float64`,
/// `int64`, `int32`, `uint8`, `uint16`, `int16`, `int8`, `complex64`,
/// `complex128`, `qint8`, `quint8`, `qint32`, `half`.
/// * axis: A `Tensor` of type `int32` (default: 0). Must be in the range
/// `[-rank(x), rank(x))`.
///
/// Optional attributes (see `Attrs`):
/// * exclusive: If `True`, perform exclusive cumsum.
/// * reverse: A `bool` (default: False).
///
/// Returns:
/// * `Output`: The out tensor.
class Cumsum {
 public:
  /// Optional attribute setters for Cumsum
  struct Attrs {
    /// If `True`, perform exclusive cumsum.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs Exclusive(bool x) {
      Attrs ret = *this;
      ret.exclusive_ = x;
      return ret;
    }

    /// A `bool` (default: False).
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs Reverse(bool x) {
      Attrs ret = *this;
      ret.reverse_ = x;
      return ret;
    }

    bool exclusive_ = false;
    bool reverse_ = false;
  };
  Cumsum(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
       ::tensorflow::Input axis);
  Cumsum(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
       ::tensorflow::Input axis, const Cumsum::Attrs& attrs);
  operator ::tensorflow::Output() const { return out; }
  operator ::tensorflow::Input() const { return out; }
  ::tensorflow::Node* node() const { return out.node(); }

  static Attrs Exclusive(bool x) {
    return Attrs().Exclusive(x);
  }
  static Attrs Reverse(bool x) {
    return Attrs().Reverse(x);
  }

  ::tensorflow::Output out;
};

/// Computes Psi, the derivative of Lgamma (the log of the absolute value of
///
/// `Gamma(x)`), element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Digamma {
 public:
  Digamma(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Returns x / y element-wise.
///
/// *NOTE*: `Div` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class Div {
 public:
  Div(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
    ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Returns the truth value of (x == y) element-wise.
///
/// *NOTE*: `Equal` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class Equal {
 public:
  Equal(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
      ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Computes the Gauss error function of `x` element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Erf {
 public:
  Erf(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Computes the complementary error function of `x` element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Erfc {
 public:
  Erfc(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Computes exponential of x element-wise.  \\(y = e^x\\).
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Exp {
 public:
  Exp(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Computes exponential of x - 1 element-wise.
///
/// I.e., \\(y = (\exp x) - 1\\).
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Expm1 {
 public:
  Expm1(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Returns element-wise largest integer not greater than x.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Floor {
 public:
  Floor(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Returns x // y element-wise.
///
/// *NOTE*: `FloorDiv` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class FloorDiv {
 public:
  FloorDiv(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
         ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Returns element-wise remainder of division. When `x < 0` xor `y < 0` is
///
/// true, this follows Python semantics in that the result here is consistent
/// with a flooring divide. E.g. `floor(x / y) * y + mod(x, y) = x`.
///
/// *NOTE*: `FloorMod` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class FloorMod {
 public:
  FloorMod(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
         ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Returns the truth value of (x > y) element-wise.
///
/// *NOTE*: `Greater` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class Greater {
 public:
  Greater(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
        ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Returns the truth value of (x >= y) element-wise.
///
/// *NOTE*: `GreaterEqual` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class GreaterEqual {
 public:
  GreaterEqual(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
             ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Return histogram of values.
///
/// Given the tensor `values`, this operation returns a rank 1 histogram counting
/// the number of entries in `values` that fall into every bin.  The bins are
/// equal width and determined by the arguments `value_range` and `nbins`.
///
/// ```python
/// # Bins will be:  (-inf, 1), [1, 2), [2, 3), [3, 4), [4, inf)
/// nbins = 5
/// value_range = [0.0, 5.0]
/// new_values = [-1.0, 0.0, 1.5, 2.0, 5.0, 15]
///
/// with tf.get_default_session() as sess:
///   hist = tf.histogram_fixed_width(new_values, value_range, nbins=5)
///   variables.global_variables_initializer().run()
///   sess.run(hist) => [2, 1, 1, 0, 2]
/// ```
///
/// Arguments:
/// * scope: A Scope object
/// * values: Numeric `Tensor`.
/// * value_range: Shape [2] `Tensor` of same `dtype` as `values`.
/// values <= value_range[0] will be mapped to hist[0],
/// values >= value_range[1] will be mapped to hist[-1].
/// * nbins: Scalar `int32 Tensor`.  Number of histogram bins.
///
/// Returns:
/// * `Output`: A 1-D `Tensor` holding histogram of values.
class HistogramFixedWidth {
 public:
  /// Optional attribute setters for HistogramFixedWidth
  struct Attrs {
    /// Defaults to DT_INT32
    TF_MUST_USE_RESULT Attrs Dtype(DataType x) {
      Attrs ret = *this;
      ret.dtype_ = x;
      return ret;
    }

    DataType dtype_ = DT_INT32;
  };
  HistogramFixedWidth(const ::tensorflow::Scope& scope, ::tensorflow::Input
                    values, ::tensorflow::Input value_range,
                    ::tensorflow::Input nbins);
  HistogramFixedWidth(const ::tensorflow::Scope& scope, ::tensorflow::Input
                    values, ::tensorflow::Input value_range,
                    ::tensorflow::Input nbins, const
                    HistogramFixedWidth::Attrs& attrs);
  operator ::tensorflow::Output() const { return out; }
  operator ::tensorflow::Input() const { return out; }
  ::tensorflow::Node* node() const { return out.node(); }

  static Attrs Dtype(DataType x) {
    return Attrs().Dtype(x);
  }

  ::tensorflow::Output out;
};

/// Compute the lower regularized incomplete Gamma function `Q(a, x)`.
///
/// The lower regularized incomplete Gamma function is defined as:
///
///
/// \\(P(a, x) = gamma(a, x) / Gamma(a) = 1 - Q(a, x)\\)
///
/// where
///
/// \\(gamma(a, x) = int_{0}^{x} t^{a-1} exp(-t) dt\\)
///
/// is the lower incomplete Gamma function.
///
/// Note, above `Q(a, x)` (`Igammac`) is the upper regularized complete
/// Gamma function.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class Igamma {
 public:
  Igamma(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
       ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Compute the upper regularized incomplete Gamma function `Q(a, x)`.
///
/// The upper regularized incomplete Gamma function is defined as:
///
/// \\(Q(a, x) = Gamma(a, x) / Gamma(a) = 1 - P(a, x)\\)
///
/// where
///
/// \\(Gamma(a, x) = int_{x}^{\infty} t^{a-1} exp(-t) dt\\)
///
/// is the upper incomplete Gama function.
///
/// Note, above `P(a, x)` (`Igamma`) is the lower regularized complete
/// Gamma function.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class Igammac {
 public:
  Igammac(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
        ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Returns the imaginary part of a complex number.
///
/// Given a tensor `input` of complex numbers, this operation returns a tensor of
/// type `float` that is the imaginary part of each element in `input`. All
/// elements in `input` must be complex numbers of the form \\(a + bj\\), where *a*
/// is the real part and *b* is the imaginary part returned by this operation.
///
/// For example:
///
/// ```
/// # tensor 'input' is [-2.25 + 4.75j, 3.25 + 5.75j]
/// tf.imag(input) ==> [4.75, 5.75]
/// ```
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The output tensor.
class Imag {
 public:
  /// Optional attribute setters for Imag
  struct Attrs {
    /// Defaults to DT_FLOAT
    TF_MUST_USE_RESULT Attrs Tout(DataType x) {
      Attrs ret = *this;
      ret.Tout_ = x;
      return ret;
    }

    DataType Tout_ = DT_FLOAT;
  };
  Imag(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  Imag(const ::tensorflow::Scope& scope, ::tensorflow::Input input, const
     Imag::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs Tout(DataType x) {
    return Attrs().Tout(x);
  }

  ::tensorflow::Output output;
};

/// Computes the reciprocal of x element-wise.
///
/// I.e., \\(y = 1 / x\\).
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Inv {
 public:
  Inv(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Returns which elements of x are finite.
///
/// @compatibility(numpy)
/// Equivalent to np.isfinite
/// @end_compatibility
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class IsFinite {
 public:
  IsFinite(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Returns which elements of x are Inf.
///
/// @compatibility(numpy)
/// Equivalent to np.isinf
/// @end_compatibility
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class IsInf {
 public:
  IsInf(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Returns which elements of x are NaN.
///
/// @compatibility(numpy)
/// Equivalent to np.isnan
/// @end_compatibility
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class IsNan {
 public:
  IsNan(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Returns the truth value of (x < y) element-wise.
///
/// *NOTE*: `Less` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class Less {
 public:
  Less(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
     ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Returns the truth value of (x <= y) element-wise.
///
/// *NOTE*: `LessEqual` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class LessEqual {
 public:
  LessEqual(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
          ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Computes the log of the absolute value of `Gamma(x)` element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Lgamma {
 public:
  Lgamma(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Generates values in an interval.
///
/// A sequence of `num` evenly-spaced values are generated beginning at `start`.
/// If `num > 1`, the values in the sequence increase by `stop - start / num - 1`,
/// so that the last one is exactly `stop`.
///
/// For example:
///
/// ```
/// tf.linspace(10.0, 12.0, 3, name="linspace") => [ 10.0  11.0  12.0]
/// ```
///
/// Arguments:
/// * scope: A Scope object
/// * start: 0-D tensor. First entry in the range.
/// * stop: 0-D tensor. Last entry in the range.
/// * num: 0-D tensor. Number of values to generate.
///
/// Returns:
/// * `Output`: 1-D. The generated values.
class LinSpace {
 public:
  LinSpace(const ::tensorflow::Scope& scope, ::tensorflow::Input start,
         ::tensorflow::Input stop, ::tensorflow::Input num);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes natural logarithm of x element-wise.
///
/// I.e., \\(y = \log_e x\\).
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Log {
 public:
  Log(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Computes natural logarithm of (1 + x) element-wise.
///
/// I.e., \\(y = \log_e (1 + x)\\).
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Log1p {
 public:
  Log1p(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Returns the truth value of x AND y element-wise.
///
/// *NOTE*: `LogicalAnd` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class LogicalAnd {
 public:
  LogicalAnd(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
           ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Returns the truth value of NOT x element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class LogicalNot {
 public:
  LogicalNot(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Returns the truth value of x OR y element-wise.
///
/// *NOTE*: `LogicalOr` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class LogicalOr {
 public:
  LogicalOr(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
          ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Multiply the matrix "a" by the matrix "b".
///
/// The inputs must be two-dimensional matrices and the inner dimension of
/// "a" (after being transposed if transpose_a is true) must match the
/// outer dimension of "b" (after being transposed if transposed_b is
/// true).
///
/// *Note*: The default kernel implementation for MatMul on GPUs uses
/// cublas.
///
/// Arguments:
/// * scope: A Scope object
///
/// Optional attributes (see `Attrs`):
/// * transpose_a: If true, "a" is transposed before multiplication.
/// * transpose_b: If true, "b" is transposed before multiplication.
///
/// Returns:
/// * `Output`: The product tensor.
class MatMul {
 public:
  /// Optional attribute setters for MatMul
  struct Attrs {
    /// If true, "a" is transposed before multiplication.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs TransposeA(bool x) {
      Attrs ret = *this;
      ret.transpose_a_ = x;
      return ret;
    }

    /// If true, "b" is transposed before multiplication.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs TransposeB(bool x) {
      Attrs ret = *this;
      ret.transpose_b_ = x;
      return ret;
    }

    bool transpose_a_ = false;
    bool transpose_b_ = false;
  };
  MatMul(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
       ::tensorflow::Input b);
  MatMul(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
       ::tensorflow::Input b, const MatMul::Attrs& attrs);
  operator ::tensorflow::Output() const { return product; }
  operator ::tensorflow::Input() const { return product; }
  ::tensorflow::Node* node() const { return product.node(); }

  static Attrs TransposeA(bool x) {
    return Attrs().TransposeA(x);
  }
  static Attrs TransposeB(bool x) {
    return Attrs().TransposeB(x);
  }

  ::tensorflow::Output product;
};

/// Computes the maximum of elements across dimensions of a tensor.
///
/// Reduces `input` along the dimensions given in `axis`. Unless
/// `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
/// `axis`. If `keep_dims` is true, the reduced dimensions are
/// retained with length 1.
///
/// Arguments:
/// * scope: A Scope object
/// * input: The tensor to reduce.
/// * axis: The dimensions to reduce. Must be in the range
/// `[-rank(input), rank(input))`.
///
/// Optional attributes (see `Attrs`):
/// * keep_dims: If true, retain reduced dimensions with length 1.
///
/// Returns:
/// * `Output`: The reduced tensor.
///
/// Aliases:
/// * ReduceMax
class Max {
 public:
  /// Optional attribute setters for Max
  struct Attrs {
    /// If true, retain reduced dimensions with length 1.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs KeepDims(bool x) {
      Attrs ret = *this;
      ret.keep_dims_ = x;
      return ret;
    }

    bool keep_dims_ = false;
  };
  Max(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
    ::tensorflow::Input axis);
  Max(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
    ::tensorflow::Input axis, const Max::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs KeepDims(bool x) {
    return Attrs().KeepDims(x);
  }

  ::tensorflow::Output output;
};
typedef Max ReduceMax;

/// Returns the max of x and y (i.e. x > y ? x : y) element-wise.
///
/// *NOTE*: `Maximum` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class Maximum {
 public:
  Maximum(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
        ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Computes the mean of elements across dimensions of a tensor.
///
/// Reduces `input` along the dimensions given in `axis`. Unless
/// `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
/// `axis`. If `keep_dims` is true, the reduced dimensions are
/// retained with length 1.
///
/// Arguments:
/// * scope: A Scope object
/// * input: The tensor to reduce.
/// * axis: The dimensions to reduce. Must be in the range
/// `[-rank(input), rank(input))`.
///
/// Optional attributes (see `Attrs`):
/// * keep_dims: If true, retain reduced dimensions with length 1.
///
/// Returns:
/// * `Output`: The reduced tensor.
///
/// Aliases:
/// * ReduceMean
class Mean {
 public:
  /// Optional attribute setters for Mean
  struct Attrs {
    /// If true, retain reduced dimensions with length 1.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs KeepDims(bool x) {
      Attrs ret = *this;
      ret.keep_dims_ = x;
      return ret;
    }

    bool keep_dims_ = false;
  };
  Mean(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
     ::tensorflow::Input axis);
  Mean(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
     ::tensorflow::Input axis, const Mean::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs KeepDims(bool x) {
    return Attrs().KeepDims(x);
  }

  ::tensorflow::Output output;
};
typedef Mean ReduceMean;

/// Computes the minimum of elements across dimensions of a tensor.
///
/// Reduces `input` along the dimensions given in `axis`. Unless
/// `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
/// `axis`. If `keep_dims` is true, the reduced dimensions are
/// retained with length 1.
///
/// Arguments:
/// * scope: A Scope object
/// * input: The tensor to reduce.
/// * axis: The dimensions to reduce. Must be in the range
/// `[-rank(input), rank(input))`.
///
/// Optional attributes (see `Attrs`):
/// * keep_dims: If true, retain reduced dimensions with length 1.
///
/// Returns:
/// * `Output`: The reduced tensor.
///
/// Aliases:
/// * ReduceMin
class Min {
 public:
  /// Optional attribute setters for Min
  struct Attrs {
    /// If true, retain reduced dimensions with length 1.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs KeepDims(bool x) {
      Attrs ret = *this;
      ret.keep_dims_ = x;
      return ret;
    }

    bool keep_dims_ = false;
  };
  Min(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
    ::tensorflow::Input axis);
  Min(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
    ::tensorflow::Input axis, const Min::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs KeepDims(bool x) {
    return Attrs().KeepDims(x);
  }

  ::tensorflow::Output output;
};
typedef Min ReduceMin;

/// Returns the min of x and y (i.e. x < y ? x : y) element-wise.
///
/// *NOTE*: `Minimum` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class Minimum {
 public:
  Minimum(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
        ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Returns element-wise remainder of division. This emulates C semantics in that
///
/// the result here is consistent with a truncating divide. E.g.
/// `tf.truncatediv(x, y) * y + truncate_mod(x, y) = x`.
///
/// *NOTE*: `Mod` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class Mod {
 public:
  Mod(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
    ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Returns x * y element-wise.
///
/// *NOTE*: `Multiply` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
///
/// Aliases:
/// * Mul
class Multiply {
 public:
  Multiply(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
         ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};
typedef Multiply Mul;

/// Computes numerical negative value element-wise.
///
/// I.e., \\(y = -x\\).
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
///
/// Aliases:
/// * Neg
class Negate {
 public:
  Negate(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};
typedef Negate Neg;

/// Returns the truth value of (x != y) element-wise.
///
/// *NOTE*: `NotEqual` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class NotEqual {
 public:
  NotEqual(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
         ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Compute the polygamma function \\(\psi^{(n)}(x)\\).
///
/// The polygamma function is defined as:
///
///
/// \\(\psi^{(n)}(x) = \frac{d^n}{dx^n} \psi(x)\\)
///
/// where \\(\psi(x)\\) is the digamma function.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class Polygamma {
 public:
  Polygamma(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
          ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Computes the power of one value to another.
///
/// Given a tensor `x` and a tensor `y`, this operation computes \\(x^y\\) for
/// corresponding elements in `x` and `y`. For example:
///
/// ```
/// # tensor 'x' is [[2, 2]], [3, 3]]
/// # tensor 'y' is [[8, 16], [2, 3]]
/// tf.pow(x, y) ==> [[256, 65536], [9, 27]]
/// ```
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class Pow {
 public:
  Pow(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
    ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Computes the product of elements across dimensions of a tensor.
///
/// Reduces `input` along the dimensions given in `axis`. Unless
/// `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
/// `axis`. If `keep_dims` is true, the reduced dimensions are
/// retained with length 1.
///
/// Arguments:
/// * scope: A Scope object
/// * input: The tensor to reduce.
/// * axis: The dimensions to reduce. Must be in the range
/// `[-rank(input), rank(input))`.
///
/// Optional attributes (see `Attrs`):
/// * keep_dims: If true, retain reduced dimensions with length 1.
///
/// Returns:
/// * `Output`: The reduced tensor.
///
/// Aliases:
/// * ReduceProd
class Prod {
 public:
  /// Optional attribute setters for Prod
  struct Attrs {
    /// If true, retain reduced dimensions with length 1.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs KeepDims(bool x) {
      Attrs ret = *this;
      ret.keep_dims_ = x;
      return ret;
    }

    bool keep_dims_ = false;
  };
  Prod(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
     ::tensorflow::Input axis);
  Prod(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
     ::tensorflow::Input axis, const Prod::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs KeepDims(bool x) {
    return Attrs().KeepDims(x);
  }

  ::tensorflow::Output output;
};
typedef Prod ReduceProd;

/// Convert the quantized 'input' tensor into a lower-precision 'output', using the
///
/// actual distribution of the values to maximize the usage of the lower bit depth
/// and adjusting the output min and max ranges accordingly.
///
/// [input_min, input_max] are scalar floats that specify the range for the float
/// interpretation of the 'input' data. For example, if input_min is -1.0f and
/// input_max is 1.0f, and we are dealing with quint16 quantized data, then a 0
/// value in the 16-bit data should be interpreted as -1.0f, and a 65535 means 1.0f.
///
/// This operator tries to squeeze as much precision as possible into an output with
/// a lower bit depth by calculating the actual min and max values found in the
/// data. For example, maybe that quint16 input has no values lower than 16,384 and
/// none higher than 49,152. That means only half the range is actually needed, all
/// the float interpretations are between -0.5f and 0.5f, so if we want to compress
/// the data into a quint8 output, we can use that range rather than the theoretical
/// -1.0f to 1.0f that is suggested by the input min and max.
///
/// In practice, this is most useful for taking output from operations like
/// QuantizedMatMul that can produce higher bit-depth outputs than their inputs and
/// may have large potential output ranges, but in practice have a distribution of
/// input values that only uses a small fraction of the possible range. By feeding
/// that output into this operator, we can reduce it from 32 bits down to 8 with
/// minimal loss of accuracy.
///
/// Arguments:
/// * scope: A Scope object
/// * input_min: The float value that the minimum quantized input value represents.
/// * input_max: The float value that the maximum quantized input value represents.
/// * out_type: The type of the output. Should be a lower bit depth than Tinput.
///
/// Returns:
/// * `Output` output
/// * `Output` output_min: The float value that the minimum quantized output value represents.
/// * `Output` output_max: The float value that the maximum quantized output value represents.
class QuantizeDownAndShrinkRange {
 public:
  QuantizeDownAndShrinkRange(const ::tensorflow::Scope& scope,
                           ::tensorflow::Input input, ::tensorflow::Input
                           input_min, ::tensorflow::Input input_max, DataType
                           out_type);

  ::tensorflow::Output output;
  ::tensorflow::Output output_min;
  ::tensorflow::Output output_max;
};

/// Returns x + y element-wise, working on quantized buffers.
///
/// Arguments:
/// * scope: A Scope object
/// * min_x: The float value that the lowest quantized `x` value represents.
/// * max_x: The float value that the highest quantized `x` value represents.
/// * min_y: The float value that the lowest quantized `y` value represents.
/// * max_y: The float value that the highest quantized `y` value represents.
///
/// Returns:
/// * `Output` z
/// * `Output` min_z: The float value that the lowest quantized output value represents.
/// * `Output` max_z: The float value that the highest quantized output value represents.
///
/// *NOTE*: `QuantizedAdd` supports limited forms of broadcasting. More about
/// broadcasting [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
class QuantizedAdd {
 public:
  /// Optional attribute setters for QuantizedAdd
  struct Attrs {
    /// Defaults to DT_QINT32
    TF_MUST_USE_RESULT Attrs Toutput(DataType x) {
      Attrs ret = *this;
      ret.Toutput_ = x;
      return ret;
    }

    DataType Toutput_ = DT_QINT32;
  };
  QuantizedAdd(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
             ::tensorflow::Input y, ::tensorflow::Input min_x,
             ::tensorflow::Input max_x, ::tensorflow::Input min_y,
             ::tensorflow::Input max_y);
  QuantizedAdd(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
             ::tensorflow::Input y, ::tensorflow::Input min_x,
             ::tensorflow::Input max_x, ::tensorflow::Input min_y,
             ::tensorflow::Input max_y, const QuantizedAdd::Attrs& attrs);

  static Attrs Toutput(DataType x) {
    return Attrs().Toutput(x);
  }

  ::tensorflow::Output z;
  ::tensorflow::Output min_z;
  ::tensorflow::Output max_z;
};

/// Perform a quantized matrix multiplication of  `a` by the matrix `b`.
///
/// The inputs must be two-dimensional matrices and the inner dimension of
/// `a` (after being transposed if `transpose_a` is non-zero) must match the
/// outer dimension of `b` (after being transposed if `transposed_b` is
/// non-zero).
///
/// Arguments:
/// * scope: A Scope object
/// * a: Must be a two-dimensional tensor.
/// * b: Must be a two-dimensional tensor.
/// * min_a: The float value that the lowest quantized `a` value represents.
/// * max_a: The float value that the highest quantized `a` value represents.
/// * min_b: The float value that the lowest quantized `b` value represents.
/// * max_b: The float value that the highest quantized `b` value represents.
///
/// Optional attributes (see `Attrs`):
/// * transpose_a: If true, `a` is transposed before multiplication.
/// * transpose_b: If true, `b` is transposed before multiplication.
/// * Tactivation: The type of output produced by activation function
/// following this operation.
///
/// Returns:
/// * `Output` out
/// * `Output` min_out: The float value that the lowest quantized output value represents.
/// * `Output` max_out: The float value that the highest quantized output value represents.
class QuantizedMatMul {
 public:
  /// Optional attribute setters for QuantizedMatMul
  struct Attrs {
    /// Defaults to DT_QINT32
    TF_MUST_USE_RESULT Attrs Toutput(DataType x) {
      Attrs ret = *this;
      ret.Toutput_ = x;
      return ret;
    }

    /// If true, `a` is transposed before multiplication.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs TransposeA(bool x) {
      Attrs ret = *this;
      ret.transpose_a_ = x;
      return ret;
    }

    /// If true, `b` is transposed before multiplication.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs TransposeB(bool x) {
      Attrs ret = *this;
      ret.transpose_b_ = x;
      return ret;
    }

    /// The type of output produced by activation function
    /// following this operation.
    ///
    /// Defaults to DT_QUINT8
    TF_MUST_USE_RESULT Attrs Tactivation(DataType x) {
      Attrs ret = *this;
      ret.Tactivation_ = x;
      return ret;
    }

    DataType Toutput_ = DT_QINT32;
    bool transpose_a_ = false;
    bool transpose_b_ = false;
    DataType Tactivation_ = DT_QUINT8;
  };
  QuantizedMatMul(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
                ::tensorflow::Input b, ::tensorflow::Input min_a,
                ::tensorflow::Input max_a, ::tensorflow::Input min_b,
                ::tensorflow::Input max_b);
  QuantizedMatMul(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
                ::tensorflow::Input b, ::tensorflow::Input min_a,
                ::tensorflow::Input max_a, ::tensorflow::Input min_b,
                ::tensorflow::Input max_b, const QuantizedMatMul::Attrs& attrs);

  static Attrs Toutput(DataType x) {
    return Attrs().Toutput(x);
  }
  static Attrs TransposeA(bool x) {
    return Attrs().TransposeA(x);
  }
  static Attrs TransposeB(bool x) {
    return Attrs().TransposeB(x);
  }
  static Attrs Tactivation(DataType x) {
    return Attrs().Tactivation(x);
  }

  ::tensorflow::Output out;
  ::tensorflow::Output min_out;
  ::tensorflow::Output max_out;
};

/// Returns x * y element-wise, working on quantized buffers.
///
/// Arguments:
/// * scope: A Scope object
/// * min_x: The float value that the lowest quantized `x` value represents.
/// * max_x: The float value that the highest quantized `x` value represents.
/// * min_y: The float value that the lowest quantized `y` value represents.
/// * max_y: The float value that the highest quantized `y` value represents.
///
/// Returns:
/// * `Output` z
/// * `Output` min_z: The float value that the lowest quantized output value represents.
/// * `Output` max_z: The float value that the highest quantized output value represents.
///
/// *NOTE*: `QuantizedMul` supports limited forms of broadcasting. More about
/// broadcasting [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
class QuantizedMul {
 public:
  /// Optional attribute setters for QuantizedMul
  struct Attrs {
    /// Defaults to DT_QINT32
    TF_MUST_USE_RESULT Attrs Toutput(DataType x) {
      Attrs ret = *this;
      ret.Toutput_ = x;
      return ret;
    }

    DataType Toutput_ = DT_QINT32;
  };
  QuantizedMul(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
             ::tensorflow::Input y, ::tensorflow::Input min_x,
             ::tensorflow::Input max_x, ::tensorflow::Input min_y,
             ::tensorflow::Input max_y);
  QuantizedMul(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
             ::tensorflow::Input y, ::tensorflow::Input min_x,
             ::tensorflow::Input max_x, ::tensorflow::Input min_y,
             ::tensorflow::Input max_y, const QuantizedMul::Attrs& attrs);

  static Attrs Toutput(DataType x) {
    return Attrs().Toutput(x);
  }

  ::tensorflow::Output z;
  ::tensorflow::Output min_z;
  ::tensorflow::Output max_z;
};

/// Creates a sequence of numbers.
///
/// This operation creates a sequence of numbers that begins at `start` and
/// extends by increments of `delta` up to but not including `limit`.
///
/// For example:
///
/// ```
/// # 'start' is 3
/// # 'limit' is 18
/// # 'delta' is 3
/// tf.range(start, limit, delta) ==> [3, 6, 9, 12, 15]
/// ```
///
/// Arguments:
/// * scope: A Scope object
/// * start: 0-D (scalar). First entry in the sequence.
/// * limit: 0-D (scalar). Upper limit of sequence, exclusive.
/// * delta: 0-D (scalar). Optional. Default is 1. Number that increments `start`.
///
/// Returns:
/// * `Output`: 1-D.
class Range {
 public:
  Range(const ::tensorflow::Scope& scope, ::tensorflow::Input start,
      ::tensorflow::Input limit, ::tensorflow::Input delta);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Returns the real part of a complex number.
///
/// Given a tensor `input` of complex numbers, this operation returns a tensor of
/// type `float` that is the real part of each element in `input`. All elements in
/// `input` must be complex numbers of the form \\(a + bj\\), where *a* is the real
///  part returned by this operation and *b* is the imaginary part.
///
/// For example:
///
/// ```
/// # tensor 'input' is [-2.25 + 4.75j, 3.25 + 5.75j]
/// tf.real(input) ==> [-2.25, 3.25]
/// ```
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The output tensor.
class Real {
 public:
  /// Optional attribute setters for Real
  struct Attrs {
    /// Defaults to DT_FLOAT
    TF_MUST_USE_RESULT Attrs Tout(DataType x) {
      Attrs ret = *this;
      ret.Tout_ = x;
      return ret;
    }

    DataType Tout_ = DT_FLOAT;
  };
  Real(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  Real(const ::tensorflow::Scope& scope, ::tensorflow::Input input, const
     Real::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs Tout(DataType x) {
    return Attrs().Tout(x);
  }

  ::tensorflow::Output output;
};

/// Returns x / y element-wise for real types.
///
/// If `x` and `y` are reals, this will return the floating-point division.
///
/// *NOTE*: `Div` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class RealDiv {
 public:
  RealDiv(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
        ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Computes the reciprocal of x element-wise.
///
/// I.e., \\(y = 1 / x\\).
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Reciprocal {
 public:
  Reciprocal(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Given a quantized tensor described by (input, input_min, input_max), outputs a
///
/// range that covers the actual values present in that tensor.  This op is
/// typically used to produce the requested_output_min and requested_output_max for
/// Requantize.
///
/// Arguments:
/// * scope: A Scope object
/// * input_min: The float value that the minimum quantized input value represents.
/// * input_max: The float value that the maximum quantized input value represents.
///
/// Returns:
/// * `Output` output_min: The computed min output.
/// * `Output` output_max: the computed max output.
class RequantizationRange {
 public:
  RequantizationRange(const ::tensorflow::Scope& scope, ::tensorflow::Input
                    input, ::tensorflow::Input input_min, ::tensorflow::Input
                    input_max);

  ::tensorflow::Output output_min;
  ::tensorflow::Output output_max;
};

/// Convert the quantized 'input' tensor into a lower-precision 'output', using the
///
/// output range specified with 'requested_output_min' and 'requested_output_max'.
///
/// [input_min, input_max] are scalar floats that specify the range for the float
/// interpretation of the 'input' data. For example, if input_min is -1.0f and
/// input_max is 1.0f, and we are dealing with quint16 quantized data, then a 0
/// value in the 16-bit data should be interpreted as -1.0f, and a 65535 means 1.0f.
///
/// Arguments:
/// * scope: A Scope object
/// * input_min: The float value that the minimum quantized input value represents.
/// * input_max: The float value that the maximum quantized input value represents.
/// * requested_output_min: The float value that the minimum quantized output value represents.
/// * requested_output_max: The float value that the maximum quantized output value represents.
/// * out_type: The type of the output. Should be a lower bit depth than Tinput.
///
/// Returns:
/// * `Output` output
/// * `Output` output_min: The requested_output_min value is copied into this output.
/// * `Output` output_max: The requested_output_max value is copied into this output.
class Requantize {
 public:
  Requantize(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
           ::tensorflow::Input input_min, ::tensorflow::Input input_max,
           ::tensorflow::Input requested_output_min, ::tensorflow::Input
           requested_output_max, DataType out_type);

  ::tensorflow::Output output;
  ::tensorflow::Output output_min;
  ::tensorflow::Output output_max;
};

/// Returns element-wise integer closest to x.
///
/// If the result is midway between two representable values,
/// the even representable is chosen.
/// For example:
///
/// ```
/// rint(-1.5) ==> -2.0
/// rint(0.5000001) ==> 1.0
/// rint([-1.7, -1.5, -0.2, 0.2, 1.5, 1.7, 2.0]) ==> [-2., -2., -0., 0., 2., 2., 2.]
/// ```
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Rint {
 public:
  Rint(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Rounds the values of a tensor to the nearest integer, element-wise.
///
/// Rounds half to even.  Also known as bankers rounding. If you want to round
/// according to the current system rounding mode use std::cint.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Round {
 public:
  Round(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Computes reciprocal of square root of x element-wise.
///
/// I.e., \\(y = 1 / \sqrt{x}\\).
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Rsqrt {
 public:
  Rsqrt(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Computes the maximum along segments of a tensor.
///
/// Read @{$math_ops#Segmentation$the section on segmentation} for an explanation of
/// segments.
///
/// Computes a tensor such that
/// \\(output_i = \max_j(data_j)\\) where `max` is over `j` such
/// that `segment_ids[j] == i`.
///
/// If the max is empty for a given segment ID `i`, `output[i] = 0`.
///
/// <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
/// <img style="width:100%" src="https://www.tensorflow.org/images/SegmentMax.png" alt>
/// </div>
///
/// Arguments:
/// * scope: A Scope object
/// * segment_ids: A 1-D tensor whose rank is equal to the rank of `data`'s
/// first dimension.  Values should be sorted and can be repeated.
///
/// Returns:
/// * `Output`: Has same shape as data, except for dimension 0 which
/// has size `k`, the number of segments.
class SegmentMax {
 public:
  SegmentMax(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
           ::tensorflow::Input segment_ids);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes the mean along segments of a tensor.
///
/// Read @{$math_ops#Segmentation$the section on segmentation} for an explanation of
/// segments.
///
/// Computes a tensor such that
/// \\(output_i = \frac{\sum_j data_j}{N}\\) where `mean` is
/// over `j` such that `segment_ids[j] == i` and `N` is the total number of
/// values summed.
///
/// If the mean is empty for a given segment ID `i`, `output[i] = 0`.
///
/// <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
/// <img style="width:100%" src="https://www.tensorflow.org/images/SegmentMean.png" alt>
/// </div>
///
/// Arguments:
/// * scope: A Scope object
/// * segment_ids: A 1-D tensor whose rank is equal to the rank of `data`'s
/// first dimension.  Values should be sorted and can be repeated.
///
/// Returns:
/// * `Output`: Has same shape as data, except for dimension 0 which
/// has size `k`, the number of segments.
class SegmentMean {
 public:
  SegmentMean(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
            ::tensorflow::Input segment_ids);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes the minimum along segments of a tensor.
///
/// Read @{$math_ops#Segmentation$the section on segmentation} for an explanation of
/// segments.
///
/// Computes a tensor such that
/// \\(output_i = \min_j(data_j)\\) where `min` is over `j` such
/// that `segment_ids[j] == i`.
///
/// If the min is empty for a given segment ID `i`, `output[i] = 0`.
///
/// <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
/// <img style="width:100%" src="https://www.tensorflow.org/images/SegmentMin.png" alt>
/// </div>
///
/// Arguments:
/// * scope: A Scope object
/// * segment_ids: A 1-D tensor whose rank is equal to the rank of `data`'s
/// first dimension.  Values should be sorted and can be repeated.
///
/// Returns:
/// * `Output`: Has same shape as data, except for dimension 0 which
/// has size `k`, the number of segments.
class SegmentMin {
 public:
  SegmentMin(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
           ::tensorflow::Input segment_ids);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes the product along segments of a tensor.
///
/// Read @{$math_ops#Segmentation$the section on segmentation} for an explanation of
/// segments.
///
/// Computes a tensor such that
/// \\(output_i = \prod_j data_j\\) where the product is over `j` such
/// that `segment_ids[j] == i`.
///
/// If the product is empty for a given segment ID `i`, `output[i] = 1`.
///
/// <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
/// <img style="width:100%" src="https://www.tensorflow.org/images/SegmentProd.png" alt>
/// </div>
///
/// Arguments:
/// * scope: A Scope object
/// * segment_ids: A 1-D tensor whose rank is equal to the rank of `data`'s
/// first dimension.  Values should be sorted and can be repeated.
///
/// Returns:
/// * `Output`: Has same shape as data, except for dimension 0 which
/// has size `k`, the number of segments.
class SegmentProd {
 public:
  SegmentProd(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
            ::tensorflow::Input segment_ids);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes the sum along segments of a tensor.
///
/// Read @{$math_ops#Segmentation$the section on segmentation} for an explanation of
/// segments.
///
/// Computes a tensor such that
/// \\(output_i = \sum_j data_j\\) where sum is over `j` such
/// that `segment_ids[j] == i`.
///
/// If the sum is empty for a given segment ID `i`, `output[i] = 0`.
///
/// <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
/// <img style="width:100%" src="https://www.tensorflow.org/images/SegmentSum.png" alt>
/// </div>
///
/// Arguments:
/// * scope: A Scope object
/// * segment_ids: A 1-D tensor whose rank is equal to the rank of `data`'s
/// first dimension.  Values should be sorted and can be repeated.
///
/// Returns:
/// * `Output`: Has same shape as data, except for dimension 0 which
/// has size `k`, the number of segments.
class SegmentSum {
 public:
  SegmentSum(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
           ::tensorflow::Input segment_ids);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Selects elements from `x` or `y`, depending on `condition`.
///
/// The `x`, and `y` tensors must all have the same shape, and the
/// output will also have that shape.
///
/// The `condition` tensor must be a scalar if `x` and `y` are scalars.
/// If `x` and `y` are vectors or higher rank, then `condition` must be either a
/// scalar, a vector with size matching the first dimension of `x`, or must have
/// the same shape as `x`.
///
/// The `condition` tensor acts as a mask that chooses, based on the value at each
/// element, whether the corresponding element / row in the output should be
/// taken from `x` (if true) or `y` (if false).
///
/// If `condition` is a vector and `x` and `y` are higher rank matrices, then
/// it chooses which row (outer dimension) to copy from `x` and `y`.
/// If `condition` has the same shape as `x` and `y`, then it chooses which
/// element to copy from `x` and `y`.
///
/// For example:
///
/// ```python
/// # 'condition' tensor is [[True,  False]
/// #                        [False, True]]
/// # 't' is [[1, 2],
/// #         [3, 4]]
/// # 'e' is [[5, 6],
/// #         [7, 8]]
/// select(condition, t, e)  # => [[1, 6], [7, 4]]
///
///
/// # 'condition' tensor is [True, False]
/// # 't' is [[1, 2],
/// #         [3, 4]]
/// # 'e' is [[5, 6],
/// #         [7, 8]]
/// select(condition, t, e) ==> [[1, 2],
///                              [7, 8]]
///
/// ```
///
/// Arguments:
/// * scope: A Scope object
/// * x: = A `Tensor` which may have the same shape as `condition`.
/// If `condition` is rank 1, `x` may have higher rank,
/// but its first dimension must match the size of `condition`.
/// * y: = A `Tensor` with the same type and shape as `x`.
///
/// Returns:
/// * `Output`: = A `Tensor` with the same type and shape as `x` and `y`.
class Where3 {
 public:
  Where3(const ::tensorflow::Scope& scope, ::tensorflow::Input condition,
       ::tensorflow::Input x, ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes sigmoid of `x` element-wise.
///
/// Specifically, `y = 1 / (1 + exp(-x))`.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Sigmoid {
 public:
  Sigmoid(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Returns an element-wise indication of the sign of a number.
///
/// `y = sign(x) = -1` if `x < 0`; 0 if `x == 0`; 1 if `x > 0`.
///
/// For complex numbers, `y = sign(x) = x / |x|` if `x != 0`, otherwise `y = 0`.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Sign {
 public:
  Sign(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Computes sin of x element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Sin {
 public:
  Sin(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Computes hyperbolic sine of x element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Sinh {
 public:
  Sinh(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Multiply matrix "a" by matrix "b".
///
/// The inputs must be two-dimensional matrices and the inner dimension of "a" must
/// match the outer dimension of "b". Both "a" and "b" must be `Tensor`s not
/// `SparseTensor`s.  This op is optimized for the case where at least one of "a" or
/// "b" is sparse, in the sense that they have a large proportion of zero values.
/// The breakeven for using this versus a dense matrix multiply on one platform was
/// 30% zero values in the sparse matrix.
///
/// The gradient computation of this operation will only take advantage of sparsity
/// in the input gradient when that gradient comes from a Relu.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The product tensor.
class SparseMatMul {
 public:
  /// Optional attribute setters for SparseMatMul
  struct Attrs {
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs TransposeA(bool x) {
      Attrs ret = *this;
      ret.transpose_a_ = x;
      return ret;
    }

    /// Defaults to false
    TF_MUST_USE_RESULT Attrs TransposeB(bool x) {
      Attrs ret = *this;
      ret.transpose_b_ = x;
      return ret;
    }

    /// Defaults to false
    TF_MUST_USE_RESULT Attrs AIsSparse(bool x) {
      Attrs ret = *this;
      ret.a_is_sparse_ = x;
      return ret;
    }

    /// Defaults to false
    TF_MUST_USE_RESULT Attrs BIsSparse(bool x) {
      Attrs ret = *this;
      ret.b_is_sparse_ = x;
      return ret;
    }

    bool transpose_a_ = false;
    bool transpose_b_ = false;
    bool a_is_sparse_ = false;
    bool b_is_sparse_ = false;
  };
  SparseMatMul(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
             ::tensorflow::Input b);
  SparseMatMul(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
             ::tensorflow::Input b, const SparseMatMul::Attrs& attrs);
  operator ::tensorflow::Output() const { return product; }
  operator ::tensorflow::Input() const { return product; }
  ::tensorflow::Node* node() const { return product.node(); }

  static Attrs TransposeA(bool x) {
    return Attrs().TransposeA(x);
  }
  static Attrs TransposeB(bool x) {
    return Attrs().TransposeB(x);
  }
  static Attrs AIsSparse(bool x) {
    return Attrs().AIsSparse(x);
  }
  static Attrs BIsSparse(bool x) {
    return Attrs().BIsSparse(x);
  }

  ::tensorflow::Output product;
};

/// Computes the mean along sparse segments of a tensor.
///
/// Read @{$math_ops#Segmentation$the section on segmentation} for an explanation of
/// segments.
///
/// Like `SegmentMean`, but `segment_ids` can have rank less than `data`'s first
/// dimension, selecting a subset of dimension 0, specified by `indices`.
///
/// Arguments:
/// * scope: A Scope object
/// * indices: A 1-D tensor. Has same rank as `segment_ids`.
/// * segment_ids: A 1-D tensor. Values should be sorted and can be repeated.
///
/// Returns:
/// * `Output`: Has same shape as data, except for dimension 0 which
/// has size `k`, the number of segments.
class SparseSegmentMean {
 public:
  SparseSegmentMean(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
                  ::tensorflow::Input indices, ::tensorflow::Input segment_ids);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes gradients for SparseSegmentMean.
///
/// Returns tensor "output" with same shape as grad, except for dimension 0 whose
/// value is output_dim0.
///
/// Arguments:
/// * scope: A Scope object
/// * grad: gradient propagated to the SparseSegmentMean op.
/// * indices: indices passed to the corresponding SparseSegmentMean op.
/// * segment_ids: segment_ids passed to the corresponding SparseSegmentMean op.
/// * output_dim0: dimension 0 of "data" passed to SparseSegmentMean op.
///
/// Returns:
/// * `Output`: The output tensor.
class SparseSegmentMeanGrad {
 public:
  SparseSegmentMeanGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                      grad, ::tensorflow::Input indices, ::tensorflow::Input
                      segment_ids, ::tensorflow::Input output_dim0);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes the mean along sparse segments of a tensor.
///
/// Like `SparseSegmentMean`, but allows missing ids in `segment_ids`. If an id is
/// misisng, the `output` tensor at that position will be zeroed.
///
/// Read @{$math_ops#Segmentation$the section on segmentation} for an explanation of
/// segments.
///
/// Arguments:
/// * scope: A Scope object
/// * indices: A 1-D tensor. Has same rank as `segment_ids`.
/// * segment_ids: A 1-D tensor. Values should be sorted and can be repeated.
/// * num_segments: Should equal the number of distinct segment IDs.
///
/// Returns:
/// * `Output`: Has same shape as data, except for dimension 0 which has size
/// `num_segments`.
class SparseSegmentMeanWithNumSegments {
 public:
  SparseSegmentMeanWithNumSegments(const ::tensorflow::Scope& scope,
                                 ::tensorflow::Input data, ::tensorflow::Input
                                 indices, ::tensorflow::Input segment_ids,
                                 ::tensorflow::Input num_segments);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes the sum along sparse segments of a tensor divided by the sqrt of N.
///
/// N is the size of the segment being reduced.
///
/// Read @{$math_ops#Segmentation$the section on segmentation} for an explanation of
/// segments.
///
/// Arguments:
/// * scope: A Scope object
/// * indices: A 1-D tensor. Has same rank as `segment_ids`.
/// * segment_ids: A 1-D tensor. Values should be sorted and can be repeated.
///
/// Returns:
/// * `Output`: Has same shape as data, except for dimension 0 which
/// has size `k`, the number of segments.
class SparseSegmentSqrtN {
 public:
  SparseSegmentSqrtN(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
                   ::tensorflow::Input indices, ::tensorflow::Input
                   segment_ids);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes gradients for SparseSegmentSqrtN.
///
/// Returns tensor "output" with same shape as grad, except for dimension 0 whose
/// value is output_dim0.
///
/// Arguments:
/// * scope: A Scope object
/// * grad: gradient propagated to the SparseSegmentSqrtN op.
/// * indices: indices passed to the corresponding SparseSegmentSqrtN op.
/// * segment_ids: segment_ids passed to the corresponding SparseSegmentSqrtN op.
/// * output_dim0: dimension 0 of "data" passed to SparseSegmentSqrtN op.
///
/// Returns:
/// * `Output`: The output tensor.
class SparseSegmentSqrtNGrad {
 public:
  SparseSegmentSqrtNGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       grad, ::tensorflow::Input indices, ::tensorflow::Input
                       segment_ids, ::tensorflow::Input output_dim0);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes the sum along sparse segments of a tensor divided by the sqrt of N.
///
/// N is the size of the segment being reduced.
///
/// Like `SparseSegmentSqrtN`, but allows missing ids in `segment_ids`. If an id is
/// misisng, the `output` tensor at that position will be zeroed.
///
/// Read @{$math_ops#Segmentation$the section on segmentation} for an explanation of
/// segments.
///
/// Arguments:
/// * scope: A Scope object
/// * indices: A 1-D tensor. Has same rank as `segment_ids`.
/// * segment_ids: A 1-D tensor. Values should be sorted and can be repeated.
/// * num_segments: Should equal the number of distinct segment IDs.
///
/// Returns:
/// * `Output`: Has same shape as data, except for dimension 0 which
/// has size `k`, the number of segments.
class SparseSegmentSqrtNWithNumSegments {
 public:
  SparseSegmentSqrtNWithNumSegments(const ::tensorflow::Scope& scope,
                                  ::tensorflow::Input data, ::tensorflow::Input
                                  indices, ::tensorflow::Input segment_ids,
                                  ::tensorflow::Input num_segments);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes the sum along sparse segments of a tensor.
///
/// Read @{$math_ops#Segmentation$the section on segmentation} for an explanation of
/// segments.
///
/// Like `SegmentSum`, but `segment_ids` can have rank less than `data`'s first
/// dimension, selecting a subset of dimension 0, specified by `indices`.
///
/// For example:
///
/// ```python
/// c = tf.constant([[1,2,3,4], [-1,-2,-3,-4], [5,6,7,8]])
///
/// # Select two rows, one segment.
/// tf.sparse_segment_sum(c, tf.constant([0, 1]), tf.constant([0, 0]))
/// # => [[0 0 0 0]]
///
/// # Select two rows, two segment.
/// tf.sparse_segment_sum(c, tf.constant([0, 1]), tf.constant([0, 1]))
/// # => [[ 1  2  3  4]
/// #     [-1 -2 -3 -4]]
///
/// # Select all rows, two segments.
/// tf.sparse_segment_sum(c, tf.constant([0, 1, 2]), tf.constant([0, 0, 1]))
/// # => [[0 0 0 0]
/// #     [5 6 7 8]]
///
/// # Which is equivalent to:
/// tf.segment_sum(c, tf.constant([0, 0, 1]))
/// ```
///
/// Arguments:
/// * scope: A Scope object
/// * indices: A 1-D tensor. Has same rank as `segment_ids`.
/// * segment_ids: A 1-D tensor. Values should be sorted and can be repeated.
///
/// Returns:
/// * `Output`: Has same shape as data, except for dimension 0 which
/// has size `k`, the number of segments.
class SparseSegmentSum {
 public:
  SparseSegmentSum(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
                 ::tensorflow::Input indices, ::tensorflow::Input segment_ids);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes the sum along sparse segments of a tensor.
///
/// Like `SparseSegmentSum`, but allows missing ids in `segment_ids`. If an id is
/// misisng, the `output` tensor at that position will be zeroed.
///
/// Read @{$math_ops#Segmentation$the section on segmentation} for an explanation of
/// segments.
///
/// For example:
///
/// ```python
/// c = tf.constant([[1,2,3,4], [-1,-2,-3,-4], [5,6,7,8]])
///
/// tf.sparse_segment_sum_with_num_segments(
///     c, tf.constant([0, 1]), tf.constant([0, 0]), num_segments=3)
/// # => [[0 0 0 0]
/// #     [0 0 0 0]
/// #     [0 0 0 0]]
///
/// tf.sparse_segment_sum_with_num_segments(c,
///                                         tf.constant([0, 1]),
///                                         tf.constant([0, 2],
///                                         num_segments=4))
/// # => [[ 1  2  3  4]
/// #     [ 0  0  0  0]
/// #     [-1 -2 -3 -4]
/// #     [ 0  0  0  0]]
/// ```
///
/// Arguments:
/// * scope: A Scope object
/// * indices: A 1-D tensor. Has same rank as `segment_ids`.
/// * segment_ids: A 1-D tensor. Values should be sorted and can be repeated.
/// * num_segments: Should equal the number of distinct segment IDs.
///
/// Returns:
/// * `Output`: Has same shape as data, except for dimension 0 which
/// has size `num_segments`.
class SparseSegmentSumWithNumSegments {
 public:
  SparseSegmentSumWithNumSegments(const ::tensorflow::Scope& scope,
                                ::tensorflow::Input data, ::tensorflow::Input
                                indices, ::tensorflow::Input segment_ids,
                                ::tensorflow::Input num_segments);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes square root of x element-wise.
///
/// I.e., \\(y = \sqrt{x} = x^{1/2}\\).
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Sqrt {
 public:
  Sqrt(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Computes square of x element-wise.
///
/// I.e., \\(y = x * x = x^2\\).
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Square {
 public:
  Square(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Returns (x - y)(x - y) element-wise.
///
/// *NOTE*: `SquaredDifference` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class SquaredDifference {
 public:
  SquaredDifference(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
                  ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Returns x - y element-wise.
///
/// *NOTE*: `Subtract` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
///
/// Aliases:
/// * Sub
class Subtract {
 public:
  Subtract(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
         ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};
typedef Subtract Sub;

/// Computes the sum of elements across dimensions of a tensor.
///
/// Reduces `input` along the dimensions given in `axis`. Unless
/// `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
/// `axis`. If `keep_dims` is true, the reduced dimensions are
/// retained with length 1.
///
/// Arguments:
/// * scope: A Scope object
/// * input: The tensor to reduce.
/// * axis: The dimensions to reduce. Must be in the range
/// `[-rank(input), rank(input))`.
///
/// Optional attributes (see `Attrs`):
/// * keep_dims: If true, retain reduced dimensions with length 1.
///
/// Returns:
/// * `Output`: The reduced tensor.
///
/// Aliases:
/// * ReduceSum
class Sum {
 public:
  /// Optional attribute setters for Sum
  struct Attrs {
    /// If true, retain reduced dimensions with length 1.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs KeepDims(bool x) {
      Attrs ret = *this;
      ret.keep_dims_ = x;
      return ret;
    }

    bool keep_dims_ = false;
  };
  Sum(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
    ::tensorflow::Input axis);
  Sum(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
    ::tensorflow::Input axis, const Sum::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs KeepDims(bool x) {
    return Attrs().KeepDims(x);
  }

  ::tensorflow::Output output;
};
typedef Sum ReduceSum;

/// Computes tan of x element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Tan {
 public:
  Tan(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Computes hyperbolic tangent of `x` element-wise.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The y tensor.
class Tanh {
 public:
  Tanh(const ::tensorflow::Scope& scope, ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return y; }
  operator ::tensorflow::Input() const { return y; }
  ::tensorflow::Node* node() const { return y.node(); }

  ::tensorflow::Output y;
};

/// Returns x / y element-wise for integer types.
///
/// Truncation designates that negative numbers will round fractional quantities
/// toward zero. I.e. -7 / 5 = -1. This matches C semantics but it is different
/// than Python semantics. See `FloorDiv` for a division function that matches
/// Python Semantics.
///
/// *NOTE*: `TruncateDiv` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class TruncateDiv {
 public:
  TruncateDiv(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
            ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Returns element-wise remainder of division. This emulates C semantics in that
///
/// the result here is consistent with a truncating divide. E.g. `truncate(x / y) *
/// y + truncate_mod(x, y) = x`.
///
/// *NOTE*: `TruncateMod` supports broadcasting. More about broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class TruncateMod {
 public:
  TruncateMod(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
            ::tensorflow::Input y);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Computes the maximum along segments of a tensor.
///
/// Read @{$math_ops#Segmentation$the section on segmentation} for an explanation of
/// segments.
///
/// This operator is similar to the unsorted segment sum operator found
/// [(here)](../../../api_docs/python/math_ops.md#UnsortedSegmentSum).
/// Instead of computing the sum over segments, it computes the maximum such that:
///
/// \\(output_i = \max_j data_j\\) where max is over `j` such
/// that `segment_ids[j] == i`.
///
/// If the maximum is empty for a given segment ID `i`, it outputs the smallest
/// possible value for the specific numeric type,
/// `output[i] = numeric_limits<T>::lowest()`.
///
/// <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
/// <img style="width:100%" src="https://www.tensorflow.org/images/UnsortedSegmentMax.png" alt>
/// </div>
///
/// Arguments:
/// * scope: A Scope object
/// * segment_ids: A 1-D tensor whose rank is equal to the rank of `data`'s
/// first dimension.
///
/// Returns:
/// * `Output`: Has same shape as data, except for dimension 0 which
/// has size `num_segments`.
class UnsortedSegmentMax {
 public:
  UnsortedSegmentMax(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
                   ::tensorflow::Input segment_ids, ::tensorflow::Input
                   num_segments);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes the minimum along segments of a tensor.
///
/// Read @{$math_ops#segmentation$the section on segmentation} for an explanation of
/// segments.
///
/// This operator is similar to the unsorted segment sum operator found
/// [(here)](../../../api_docs/python/math_ops.md#UnsortedSegmentSum).
/// Instead of computing the sum over segments, it computes the minimum such that:
///
/// \\(output_i = \min_j data_j\\) where min is over `j` such
/// that `segment_ids[j] == i`.
///
/// If the minimum is empty for a given segment ID `i`, it outputs the largest
/// possible value for the specific numeric type,
/// `output[i] = numeric_limits<T>::max()`.
///
/// Arguments:
/// * scope: A Scope object
/// * segment_ids: A 1-D tensor whose rank is equal to the rank of `data`'s
/// first dimension.
///
/// Returns:
/// * `Output`: Has same shape as data, except for dimension 0 which
/// has size `num_segments`.
class UnsortedSegmentMin {
 public:
  UnsortedSegmentMin(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
                   ::tensorflow::Input segment_ids, ::tensorflow::Input
                   num_segments);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes the product along segments of a tensor.
///
/// Read @{$math_ops#segmentation$the section on segmentation} for an explanation of
/// segments.
///
/// This operator is similar to the unsorted segment sum operator found
/// [(here)](../../../api_docs/python/math_ops.md#UnsortedSegmentSum).
/// Instead of computing the sum over segments, it computes the product of all
/// entries belonging to a segment such that:
///
/// \\(output_i = \prod_j data_j\\) where the product is over `j` such
/// that `segment_ids[j] == i`.
///
/// If there is no entry for a given segment ID `i`, it outputs 1.
///
/// Arguments:
/// * scope: A Scope object
/// * segment_ids: A 1-D tensor whose rank is equal to the rank of `data`'s
/// first dimension.
///
/// Returns:
/// * `Output`: Has same shape as data, except for dimension 0 which
/// has size `num_segments`.
class UnsortedSegmentProd {
 public:
  UnsortedSegmentProd(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
                    ::tensorflow::Input segment_ids, ::tensorflow::Input
                    num_segments);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes the sum along segments of a tensor.
///
/// Read @{$math_ops#Segmentation$the section on segmentation} for an explanation of
/// segments.
///
/// Computes a tensor such that
/// \\(output[i] = sum_{j...} data[j...]\\) where the sum is over tuples `j...` such
/// that `segment_ids[j...] == i`.  Unlike `SegmentSum`, `segment_ids`
/// need not be sorted and need not cover all values in the full
/// range of valid values.
///
/// If the sum is empty for a given segment ID `i`, `output[i] = 0`.
/// If the given segment ID `i` is negative, the value is dropped and will not be
/// added to the sum of the segment.
///
/// `num_segments` should equal the number of distinct segment IDs.
///
/// <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
/// <img style="width:100%" src="https://www.tensorflow.org/images/UnsortedSegmentSum.png" alt>
/// </div>
///
/// Arguments:
/// * scope: A Scope object
/// * segment_ids: A tensor whose shape is a prefix of `data.shape`.
///
/// Returns:
/// * `Output`: Has same shape as data, except for the first `segment_ids.rank`
/// dimensions, which are replaced with a single dimension which has size
/// `num_segments`.
class UnsortedSegmentSum {
 public:
  UnsortedSegmentSum(const ::tensorflow::Scope& scope, ::tensorflow::Input data,
                   ::tensorflow::Input segment_ids, ::tensorflow::Input
                   num_segments);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Compute the Hurwitz zeta function \\(\zeta(x, q)\\).
///
/// The Hurwitz zeta function is defined as:
///
///
/// \\(\zeta(x, q) = \sum_{n=0}^{\infty} (q + n)^{-x}\\)
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class Zeta {
 public:
  Zeta(const ::tensorflow::Scope& scope, ::tensorflow::Input x,
     ::tensorflow::Input q);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// @}

}  // namespace ops
}  // namespace tensorflow

#endif  // TENSORFLOW_CC_OPS_MATH_OPS_H_
