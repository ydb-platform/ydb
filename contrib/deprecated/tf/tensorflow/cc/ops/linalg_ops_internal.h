// This file is MACHINE GENERATED! Do not edit.

#ifndef TENSORFLOW_CC_OPS_LINALG_OPS_INTERNAL_H_
#define TENSORFLOW_CC_OPS_LINALG_OPS_INTERNAL_H_

// This file is MACHINE GENERATED! Do not edit.

#include "tensorflow/cc/framework/ops.h"
#include "tensorflow/cc/framework/scope.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/lib/gtl/array_slice.h"

namespace tensorflow {
namespace ops {
namespace internal {
// NOTE: This namespace has internal TensorFlow details that
// are not part of TensorFlow's public API.

/// @defgroup linalg_ops_internal Linalg Ops Internal
/// @{

/// Computes the matrix logarithm of one or more square matrices:
///
///
/// \\(log(exp(A)) = A\\)
///
/// This op is only defined for complex matrices. If A is positive-definite and
/// real, then casting to a complex matrix, taking the logarithm and casting back
/// to a real matrix will give the correct result.
///
/// This function computes the matrix logarithm using the Schur-Parlett algorithm.
/// Details of the algorithm can be found in Section 11.6.2 of:
/// Nicholas J. Higham, Functions of Matrices: Theory and Computation, SIAM 2008.
/// ISBN 978-0-898716-46-7.
///
/// The input is a tensor of shape `[..., M, M]` whose inner-most 2 dimensions
/// form square matrices. The output is a tensor of the same shape as the input
/// containing the exponential for all input submatrices `[..., :, :]`.
///
/// Arguments:
/// * scope: A Scope object
/// * input: Shape is `[..., M, M]`.
///
/// Returns:
/// * `Output`: Shape is `[..., M, M]`.
///
/// @compatibility(scipy)
/// Equivalent to scipy.linalg.logm
/// @end_compatibility
class MatrixLogarithm {
 public:
  MatrixLogarithm(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

}  // namespace internal
}  // namespace ops
}  // namespace tensorflow

#endif  // TENSORFLOW_CC_OPS_LINALG_OPS_INTERNAL_H_
