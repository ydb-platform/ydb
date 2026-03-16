// This file is MACHINE GENERATED! Do not edit.

#ifndef TENSORFLOW_CC_OPS_ARRAY_OPS_INTERNAL_H_
#define TENSORFLOW_CC_OPS_ARRAY_OPS_INTERNAL_H_

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

/// @defgroup array_ops_internal Array Ops Internal
/// @{

/// Return the reduction indices for computing gradients of s0 op s1 with broadcast.
///
/// This is typically used by gradient computations for a broadcasting operation.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output` r0
/// * `Output` r1
class BroadcastGradientArgs {
 public:
  BroadcastGradientArgs(const ::tensorflow::Scope& scope, ::tensorflow::Input s0,
                      ::tensorflow::Input s1);

  ::tensorflow::Output r0;
  ::tensorflow::Output r1;
};

/// Gradient op for `MirrorPad` op. This op folds a mirror-padded tensor.
///
/// This operation folds the padded areas of `input` by `MirrorPad` according to the
/// `paddings` you specify. `paddings` must be the same as `paddings` argument
/// given to the corresponding `MirrorPad` op.
///
/// The folded size of each dimension D of the output is:
///
/// `input.dim_size(D) - paddings(D, 0) - paddings(D, 1)`
///
/// For example:
///
/// ```
/// # 't' is [[1, 2, 3], [4, 5, 6], [7, 8, 9]].
/// # 'paddings' is [[0, 1]], [0, 1]].
/// # 'mode' is SYMMETRIC.
/// # rank of 't' is 2.
/// pad(t, paddings) ==> [[ 1,  5]
///                       [11, 28]]
/// ```
///
/// Arguments:
/// * scope: A Scope object
/// * input: The input tensor to be folded.
/// * paddings: A two-column matrix specifying the padding sizes. The number of
/// rows must be the same as the rank of `input`.
/// * mode: The mode used in the `MirrorPad` op.
///
/// Returns:
/// * `Output`: The folded tensor.
class MirrorPadGrad {
 public:
  MirrorPadGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
              ::tensorflow::Input paddings, StringPiece mode);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Return the same ref tensor as the input ref tensor.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The output tensor.
class RefIdentity {
 public:
  RefIdentity(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

}  // namespace internal
}  // namespace ops
}  // namespace tensorflow

#endif  // TENSORFLOW_CC_OPS_ARRAY_OPS_INTERNAL_H_
