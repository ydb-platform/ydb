// This file is MACHINE GENERATED! Do not edit.

#ifndef TENSORFLOW_CC_OPS_MATH_OPS_INTERNAL_H_
#define TENSORFLOW_CC_OPS_MATH_OPS_INTERNAL_H_

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

/// @defgroup math_ops_internal Math Ops Internal
/// @{

/// Computes the gradient of `igamma(a, x)` wrt `a`.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class IgammaGradA {
 public:
  IgammaGradA(const ::tensorflow::Scope& scope, ::tensorflow::Input a,
            ::tensorflow::Input x);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Computes the gradient for the inverse of `x` wrt its input.
///
/// Specifically, `grad = -dy * y*y`, where `y = 1/x`, and `dy`
/// is the corresponding input gradient.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class InvGrad {
 public:
  InvGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input y,
        ::tensorflow::Input dy);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Computes the gradient for the inverse of `x` wrt its input.
///
/// Specifically, `grad = -dy * y*y`, where `y = 1/x`, and `dy`
/// is the corresponding input gradient.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class ReciprocalGrad {
 public:
  ReciprocalGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input y,
               ::tensorflow::Input dy);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Computes the gradient for the rsqrt of `x` wrt its input.
///
/// Specifically, `grad = dy * -0.5 * y^3`, where `y = rsqrt(x)`, and `dy`
/// is the corresponding input gradient.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class RsqrtGrad {
 public:
  RsqrtGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input y,
          ::tensorflow::Input dy);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Computes the gradient of the sigmoid of `x` wrt its input.
///
/// Specifically, `grad = dy * y * (1 - y)`, where `y = sigmoid(x)`, and
/// `dy` is the corresponding input gradient.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class SigmoidGrad {
 public:
  SigmoidGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input y,
            ::tensorflow::Input dy);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Computes the gradient for the sqrt of `x` wrt its input.
///
/// Specifically, `grad = dy * 0.5 / y`, where `y = sqrt(x)`, and `dy`
/// is the corresponding input gradient.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class SqrtGrad {
 public:
  SqrtGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input y,
         ::tensorflow::Input dy);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

/// Computes the gradient for the tanh of `x` wrt its input.
///
/// Specifically, `grad = dy * (1 - y*y)`, where `y = tanh(x)`, and `dy`
/// is the corresponding input gradient.
///
/// Arguments:
/// * scope: A Scope object
///
/// Returns:
/// * `Output`: The z tensor.
class TanhGrad {
 public:
  TanhGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input y,
         ::tensorflow::Input dy);
  operator ::tensorflow::Output() const { return z; }
  operator ::tensorflow::Input() const { return z; }
  ::tensorflow::Node* node() const { return z.node(); }

  ::tensorflow::Output z;
};

}  // namespace internal
}  // namespace ops
}  // namespace tensorflow

#endif  // TENSORFLOW_CC_OPS_MATH_OPS_INTERNAL_H_
