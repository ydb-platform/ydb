// This file is MACHINE GENERATED! Do not edit.

#ifndef TENSORFLOW_CC_OPS_NN_OPS_INTERNAL_H_
#define TENSORFLOW_CC_OPS_NN_OPS_INTERNAL_H_

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

/// @defgroup nn_ops_internal Nn Ops Internal
/// @{

/// Computes gradients of the average pooling function.
///
/// Arguments:
/// * scope: A Scope object
/// * orig_input_shape: 1-D.  Shape of the original input to `avg_pool`.
/// * grad: 4-D with shape `[batch, height, width, channels]`.  Gradients w.r.t.
/// the output of `avg_pool`.
/// * ksize: The size of the sliding window for each dimension of the input.
/// * strides: The stride of the sliding window for each dimension of the input.
/// * padding: The type of padding algorithm to use.
///
/// Optional attributes (see `Attrs`):
/// * data_format: Specify the data format of the input and output data. With the
/// default format "NHWC", the data is stored in the order of:
///     [batch, in_height, in_width, in_channels].
/// Alternatively, the format could be "NCHW", the data storage order of:
///     [batch, in_channels, in_height, in_width].
///
/// Returns:
/// * `Output`: 4-D.  Gradients w.r.t. the input of `avg_pool`.
class AvgPoolGrad {
 public:
  /// Optional attribute setters for AvgPoolGrad
  struct Attrs {
    /// Specify the data format of the input and output data. With the
    /// default format "NHWC", the data is stored in the order of:
    ///     [batch, in_height, in_width, in_channels].
    /// Alternatively, the format could be "NCHW", the data storage order of:
    ///     [batch, in_channels, in_height, in_width].
    ///
    /// Defaults to "NHWC"
    TF_MUST_USE_RESULT Attrs DataFormat(StringPiece x) {
      Attrs ret = *this;
      ret.data_format_ = x;
      return ret;
    }

    StringPiece data_format_ = "NHWC";
  };
  AvgPoolGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
            orig_input_shape, ::tensorflow::Input grad, const
            gtl::ArraySlice<int>& ksize, const gtl::ArraySlice<int>& strides,
            StringPiece padding);
  AvgPoolGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
            orig_input_shape, ::tensorflow::Input grad, const
            gtl::ArraySlice<int>& ksize, const gtl::ArraySlice<int>& strides,
            StringPiece padding, const AvgPoolGrad::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs DataFormat(StringPiece x) {
    return Attrs().DataFormat(x);
  }

  ::tensorflow::Output output;
};

/// Computes gradients for the exponential linear (Elu) operation.
///
/// Arguments:
/// * scope: A Scope object
/// * gradients: The backpropagated gradients to the corresponding Elu operation.
/// * outputs: The outputs of the corresponding Elu operation.
///
/// Returns:
/// * `Output`: The gradients: `gradients * (outputs + 1)` if outputs < 0,
/// `gradients` otherwise.
class EluGrad {
 public:
  EluGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input gradients,
        ::tensorflow::Input outputs);
  operator ::tensorflow::Output() const { return backprops; }
  operator ::tensorflow::Input() const { return backprops; }
  ::tensorflow::Node* node() const { return backprops.node(); }

  ::tensorflow::Output backprops;
};

/// Computes gradient of the FractionalAvgPool function.
///
/// Unlike FractionalMaxPoolGrad, we don't need to find arg_max for
/// FractionalAvgPoolGrad, we just need to evenly back-propagate each element of
/// out_backprop to those indices that form the same pooling cell. Therefore, we
/// just need to know the shape of original input tensor, instead of the whole
/// tensor.
///
/// Arguments:
/// * scope: A Scope object
/// * orig_input_tensor_shape: Original input tensor shape for `fractional_avg_pool`
/// * out_backprop: 4-D with shape `[batch, height, width, channels]`.  Gradients
/// w.r.t. the output of `fractional_avg_pool`.
/// * row_pooling_sequence: row pooling sequence, form pooling region with
/// col_pooling_sequence.
/// * col_pooling_sequence: column pooling sequence, form pooling region with
/// row_pooling sequence.
///
/// Optional attributes (see `Attrs`):
/// * overlapping: When set to True, it means when pooling, the values at the boundary
/// of adjacent pooling cells are used by both cells. For example:
///
/// `index  0  1  2  3  4`
///
/// `value  20 5  16 3  7`
///
/// If the pooling sequence is [0, 2, 4], then 16, at index 2 will be used twice.
/// The result would be [41/3, 26/3] for fractional avg pooling.
///
/// Returns:
/// * `Output`: 4-D.  Gradients w.r.t. the input of `fractional_avg_pool`.
class FractionalAvgPoolGrad {
 public:
  /// Optional attribute setters for FractionalAvgPoolGrad
  struct Attrs {
    /// When set to True, it means when pooling, the values at the boundary
    /// of adjacent pooling cells are used by both cells. For example:
    ///
    /// `index  0  1  2  3  4`
    ///
    /// `value  20 5  16 3  7`
    ///
    /// If the pooling sequence is [0, 2, 4], then 16, at index 2 will be used twice.
    /// The result would be [41/3, 26/3] for fractional avg pooling.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs Overlapping(bool x) {
      Attrs ret = *this;
      ret.overlapping_ = x;
      return ret;
    }

    bool overlapping_ = false;
  };
  FractionalAvgPoolGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                      orig_input_tensor_shape, ::tensorflow::Input
                      out_backprop, ::tensorflow::Input row_pooling_sequence,
                      ::tensorflow::Input col_pooling_sequence);
  FractionalAvgPoolGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                      orig_input_tensor_shape, ::tensorflow::Input
                      out_backprop, ::tensorflow::Input row_pooling_sequence,
                      ::tensorflow::Input col_pooling_sequence, const
                      FractionalAvgPoolGrad::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs Overlapping(bool x) {
    return Attrs().Overlapping(x);
  }

  ::tensorflow::Output output;
};

/// Computes gradient of the FractionalMaxPool function.
///
/// Arguments:
/// * scope: A Scope object
/// * orig_input: Original input for `fractional_max_pool`
/// * orig_output: Original output for `fractional_max_pool`
/// * out_backprop: 4-D with shape `[batch, height, width, channels]`.  Gradients
/// w.r.t. the output of `fractional_max_pool`.
/// * row_pooling_sequence: row pooling sequence, form pooling region with
/// col_pooling_sequence.
/// * col_pooling_sequence: column pooling sequence, form pooling region with
/// row_pooling sequence.
///
/// Optional attributes (see `Attrs`):
/// * overlapping: When set to True, it means when pooling, the values at the boundary
/// of adjacent pooling cells are used by both cells. For example:
///
/// `index  0  1  2  3  4`
///
/// `value  20 5  16 3  7`
///
/// If the pooling sequence is [0, 2, 4], then 16, at index 2 will be used twice.
/// The result would be [20, 16] for fractional max pooling.
///
/// Returns:
/// * `Output`: 4-D.  Gradients w.r.t. the input of `fractional_max_pool`.
class FractionalMaxPoolGrad {
 public:
  /// Optional attribute setters for FractionalMaxPoolGrad
  struct Attrs {
    /// When set to True, it means when pooling, the values at the boundary
    /// of adjacent pooling cells are used by both cells. For example:
    ///
    /// `index  0  1  2  3  4`
    ///
    /// `value  20 5  16 3  7`
    ///
    /// If the pooling sequence is [0, 2, 4], then 16, at index 2 will be used twice.
    /// The result would be [20, 16] for fractional max pooling.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs Overlapping(bool x) {
      Attrs ret = *this;
      ret.overlapping_ = x;
      return ret;
    }

    bool overlapping_ = false;
  };
  FractionalMaxPoolGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                      orig_input, ::tensorflow::Input orig_output,
                      ::tensorflow::Input out_backprop, ::tensorflow::Input
                      row_pooling_sequence, ::tensorflow::Input
                      col_pooling_sequence);
  FractionalMaxPoolGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input
                      orig_input, ::tensorflow::Input orig_output,
                      ::tensorflow::Input out_backprop, ::tensorflow::Input
                      row_pooling_sequence, ::tensorflow::Input
                      col_pooling_sequence, const FractionalMaxPoolGrad::Attrs&
                      attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs Overlapping(bool x) {
    return Attrs().Overlapping(x);
  }

  ::tensorflow::Output output;
};

/// Gradients for Local Response Normalization.
///
/// Arguments:
/// * scope: A Scope object
/// * input_grads: 4-D with shape `[batch, height, width, channels]`.
/// * input_image: 4-D with shape `[batch, height, width, channels]`.
/// * output_image: 4-D with shape `[batch, height, width, channels]`.
///
/// Optional attributes (see `Attrs`):
/// * depth_radius: A depth radius.
/// * bias: An offset (usually > 0 to avoid dividing by 0).
/// * alpha: A scale factor, usually positive.
/// * beta: An exponent.
///
/// Returns:
/// * `Output`: The gradients for LRN.
class LRNGrad {
 public:
  /// Optional attribute setters for LRNGrad
  struct Attrs {
    /// A depth radius.
    ///
    /// Defaults to 5
    TF_MUST_USE_RESULT Attrs DepthRadius(int64 x) {
      Attrs ret = *this;
      ret.depth_radius_ = x;
      return ret;
    }

    /// An offset (usually > 0 to avoid dividing by 0).
    ///
    /// Defaults to 1
    TF_MUST_USE_RESULT Attrs Bias(float x) {
      Attrs ret = *this;
      ret.bias_ = x;
      return ret;
    }

    /// A scale factor, usually positive.
    ///
    /// Defaults to 1
    TF_MUST_USE_RESULT Attrs Alpha(float x) {
      Attrs ret = *this;
      ret.alpha_ = x;
      return ret;
    }

    /// An exponent.
    ///
    /// Defaults to 0.5
    TF_MUST_USE_RESULT Attrs Beta(float x) {
      Attrs ret = *this;
      ret.beta_ = x;
      return ret;
    }

    int64 depth_radius_ = 5;
    float bias_ = 1.0f;
    float alpha_ = 1.0f;
    float beta_ = 0.5f;
  };
  LRNGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input input_grads,
        ::tensorflow::Input input_image, ::tensorflow::Input output_image);
  LRNGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input input_grads,
        ::tensorflow::Input input_image, ::tensorflow::Input output_image,
        const LRNGrad::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs DepthRadius(int64 x) {
    return Attrs().DepthRadius(x);
  }
  static Attrs Bias(float x) {
    return Attrs().Bias(x);
  }
  static Attrs Alpha(float x) {
    return Attrs().Alpha(x);
  }
  static Attrs Beta(float x) {
    return Attrs().Beta(x);
  }

  ::tensorflow::Output output;
};

/// Computes gradients of the maxpooling function.
///
/// Arguments:
/// * scope: A Scope object
/// * orig_input: The original input tensor.
/// * orig_output: The original output tensor.
/// * grad: 4-D.  Gradients w.r.t. the output of `max_pool`.
/// * ksize: The size of the window for each dimension of the input tensor.
/// * strides: The stride of the sliding window for each dimension of the
/// input tensor.
/// * padding: The type of padding algorithm to use.
///
/// Optional attributes (see `Attrs`):
/// * data_format: Specify the data format of the input and output data. With the
/// default format "NHWC", the data is stored in the order of:
///     [batch, in_height, in_width, in_channels].
/// Alternatively, the format could be "NCHW", the data storage order of:
///     [batch, in_channels, in_height, in_width].
///
/// Returns:
/// * `Output`: Gradients w.r.t. the input to `max_pool`.
class MaxPoolGrad {
 public:
  /// Optional attribute setters for MaxPoolGrad
  struct Attrs {
    /// Specify the data format of the input and output data. With the
    /// default format "NHWC", the data is stored in the order of:
    ///     [batch, in_height, in_width, in_channels].
    /// Alternatively, the format could be "NCHW", the data storage order of:
    ///     [batch, in_channels, in_height, in_width].
    ///
    /// Defaults to "NHWC"
    TF_MUST_USE_RESULT Attrs DataFormat(StringPiece x) {
      Attrs ret = *this;
      ret.data_format_ = x;
      return ret;
    }

    StringPiece data_format_ = "NHWC";
  };
  MaxPoolGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input orig_input,
            ::tensorflow::Input orig_output, ::tensorflow::Input grad, const
            gtl::ArraySlice<int>& ksize, const gtl::ArraySlice<int>& strides,
            StringPiece padding);
  MaxPoolGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input orig_input,
            ::tensorflow::Input orig_output, ::tensorflow::Input grad, const
            gtl::ArraySlice<int>& ksize, const gtl::ArraySlice<int>& strides,
            StringPiece padding, const MaxPoolGrad::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs DataFormat(StringPiece x) {
    return Attrs().DataFormat(x);
  }

  ::tensorflow::Output output;
};

/// Computes gradients of the maxpooling function.
///
/// Arguments:
/// * scope: A Scope object
/// * input: The original input.
/// * grad: 4-D with shape `[batch, height, width, channels]`.  Gradients w.r.t. the
/// output of `max_pool`.
/// * argmax: The indices of the maximum values chosen for each output of `max_pool`.
/// * ksize: The size of the window for each dimension of the input tensor.
/// * strides: The stride of the sliding window for each dimension of the
/// input tensor.
/// * padding: The type of padding algorithm to use.
///
/// Returns:
/// * `Output`: Gradients w.r.t. the input of `max_pool`.
class MaxPoolGradWithArgmax {
 public:
  MaxPoolGradWithArgmax(const ::tensorflow::Scope& scope, ::tensorflow::Input
                      input, ::tensorflow::Input grad, ::tensorflow::Input
                      argmax, const gtl::ArraySlice<int>& ksize, const
                      gtl::ArraySlice<int>& strides, StringPiece padding);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes rectified linear 6 gradients for a Relu6 operation.
///
/// Arguments:
/// * scope: A Scope object
/// * gradients: The backpropagated gradients to the corresponding Relu6 operation.
/// * features: The features passed as input to the corresponding Relu6 operation, or
/// its output; using either one produces the same result.
///
/// Returns:
/// * `Output`: The gradients:
/// `gradients * (features > 0) * (features < 6)`.
class Relu6Grad {
 public:
  Relu6Grad(const ::tensorflow::Scope& scope, ::tensorflow::Input gradients,
          ::tensorflow::Input features);
  operator ::tensorflow::Output() const { return backprops; }
  operator ::tensorflow::Input() const { return backprops; }
  ::tensorflow::Node* node() const { return backprops.node(); }

  ::tensorflow::Output backprops;
};

/// Computes rectified linear gradients for a Relu operation.
///
/// Arguments:
/// * scope: A Scope object
/// * gradients: The backpropagated gradients to the corresponding Relu operation.
/// * features: The features passed as input to the corresponding Relu operation, OR
/// the outputs of that operation (both work equivalently).
///
/// Returns:
/// * `Output`: `gradients * (features > 0)`.
class ReluGrad {
 public:
  ReluGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input gradients,
         ::tensorflow::Input features);
  operator ::tensorflow::Output() const { return backprops; }
  operator ::tensorflow::Input() const { return backprops; }
  ::tensorflow::Node* node() const { return backprops.node(); }

  ::tensorflow::Output backprops;
};

/// Computes gradients for the scaled exponential linear (Selu) operation.
///
/// Arguments:
/// * scope: A Scope object
/// * gradients: The backpropagated gradients to the corresponding Selu operation.
/// * outputs: The outputs of the corresponding Selu operation.
///
/// Returns:
/// * `Output`: The gradients: `gradients * (outputs + scale * alpha)`
/// if outputs < 0, `scale * gradients` otherwise.
class SeluGrad {
 public:
  SeluGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input gradients,
         ::tensorflow::Input outputs);
  operator ::tensorflow::Output() const { return backprops; }
  operator ::tensorflow::Input() const { return backprops; }
  ::tensorflow::Node* node() const { return backprops.node(); }

  ::tensorflow::Output backprops;
};

/// Computes softplus gradients for a softplus operation.
///
/// Arguments:
/// * scope: A Scope object
/// * gradients: The backpropagated gradients to the corresponding softplus operation.
/// * features: The features passed as input to the corresponding softplus operation.
///
/// Returns:
/// * `Output`: The gradients: `gradients / (1 + exp(-features))`.
class SoftplusGrad {
 public:
  SoftplusGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input gradients,
             ::tensorflow::Input features);
  operator ::tensorflow::Output() const { return backprops; }
  operator ::tensorflow::Input() const { return backprops; }
  ::tensorflow::Node* node() const { return backprops.node(); }

  ::tensorflow::Output backprops;
};

/// Computes softsign gradients for a softsign operation.
///
/// Arguments:
/// * scope: A Scope object
/// * gradients: The backpropagated gradients to the corresponding softsign operation.
/// * features: The features passed as input to the corresponding softsign operation.
///
/// Returns:
/// * `Output`: The gradients: `gradients / (1 + abs(features)) ** 2`.
class SoftsignGrad {
 public:
  SoftsignGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input gradients,
             ::tensorflow::Input features);
  operator ::tensorflow::Output() const { return backprops; }
  operator ::tensorflow::Input() const { return backprops; }
  ::tensorflow::Node* node() const { return backprops.node(); }

  ::tensorflow::Output backprops;
};

}  // namespace internal
}  // namespace ops
}  // namespace tensorflow

#endif  // TENSORFLOW_CC_OPS_NN_OPS_INTERNAL_H_
