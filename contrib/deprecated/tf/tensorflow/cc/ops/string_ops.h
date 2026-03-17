// This file is MACHINE GENERATED! Do not edit.

#ifndef TENSORFLOW_CC_OPS_STRING_OPS_H_
#define TENSORFLOW_CC_OPS_STRING_OPS_H_

// This file is MACHINE GENERATED! Do not edit.

#include "tensorflow/cc/framework/ops.h"
#include "tensorflow/cc/framework/scope.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/lib/gtl/array_slice.h"

namespace tensorflow {
namespace ops {

/// @defgroup string_ops String Ops
/// @{

/// Converts each entry in the given tensor to strings.  Supports many numeric
///
/// types and boolean.
///
/// Arguments:
/// * scope: A Scope object
///
/// Optional attributes (see `Attrs`):
/// * precision: The post-decimal precision to use for floating point numbers.
/// Only used if precision > -1.
/// * scientific: Use scientific notation for floating point numbers.
/// * shortest: Use shortest representation (either scientific or standard) for
/// floating point numbers.
/// * width: Pad pre-decimal numbers to this width.
/// Applies to both floating point and integer numbers.
/// Only used if width > -1.
/// * fill: The value to pad if width > -1.  If empty, pads with spaces.
/// Another typical value is '0'.  String cannot be longer than 1 character.
///
/// Returns:
/// * `Output`: The output tensor.
class AsString {
 public:
  /// Optional attribute setters for AsString
  struct Attrs {
    /// The post-decimal precision to use for floating point numbers.
    /// Only used if precision > -1.
    ///
    /// Defaults to -1
    TF_MUST_USE_RESULT Attrs Precision(int64 x) {
      Attrs ret = *this;
      ret.precision_ = x;
      return ret;
    }

    /// Use scientific notation for floating point numbers.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs Scientific(bool x) {
      Attrs ret = *this;
      ret.scientific_ = x;
      return ret;
    }

    /// Use shortest representation (either scientific or standard) for
    /// floating point numbers.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs Shortest(bool x) {
      Attrs ret = *this;
      ret.shortest_ = x;
      return ret;
    }

    /// Pad pre-decimal numbers to this width.
    /// Applies to both floating point and integer numbers.
    /// Only used if width > -1.
    ///
    /// Defaults to -1
    TF_MUST_USE_RESULT Attrs Width(int64 x) {
      Attrs ret = *this;
      ret.width_ = x;
      return ret;
    }

    /// The value to pad if width > -1.  If empty, pads with spaces.
    /// Another typical value is '0'.  String cannot be longer than 1 character.
    ///
    /// Defaults to ""
    TF_MUST_USE_RESULT Attrs Fill(StringPiece x) {
      Attrs ret = *this;
      ret.fill_ = x;
      return ret;
    }

    int64 precision_ = -1;
    bool scientific_ = false;
    bool shortest_ = false;
    int64 width_ = -1;
    StringPiece fill_ = "";
  };
  AsString(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  AsString(const ::tensorflow::Scope& scope, ::tensorflow::Input input, const
         AsString::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs Precision(int64 x) {
    return Attrs().Precision(x);
  }
  static Attrs Scientific(bool x) {
    return Attrs().Scientific(x);
  }
  static Attrs Shortest(bool x) {
    return Attrs().Shortest(x);
  }
  static Attrs Width(int64 x) {
    return Attrs().Width(x);
  }
  static Attrs Fill(StringPiece x) {
    return Attrs().Fill(x);
  }

  ::tensorflow::Output output;
};

/// Decode web-safe base64-encoded strings.
///
/// Input may or may not have padding at the end. See EncodeBase64 for padding.
/// Web-safe means that input must use - and _ instead of + and /.
///
/// Arguments:
/// * scope: A Scope object
/// * input: Base64 strings to decode.
///
/// Returns:
/// * `Output`: Decoded strings.
class DecodeBase64 {
 public:
  DecodeBase64(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Encode strings into web-safe base64 format.
///
/// Refer to the following article for more information on base64 format:
/// en.wikipedia.org/wiki/Base64. Base64 strings may have padding with '=' at the
/// end so that the encoded has length multiple of 4. See Padding section of the
/// link above.
///
/// Web-safe means that the encoder uses - and _ instead of + and /.
///
/// Arguments:
/// * scope: A Scope object
/// * input: Strings to be encoded.
///
/// Optional attributes (see `Attrs`):
/// * pad: Bool whether padding is applied at the ends.
///
/// Returns:
/// * `Output`: Input strings encoded in base64.
class EncodeBase64 {
 public:
  /// Optional attribute setters for EncodeBase64
  struct Attrs {
    /// Bool whether padding is applied at the ends.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs Pad(bool x) {
      Attrs ret = *this;
      ret.pad_ = x;
      return ret;
    }

    bool pad_ = false;
  };
  EncodeBase64(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  EncodeBase64(const ::tensorflow::Scope& scope, ::tensorflow::Input input, const
             EncodeBase64::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs Pad(bool x) {
    return Attrs().Pad(x);
  }

  ::tensorflow::Output output;
};

/// Joins a string Tensor across the given dimensions.
///
/// Computes the string join across dimensions in the given string Tensor of shape
/// `[\\(d_0, d_1, ..., d_{n-1}\\)]`.  Returns a new Tensor created by joining the input
/// strings with the given separator (default: empty string).  Negative indices are
/// counted backwards from the end, with `-1` being equivalent to `n - 1`.  If
/// indices are not specified, joins across all dimensions beginning from `n - 1`
/// through `0`.
///
/// For example:
///
/// ```python
/// # tensor `a` is [["a", "b"], ["c", "d"]]
/// tf.reduce_join(a, 0) ==> ["ac", "bd"]
/// tf.reduce_join(a, 1) ==> ["ab", "cd"]
/// tf.reduce_join(a, -2) = tf.reduce_join(a, 0) ==> ["ac", "bd"]
/// tf.reduce_join(a, -1) = tf.reduce_join(a, 1) ==> ["ab", "cd"]
/// tf.reduce_join(a, 0, keep_dims=True) ==> [["ac", "bd"]]
/// tf.reduce_join(a, 1, keep_dims=True) ==> [["ab"], ["cd"]]
/// tf.reduce_join(a, 0, separator=".") ==> ["a.c", "b.d"]
/// tf.reduce_join(a, [0, 1]) ==> "acbd"
/// tf.reduce_join(a, [1, 0]) ==> "abcd"
/// tf.reduce_join(a, []) ==> [["a", "b"], ["c", "d"]]
/// tf.reduce_join(a) = tf.reduce_join(a, [1, 0]) ==> "abcd"
/// ```
///
/// Arguments:
/// * scope: A Scope object
/// * inputs: The input to be joined.  All reduced indices must have non-zero size.
/// * reduction_indices: The dimensions to reduce over.  Dimensions are reduced in the
/// order specified.  Omitting `reduction_indices` is equivalent to passing
/// `[n-1, n-2, ..., 0]`.  Negative indices from `-n` to `-1` are supported.
///
/// Optional attributes (see `Attrs`):
/// * keep_dims: If `True`, retain reduced dimensions with length `1`.
/// * separator: The separator to use when joining.
///
/// Returns:
/// * `Output`: Has shape equal to that of the input with reduced dimensions removed or
/// set to `1` depending on `keep_dims`.
class ReduceJoin {
 public:
  /// Optional attribute setters for ReduceJoin
  struct Attrs {
    /// If `True`, retain reduced dimensions with length `1`.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs KeepDims(bool x) {
      Attrs ret = *this;
      ret.keep_dims_ = x;
      return ret;
    }

    /// The separator to use when joining.
    ///
    /// Defaults to ""
    TF_MUST_USE_RESULT Attrs Separator(StringPiece x) {
      Attrs ret = *this;
      ret.separator_ = x;
      return ret;
    }

    bool keep_dims_ = false;
    StringPiece separator_ = "";
  };
  ReduceJoin(const ::tensorflow::Scope& scope, ::tensorflow::Input inputs,
           ::tensorflow::Input reduction_indices);
  ReduceJoin(const ::tensorflow::Scope& scope, ::tensorflow::Input inputs,
           ::tensorflow::Input reduction_indices, const ReduceJoin::Attrs&
           attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs KeepDims(bool x) {
    return Attrs().KeepDims(x);
  }
  static Attrs Separator(StringPiece x) {
    return Attrs().Separator(x);
  }

  ::tensorflow::Output output;
};

/// Check if the input matches the regex pattern.
///
/// The input is a string tensor of any shape. The pattern is a scalar
/// string tensor which is applied to every element of the input tensor.
/// The boolean values (True or False) of the output tensor indicate
/// if the input matches the regex pattern provided.
///
/// The pattern follows the re2 syntax (https://github.com/google/re2/wiki/Syntax)
///
/// Arguments:
/// * scope: A Scope object
/// * input: A string tensor of the text to be processed.
/// * pattern: A 1-D string tensor of the regular expression to match the input.
///
/// Returns:
/// * `Output`: A bool tensor with the same shape as `input`.
class RegexFullMatch {
 public:
  RegexFullMatch(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
               ::tensorflow::Input pattern);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Replaces the match of pattern in input with rewrite.
///
/// It follows the re2 syntax (https://github.com/google/re2/wiki/Syntax)
///
/// Arguments:
/// * scope: A Scope object
/// * input: The text to be processed.
/// * pattern: The regular expression to match the input.
/// * rewrite: The rewrite to be applied to the matched expresion.
///
/// Optional attributes (see `Attrs`):
/// * replace_global: If True, the replacement is global, otherwise the replacement
/// is done only on the first match.
///
/// Returns:
/// * `Output`: The text after applying pattern and rewrite.
class RegexReplace {
 public:
  /// Optional attribute setters for RegexReplace
  struct Attrs {
    /// If True, the replacement is global, otherwise the replacement
    /// is done only on the first match.
    ///
    /// Defaults to true
    TF_MUST_USE_RESULT Attrs ReplaceGlobal(bool x) {
      Attrs ret = *this;
      ret.replace_global_ = x;
      return ret;
    }

    bool replace_global_ = true;
  };
  RegexReplace(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
             ::tensorflow::Input pattern, ::tensorflow::Input rewrite);
  RegexReplace(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
             ::tensorflow::Input pattern, ::tensorflow::Input rewrite, const
             RegexReplace::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs ReplaceGlobal(bool x) {
    return Attrs().ReplaceGlobal(x);
  }

  ::tensorflow::Output output;
};

/// Joins the strings in the given list of string tensors into one tensor;
///
/// with the given separator (default is an empty separator).
///
/// Arguments:
/// * scope: A Scope object
/// * inputs: A list of string tensors.  The tensors must all have the same shape,
/// or be scalars.  Scalars may be mixed in; these will be broadcast to the shape
/// of non-scalar inputs.
///
/// Optional attributes (see `Attrs`):
/// * separator: string, an optional join separator.
///
/// Returns:
/// * `Output`: The output tensor.
class StringJoin {
 public:
  /// Optional attribute setters for StringJoin
  struct Attrs {
    /// string, an optional join separator.
    ///
    /// Defaults to ""
    TF_MUST_USE_RESULT Attrs Separator(StringPiece x) {
      Attrs ret = *this;
      ret.separator_ = x;
      return ret;
    }

    StringPiece separator_ = "";
  };
  StringJoin(const ::tensorflow::Scope& scope, ::tensorflow::InputList inputs);
  StringJoin(const ::tensorflow::Scope& scope, ::tensorflow::InputList inputs,
           const StringJoin::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs Separator(StringPiece x) {
    return Attrs().Separator(x);
  }

  ::tensorflow::Output output;
};

/// Split elements of `input` based on `delimiter` into a `SparseTensor`.
///
/// Let N be the size of source (typically N will be the batch size). Split each
/// element of `input` based on `delimiter` and return a `SparseTensor`
/// containing the splitted tokens. Empty tokens are ignored.
///
/// `delimiter` can be empty, or a string of split characters. If `delimiter` is an
///  empty string, each element of `input` is split into individual single-byte
///  character strings, including splitting of UTF-8 multibyte sequences. Otherwise
///  every character of `delimiter` is a potential split point.
///
/// For example:
///   N = 2, input[0] is 'hello world' and input[1] is 'a b c', then the output
///   will be
///
///   indices = [0, 0;
///              0, 1;
///              1, 0;
///              1, 1;
///              1, 2]
///   shape = [2, 3]
///   values = ['hello', 'world', 'a', 'b', 'c']
///
/// Arguments:
/// * scope: A Scope object
/// * input: 1-D. Strings to split.
/// * delimiter: 0-D. Delimiter characters (bytes), or empty string.
///
/// Optional attributes (see `Attrs`):
/// * skip_empty: A `bool`. If `True`, skip the empty strings from the result.
///
/// Returns:
/// * `Output` indices: A dense matrix of int64 representing the indices of the sparse tensor.
/// * `Output` values: A vector of strings corresponding to the splited values.
/// * `Output` shape: a length-2 vector of int64 representing the shape of the sparse
/// tensor, where the first value is N and the second value is the maximum number
/// of tokens in a single input entry.
class StringSplit {
 public:
  /// Optional attribute setters for StringSplit
  struct Attrs {
    /// A `bool`. If `True`, skip the empty strings from the result.
    ///
    /// Defaults to true
    TF_MUST_USE_RESULT Attrs SkipEmpty(bool x) {
      Attrs ret = *this;
      ret.skip_empty_ = x;
      return ret;
    }

    bool skip_empty_ = true;
  };
  StringSplit(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
            ::tensorflow::Input delimiter);
  StringSplit(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
            ::tensorflow::Input delimiter, const StringSplit::Attrs& attrs);

  static Attrs SkipEmpty(bool x) {
    return Attrs().SkipEmpty(x);
  }

  ::tensorflow::Output indices;
  ::tensorflow::Output values;
  ::tensorflow::Output shape;
};

/// Split elements of `source` based on `sep` into a `SparseTensor`.
///
/// Let N be the size of source (typically N will be the batch size). Split each
/// element of `source` based on `sep` and return a `SparseTensor`
/// containing the split tokens. Empty tokens are ignored.
///
/// For example, N = 2, source[0] is 'hello world' and source[1] is 'a b c',
/// then the output will be
/// ```
/// st.indices = [0, 0;
///               0, 1;
///               1, 0;
///               1, 1;
///               1, 2]
/// st.shape = [2, 3]
/// st.values = ['hello', 'world', 'a', 'b', 'c']
/// ```
///
/// If `sep` is given, consecutive delimiters are not grouped together and are
/// deemed to delimit empty strings. For example, source of `"1<>2<><>3"` and
/// sep of `"<>"` returns `["1", "2", "", "3"]`. If `sep` is None or an empty
/// string, consecutive whitespace are regarded as a single separator, and the
/// result will contain no empty strings at the startor end if the string has
/// leading or trailing whitespace.
///
/// Note that the above mentioned behavior matches python's str.split.
///
/// Arguments:
/// * scope: A Scope object
/// * input: `1-D` string `Tensor`, the strings to split.
/// * sep: `0-D` string `Tensor`, the delimiter character.
///
/// Optional attributes (see `Attrs`):
/// * maxsplit: An `int`. If `maxsplit > 0`, limit of the split of the result.
///
/// Returns:
/// * `Output` indices
/// * `Output` values
/// * `Output` shape
class StringSplitV2 {
 public:
  /// Optional attribute setters for StringSplitV2
  struct Attrs {
    /// An `int`. If `maxsplit > 0`, limit of the split of the result.
    ///
    /// Defaults to -1
    TF_MUST_USE_RESULT Attrs Maxsplit(int64 x) {
      Attrs ret = *this;
      ret.maxsplit_ = x;
      return ret;
    }

    int64 maxsplit_ = -1;
  };
  StringSplitV2(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
              ::tensorflow::Input sep);
  StringSplitV2(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
              ::tensorflow::Input sep, const StringSplitV2::Attrs& attrs);

  static Attrs Maxsplit(int64 x) {
    return Attrs().Maxsplit(x);
  }

  ::tensorflow::Output indices;
  ::tensorflow::Output values;
  ::tensorflow::Output shape;
};

/// Strip leading and trailing whitespaces from the Tensor.
///
/// Arguments:
/// * scope: A Scope object
/// * input: A string `Tensor` of any shape.
///
/// Returns:
/// * `Output`: A string `Tensor` of the same shape as the input.
class StringStrip {
 public:
  StringStrip(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Converts each string in the input Tensor to its hash mod by a number of buckets.
///
/// The hash function is deterministic on the content of the string within the
/// process.
///
/// Note that the hash function may change from time to time.
/// This functionality will be deprecated and it's recommended to use
/// `tf.string_to_hash_bucket_fast()` or `tf.string_to_hash_bucket_strong()`.
///
/// Arguments:
/// * scope: A Scope object
/// * num_buckets: The number of buckets.
///
/// Returns:
/// * `Output`: A Tensor of the same shape as the input `string_tensor`.
class StringToHashBucket {
 public:
  StringToHashBucket(const ::tensorflow::Scope& scope, ::tensorflow::Input
                   string_tensor, int64 num_buckets);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Converts each string in the input Tensor to its hash mod by a number of buckets.
///
/// The hash function is deterministic on the content of the string within the
/// process and will never change. However, it is not suitable for cryptography.
/// This function may be used when CPU time is scarce and inputs are trusted or
/// unimportant. There is a risk of adversaries constructing inputs that all hash
/// to the same bucket. To prevent this problem, use a strong hash function with
/// `tf.string_to_hash_bucket_strong`.
///
/// Arguments:
/// * scope: A Scope object
/// * input: The strings to assign a hash bucket.
/// * num_buckets: The number of buckets.
///
/// Returns:
/// * `Output`: A Tensor of the same shape as the input `string_tensor`.
class StringToHashBucketFast {
 public:
  StringToHashBucketFast(const ::tensorflow::Scope& scope, ::tensorflow::Input
                       input, int64 num_buckets);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Converts each string in the input Tensor to its hash mod by a number of buckets.
///
/// The hash function is deterministic on the content of the string within the
/// process. The hash function is a keyed hash function, where attribute `key`
/// defines the key of the hash function. `key` is an array of 2 elements.
///
/// A strong hash is important when inputs may be malicious, e.g. URLs with
/// additional components. Adversaries could try to make their inputs hash to the
/// same bucket for a denial-of-service attack or to skew the results. A strong
/// hash prevents this by making it difficult, if not infeasible, to compute inputs
/// that hash to the same bucket. This comes at a cost of roughly 4x higher compute
/// time than `tf.string_to_hash_bucket_fast`.
///
/// Arguments:
/// * scope: A Scope object
/// * input: The strings to assign a hash bucket.
/// * num_buckets: The number of buckets.
/// * key: The key for the keyed hash function passed as a list of two uint64
/// elements.
///
/// Returns:
/// * `Output`: A Tensor of the same shape as the input `string_tensor`.
class StringToHashBucketStrong {
 public:
  StringToHashBucketStrong(const ::tensorflow::Scope& scope, ::tensorflow::Input
                         input, int64 num_buckets, const gtl::ArraySlice<int>&
                         key);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Return substrings from `Tensor` of strings.
///
/// For each string in the input `Tensor`, creates a substring starting at index
/// `pos` with a total length of `len`.
///
/// If `len` defines a substring that would extend beyond the length of the input
/// string, then as many characters as possible are used.
///
/// If `pos` is negative or specifies a character index larger than any of the input
/// strings, then an `InvalidArgumentError` is thrown.
///
/// `pos` and `len` must have the same shape, otherwise a `ValueError` is thrown on
/// Op creation.
///
/// *NOTE*: `Substr` supports broadcasting up to two dimensions. More about
/// broadcasting
/// [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)
///
/// ---
///
/// Examples
///
/// Using scalar `pos` and `len`:
///
/// ```python
/// input = [b'Hello', b'World']
/// position = 1
/// length = 3
///
/// output = [b'ell', b'orl']
/// ```
///
/// Using `pos` and `len` with same shape as `input`:
///
/// ```python
/// input = [[b'ten', b'eleven', b'twelve'],
///          [b'thirteen', b'fourteen', b'fifteen'],
///          [b'sixteen', b'seventeen', b'eighteen']]
/// position = [[1, 2, 3],
///             [1, 2, 3],
///             [1, 2, 3]]
/// length =   [[2, 3, 4],
///             [4, 3, 2],
///             [5, 5, 5]]
///
/// output = [[b'en', b'eve', b'lve'],
///           [b'hirt', b'urt', b'te'],
///           [b'ixtee', b'vente', b'hteen']]
/// ```
///
/// Broadcasting `pos` and `len` onto `input`:
///
/// ```
/// input = [[b'ten', b'eleven', b'twelve'],
///          [b'thirteen', b'fourteen', b'fifteen'],
///          [b'sixteen', b'seventeen', b'eighteen'],
///          [b'nineteen', b'twenty', b'twentyone']]
/// position = [1, 2, 3]
/// length =   [1, 2, 3]
///
/// output = [[b'e', b'ev', b'lve'],
///           [b'h', b'ur', b'tee'],
///           [b'i', b've', b'hte'],
///           [b'i', b'en', b'nty']]
/// ```
///
/// Broadcasting `input` onto `pos` and `len`:
///
/// ```
/// input = b'thirteen'
/// position = [1, 5, 7]
/// length =   [3, 2, 1]
///
/// output = [b'hir', b'ee', b'n']
/// ```
///
/// Arguments:
/// * scope: A Scope object
/// * input: Tensor of strings
/// * pos: Scalar defining the position of first character in each substring
/// * len: Scalar defining the number of characters to include in each substring
///
/// Returns:
/// * `Output`: Tensor of substrings
class Substr {
 public:
  Substr(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
       ::tensorflow::Input pos, ::tensorflow::Input len);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// @}

}  // namespace ops
}  // namespace tensorflow

#endif  // TENSORFLOW_CC_OPS_STRING_OPS_H_
