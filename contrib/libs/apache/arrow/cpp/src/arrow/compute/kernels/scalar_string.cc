// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <algorithm>
#include <cctype>
#include <iterator>
#include <string>

#ifdef ARROW_WITH_UTF8PROC
#include <utf8proc.h>
#endif

#ifdef ARROW_WITH_RE2
#include <re2/re2.h>
#endif

#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/buffer_builder.h"

#include "arrow/builder.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/utf8.h"
#include "arrow/util/value_parsing.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace internal {

namespace {

#ifdef ARROW_WITH_RE2
util::string_view ToStringView(re2::StringPiece piece) {
  return {piece.data(), piece.length()};
}

re2::StringPiece ToStringPiece(util::string_view view) {
  return {view.data(), view.length()};
}

Status RegexStatus(const RE2& regex) {
  if (!regex.ok()) {
    return Status::Invalid("Invalid regular expression: ", regex.error());
  }
  return Status::OK();
}
#endif

// Code units in the range [a-z] can only be an encoding of an ascii
// character/codepoint, not the 2nd, 3rd or 4th code unit (byte) of an different
// codepoint. This guaranteed by non-overlap design of the unicode standard. (see
// section 2.5 of Unicode Standard Core Specification v13.0)

static inline uint8_t ascii_tolower(uint8_t utf8_code_unit) {
  return ((utf8_code_unit >= 'A') && (utf8_code_unit <= 'Z')) ? (utf8_code_unit + 32)
                                                              : utf8_code_unit;
}

static inline uint8_t ascii_toupper(uint8_t utf8_code_unit) {
  return ((utf8_code_unit >= 'a') && (utf8_code_unit <= 'z')) ? (utf8_code_unit - 32)
                                                              : utf8_code_unit;
}

template <typename T>
static inline bool IsAsciiCharacter(T character) {
  return character < 128;
}

struct BinaryLength {
  template <typename OutValue, typename Arg0Value = util::string_view>
  static OutValue Call(KernelContext*, Arg0Value val, Status*) {
    return static_cast<OutValue>(val.size());
  }
};

struct Utf8Length {
  template <typename OutValue, typename Arg0Value = util::string_view>
  static OutValue Call(KernelContext*, Arg0Value val, Status*) {
    auto str = reinterpret_cast<const uint8_t*>(val.data());
    auto strlen = val.size();
    return static_cast<OutValue>(util::UTF8Length(str, str + strlen));
  }
};

#ifdef ARROW_WITH_UTF8PROC

// Direct lookup tables for unicode properties
constexpr uint32_t kMaxCodepointLookup =
    0xffff;  // up to this codepoint is in a lookup table
std::vector<uint32_t> lut_upper_codepoint;
std::vector<uint32_t> lut_lower_codepoint;
std::vector<utf8proc_category_t> lut_category;
std::once_flag flag_case_luts;

void EnsureLookupTablesFilled() {
  std::call_once(flag_case_luts, []() {
    lut_upper_codepoint.reserve(kMaxCodepointLookup + 1);
    lut_lower_codepoint.reserve(kMaxCodepointLookup + 1);
    for (uint32_t i = 0; i <= kMaxCodepointLookup; i++) {
      lut_upper_codepoint.push_back(utf8proc_toupper(i));
      lut_lower_codepoint.push_back(utf8proc_tolower(i));
      lut_category.push_back(utf8proc_category(i));
    }
  });
}

#else

void EnsureLookupTablesFilled() {}

#endif  // ARROW_WITH_UTF8PROC

constexpr int64_t kTransformError = -1;

struct StringTransformBase {
  virtual Status PreExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return Status::OK();
  }

  // Return the maximum total size of the output in codeunits (i.e. bytes)
  // given input characteristics.
  virtual int64_t MaxCodeunits(int64_t ninputs, int64_t input_ncodeunits) {
    return input_ncodeunits;
  }

  virtual Status InvalidStatus() {
    return Status::Invalid("Invalid UTF8 sequence in input");
  }

  // Derived classes should also define this method:
  //   int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
  //                     uint8_t* output);
};

template <typename Type, typename StringTransform>
struct StringTransformExecBase {
  using offset_type = typename Type::offset_type;
  using ArrayType = typename TypeTraits<Type>::ArrayType;

  static Status Execute(KernelContext* ctx, StringTransform* transform,
                        const ExecBatch& batch, Datum* out) {
    if (batch[0].kind() == Datum::ARRAY) {
      return ExecArray(ctx, transform, batch[0].array(), out);
    }
    DCHECK_EQ(batch[0].kind(), Datum::SCALAR);
    return ExecScalar(ctx, transform, batch[0].scalar(), out);
  }

  static Status ExecArray(KernelContext* ctx, StringTransform* transform,
                          const std::shared_ptr<ArrayData>& data, Datum* out) {
    ArrayType input(data);
    ArrayData* output = out->mutable_array();

    const int64_t input_ncodeunits = input.total_values_length();
    const int64_t input_nstrings = input.length();

    const int64_t output_ncodeunits_max =
        transform->MaxCodeunits(input_nstrings, input_ncodeunits);
    if (output_ncodeunits_max > std::numeric_limits<offset_type>::max()) {
      return Status::CapacityError(
          "Result might not fit in a 32bit utf8 array, convert to large_utf8");
    }

    ARROW_ASSIGN_OR_RAISE(auto values_buffer, ctx->Allocate(output_ncodeunits_max));
    output->buffers[2] = values_buffer;

    // String offsets are preallocated
    offset_type* output_string_offsets = output->GetMutableValues<offset_type>(1);
    uint8_t* output_str = output->buffers[2]->mutable_data();
    offset_type output_ncodeunits = 0;

    output_string_offsets[0] = 0;
    for (int64_t i = 0; i < input_nstrings; i++) {
      if (!input.IsNull(i)) {
        offset_type input_string_ncodeunits;
        const uint8_t* input_string = input.GetValue(i, &input_string_ncodeunits);
        auto encoded_nbytes = static_cast<offset_type>(transform->Transform(
            input_string, input_string_ncodeunits, output_str + output_ncodeunits));
        if (encoded_nbytes < 0) {
          return transform->InvalidStatus();
        }
        output_ncodeunits += encoded_nbytes;
      }
      output_string_offsets[i + 1] = output_ncodeunits;
    }
    DCHECK_LE(output_ncodeunits, output_ncodeunits_max);

    // Trim the codepoint buffer, since we allocated too much
    return values_buffer->Resize(output_ncodeunits, /*shrink_to_fit=*/true);
  }

  static Status ExecScalar(KernelContext* ctx, StringTransform* transform,
                           const std::shared_ptr<Scalar>& scalar, Datum* out) {
    const auto& input = checked_cast<const BaseBinaryScalar&>(*scalar);
    if (!input.is_valid) {
      return Status::OK();
    }
    auto* result = checked_cast<BaseBinaryScalar*>(out->scalar().get());
    result->is_valid = true;
    const int64_t data_nbytes = static_cast<int64_t>(input.value->size());

    const int64_t output_ncodeunits_max = transform->MaxCodeunits(1, data_nbytes);
    if (output_ncodeunits_max > std::numeric_limits<offset_type>::max()) {
      return Status::CapacityError(
          "Result might not fit in a 32bit utf8 array, convert to large_utf8");
    }
    ARROW_ASSIGN_OR_RAISE(auto value_buffer, ctx->Allocate(output_ncodeunits_max));
    result->value = value_buffer;
    auto encoded_nbytes = static_cast<offset_type>(transform->Transform(
        input.value->data(), data_nbytes, value_buffer->mutable_data()));
    if (encoded_nbytes < 0) {
      return transform->InvalidStatus();
    }
    DCHECK_LE(encoded_nbytes, output_ncodeunits_max);
    return value_buffer->Resize(encoded_nbytes, /*shrink_to_fit=*/true);
  }
};

template <typename Type, typename StringTransform>
struct StringTransformExec : public StringTransformExecBase<Type, StringTransform> {
  using StringTransformExecBase<Type, StringTransform>::Execute;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    StringTransform transform;
    RETURN_NOT_OK(transform.PreExec(ctx, batch, out));
    return Execute(ctx, &transform, batch, out);
  }
};

template <typename Type, typename StringTransform>
struct StringTransformExecWithState
    : public StringTransformExecBase<Type, StringTransform> {
  using State = typename StringTransform::State;
  using StringTransformExecBase<Type, StringTransform>::Execute;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    StringTransform transform(State::Get(ctx));
    RETURN_NOT_OK(transform.PreExec(ctx, batch, out));
    return Execute(ctx, &transform, batch, out);
  }
};

#ifdef ARROW_WITH_UTF8PROC

template <typename CodepointTransform>
struct StringTransformCodepoint : public StringTransformBase {
  Status PreExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) override {
    EnsureLookupTablesFilled();
    return Status::OK();
  }

  int64_t MaxCodeunits(int64_t ninputs, int64_t input_ncodeunits) override {
    return CodepointTransform::MaxCodeunits(ninputs, input_ncodeunits);
  }

  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    uint8_t* output_start = output;
    if (ARROW_PREDICT_FALSE(
            !arrow::util::UTF8Transform(input, input + input_string_ncodeunits, &output,
                                        CodepointTransform::TransformCodepoint))) {
      return kTransformError;
    }
    return output - output_start;
  }
};

// struct CaseMappingMixin {
struct CaseMappingTransform {
  static int64_t MaxCodeunits(int64_t ninputs, int64_t input_ncodeunits) {
    // Section 5.18 of the Unicode spec claim that the number of codepoints for case
    // mapping can grow by a factor of 3. This means grow by a factor of 3 in bytes
    // However, since we don't support all casings (SpecialCasing.txt) the growth
    // in bytes iss actually only at max 3/2 (as covered by the unittest).
    // Note that rounding down the 3/2 is ok, since only codepoints encoded by
    // two code units (even) can grow to 3 code units.
    return static_cast<int64_t>(input_ncodeunits) * 3 / 2;
  }
};

struct UTF8UpperTransform : public CaseMappingTransform {
  static uint32_t TransformCodepoint(uint32_t codepoint) {
    return codepoint <= kMaxCodepointLookup ? lut_upper_codepoint[codepoint]
                                            : utf8proc_toupper(codepoint);
  }
};

template <typename Type>
using UTF8Upper = StringTransformExec<Type, StringTransformCodepoint<UTF8UpperTransform>>;

struct UTF8LowerTransform : public CaseMappingTransform {
  static uint32_t TransformCodepoint(uint32_t codepoint) {
    return codepoint <= kMaxCodepointLookup ? lut_lower_codepoint[codepoint]
                                            : utf8proc_tolower(codepoint);
  }
};

template <typename Type>
using UTF8Lower = StringTransformExec<Type, StringTransformCodepoint<UTF8LowerTransform>>;

#endif  // ARROW_WITH_UTF8PROC

struct AsciiReverseTransform : public StringTransformBase {
  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    uint8_t utf8_char_found = 0;
    for (int64_t i = 0; i < input_string_ncodeunits; i++) {
      // if a utf8 char is found, report to utf8_char_found
      utf8_char_found |= input[i] & 0x80;
      output[input_string_ncodeunits - i - 1] = input[i];
    }
    return utf8_char_found ? kTransformError : input_string_ncodeunits;
  }

  Status InvalidStatus() override {
    return Status::Invalid("Non-ASCII sequence in input");
  }
};

template <typename Type>
using AsciiReverse = StringTransformExec<Type, AsciiReverseTransform>;

struct Utf8ReverseTransform : public StringTransformBase {
  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    int64_t i = 0;
    while (i < input_string_ncodeunits) {
      int64_t char_end = std::min(i + util::ValidUtf8CodepointByteSize(input + i),
                                  input_string_ncodeunits);
      std::copy(input + i, input + char_end, output + input_string_ncodeunits - char_end);
      i = char_end;
    }
    return input_string_ncodeunits;
  }
};

template <typename Type>
using Utf8Reverse = StringTransformExec<Type, Utf8ReverseTransform>;

using TransformFunc = std::function<void(const uint8_t*, int64_t, uint8_t*)>;

// Transform a buffer of offsets to one which begins with 0 and has same
// value lengths.
template <typename T>
Status GetShiftedOffsets(KernelContext* ctx, const Buffer& input_buffer, int64_t offset,
                         int64_t length, std::shared_ptr<Buffer>* out) {
  ARROW_ASSIGN_OR_RAISE(*out, ctx->Allocate((length + 1) * sizeof(T)));
  const T* input_offsets = reinterpret_cast<const T*>(input_buffer.data()) + offset;
  T* out_offsets = reinterpret_cast<T*>((*out)->mutable_data());
  T first_offset = *input_offsets;
  for (int64_t i = 0; i < length; ++i) {
    *out_offsets++ = input_offsets[i] - first_offset;
  }
  *out_offsets = input_offsets[length] - first_offset;
  return Status::OK();
}

// Apply `transform` to input character data- this function cannot change the
// length
template <typename Type>
Status StringDataTransform(KernelContext* ctx, const ExecBatch& batch,
                           TransformFunc transform, Datum* out) {
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using offset_type = typename Type::offset_type;

  if (batch[0].kind() == Datum::ARRAY) {
    const ArrayData& input = *batch[0].array();
    ArrayType input_boxed(batch[0].array());

    ArrayData* out_arr = out->mutable_array();

    if (input.offset == 0) {
      // We can reuse offsets from input
      out_arr->buffers[1] = input.buffers[1];
    } else {
      DCHECK(input.buffers[1]);
      // We must allocate new space for the offsets and shift the existing offsets
      RETURN_NOT_OK(GetShiftedOffsets<offset_type>(ctx, *input.buffers[1], input.offset,
                                                   input.length, &out_arr->buffers[1]));
    }

    // Allocate space for output data
    int64_t data_nbytes = input_boxed.total_values_length();
    RETURN_NOT_OK(ctx->Allocate(data_nbytes).Value(&out_arr->buffers[2]));
    if (input.length > 0) {
      transform(input.buffers[2]->data() + input_boxed.value_offset(0), data_nbytes,
                out_arr->buffers[2]->mutable_data());
    }
  } else {
    const auto& input = checked_cast<const BaseBinaryScalar&>(*batch[0].scalar());
    auto result = checked_pointer_cast<BaseBinaryScalar>(MakeNullScalar(out->type()));
    if (input.is_valid) {
      result->is_valid = true;
      int64_t data_nbytes = input.value->size();
      RETURN_NOT_OK(ctx->Allocate(data_nbytes).Value(&result->value));
      transform(input.value->data(), data_nbytes, result->value->mutable_data());
    }
    out->value = result;
  }

  return Status::OK();
}

void TransformAsciiUpper(const uint8_t* input, int64_t length, uint8_t* output) {
  std::transform(input, input + length, output, ascii_toupper);
}

template <typename Type>
struct AsciiUpper {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return StringDataTransform<Type>(ctx, batch, TransformAsciiUpper, out);
  }
};

void TransformAsciiLower(const uint8_t* input, int64_t length, uint8_t* output) {
  std::transform(input, input + length, output, ascii_tolower);
}

template <typename Type>
struct AsciiLower {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return StringDataTransform<Type>(ctx, batch, TransformAsciiLower, out);
  }
};

// ----------------------------------------------------------------------
// exact pattern detection

using StrToBoolTransformFunc =
    std::function<void(const void*, const uint8_t*, int64_t, int64_t, uint8_t*)>;

// Apply `transform` to input character data- this function cannot change the
// length
template <typename Type>
void StringBoolTransform(KernelContext* ctx, const ExecBatch& batch,
                         StrToBoolTransformFunc transform, Datum* out) {
  using offset_type = typename Type::offset_type;

  if (batch[0].kind() == Datum::ARRAY) {
    const ArrayData& input = *batch[0].array();
    ArrayData* out_arr = out->mutable_array();
    if (input.length > 0) {
      transform(
          reinterpret_cast<const offset_type*>(input.buffers[1]->data()) + input.offset,
          input.buffers[2]->data(), input.length, out_arr->offset,
          out_arr->buffers[1]->mutable_data());
    }
  } else {
    const auto& input = checked_cast<const BaseBinaryScalar&>(*batch[0].scalar());
    if (input.is_valid) {
      uint8_t result_value = 0;
      std::array<offset_type, 2> offsets{0,
                                         static_cast<offset_type>(input.value->size())};
      transform(offsets.data(), input.value->data(), 1, /*output_offset=*/0,
                &result_value);
      out->value = std::make_shared<BooleanScalar>(result_value > 0);
    }
  }
}

using MatchSubstringState = OptionsWrapper<MatchSubstringOptions>;

// This is an implementation of the Knuth-Morris-Pratt algorithm
struct PlainSubstringMatcher {
  const MatchSubstringOptions& options_;
  std::vector<int64_t> prefix_table;

  static Result<std::unique_ptr<PlainSubstringMatcher>> Make(
      const MatchSubstringOptions& options) {
    // Should be handled by partial template specialization below
    DCHECK(!options.ignore_case);
    return ::arrow::internal::make_unique<PlainSubstringMatcher>(options);
  }

  explicit PlainSubstringMatcher(const MatchSubstringOptions& options)
      : options_(options) {
    // Phase 1: Build the prefix table
    const auto pattern_length = options_.pattern.size();
    prefix_table.resize(pattern_length + 1, /*value=*/0);
    int64_t prefix_length = -1;
    prefix_table[0] = -1;
    for (size_t pos = 0; pos < pattern_length; ++pos) {
      // The prefix cannot be expanded, reset.
      while (prefix_length >= 0 &&
             options_.pattern[pos] != options_.pattern[prefix_length]) {
        prefix_length = prefix_table[prefix_length];
      }
      prefix_length++;
      prefix_table[pos + 1] = prefix_length;
    }
  }

  int64_t Find(util::string_view current) const {
    // Phase 2: Find the prefix in the data
    const auto pattern_length = options_.pattern.size();
    int64_t pattern_pos = 0;
    int64_t pos = 0;
    if (pattern_length == 0) return 0;
    for (const auto c : current) {
      while ((pattern_pos >= 0) && (options_.pattern[pattern_pos] != c)) {
        pattern_pos = prefix_table[pattern_pos];
      }
      pattern_pos++;
      if (static_cast<size_t>(pattern_pos) == pattern_length) {
        return pos + 1 - pattern_length;
      }
      pos++;
    }
    return -1;
  }

  bool Match(util::string_view current) const { return Find(current) >= 0; }
};

struct PlainStartsWithMatcher {
  const MatchSubstringOptions& options_;

  explicit PlainStartsWithMatcher(const MatchSubstringOptions& options)
      : options_(options) {}

  static Result<std::unique_ptr<PlainStartsWithMatcher>> Make(
      const MatchSubstringOptions& options) {
    // Should be handled by partial template specialization below
    DCHECK(!options.ignore_case);
    return ::arrow::internal::make_unique<PlainStartsWithMatcher>(options);
  }

  bool Match(util::string_view current) const {
    // string_view::starts_with is C++20
    return current.substr(0, options_.pattern.size()) == options_.pattern;
  }
};

struct PlainEndsWithMatcher {
  const MatchSubstringOptions& options_;

  explicit PlainEndsWithMatcher(const MatchSubstringOptions& options)
      : options_(options) {}

  static Result<std::unique_ptr<PlainEndsWithMatcher>> Make(
      const MatchSubstringOptions& options) {
    // Should be handled by partial template specialization below
    DCHECK(!options.ignore_case);
    return ::arrow::internal::make_unique<PlainEndsWithMatcher>(options);
  }

  bool Match(util::string_view current) const {
    // string_view::ends_with is C++20
    return current.size() >= options_.pattern.size() &&
           current.substr(current.size() - options_.pattern.size(),
                          options_.pattern.size()) == options_.pattern;
  }
};

#ifdef ARROW_WITH_RE2
struct RegexSubstringMatcher {
  const MatchSubstringOptions& options_;
  const RE2 regex_match_;

  static Result<std::unique_ptr<RegexSubstringMatcher>> Make(
      const MatchSubstringOptions& options, bool literal = false) {
    auto matcher =
        ::arrow::internal::make_unique<RegexSubstringMatcher>(options, literal);
    RETURN_NOT_OK(RegexStatus(matcher->regex_match_));
    return std::move(matcher);
  }

  explicit RegexSubstringMatcher(const MatchSubstringOptions& options,
                                 bool literal = false)
      : options_(options),
        regex_match_(options_.pattern, MakeRE2Options(options, literal)) {}

  bool Match(util::string_view current) const {
    auto piece = re2::StringPiece(current.data(), current.length());
    return re2::RE2::PartialMatch(piece, regex_match_);
  }

  static RE2::RE2::Options MakeRE2Options(const MatchSubstringOptions& options,
                                          bool literal) {
    RE2::RE2::Options re2_options(RE2::Quiet);
    re2_options.set_case_sensitive(!options.ignore_case);
    re2_options.set_literal(literal);
    return re2_options;
  }
};
#endif

template <typename Type, typename Matcher>
struct MatchSubstringImpl {
  using offset_type = typename Type::offset_type;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out,
                     const Matcher* matcher) {
    StringBoolTransform<Type>(
        ctx, batch,
        [&matcher](const void* raw_offsets, const uint8_t* data, int64_t length,
                   int64_t output_offset, uint8_t* output) {
          const offset_type* offsets = reinterpret_cast<const offset_type*>(raw_offsets);
          FirstTimeBitmapWriter bitmap_writer(output, output_offset, length);
          for (int64_t i = 0; i < length; ++i) {
            const char* current_data = reinterpret_cast<const char*>(data + offsets[i]);
            int64_t current_length = offsets[i + 1] - offsets[i];
            if (matcher->Match(util::string_view(current_data, current_length))) {
              bitmap_writer.Set();
            }
            bitmap_writer.Next();
          }
          bitmap_writer.Finish();
        },
        out);
    return Status::OK();
  }
};

template <typename Type, typename Matcher>
struct MatchSubstring {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // TODO Cache matcher across invocations (for regex compilation)
    ARROW_ASSIGN_OR_RAISE(auto matcher, Matcher::Make(MatchSubstringState::Get(ctx)));
    return MatchSubstringImpl<Type, Matcher>::Exec(ctx, batch, out, matcher.get());
  }
};

template <typename Type>
struct MatchSubstring<Type, PlainSubstringMatcher> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    auto options = MatchSubstringState::Get(ctx);
    if (options.ignore_case) {
#ifdef ARROW_WITH_RE2
      ARROW_ASSIGN_OR_RAISE(auto matcher,
                            RegexSubstringMatcher::Make(options, /*literal=*/true));
      return MatchSubstringImpl<Type, RegexSubstringMatcher>::Exec(ctx, batch, out,
                                                                   matcher.get());
#else
      return Status::NotImplemented("ignore_case requires RE2");
#endif
    }
    ARROW_ASSIGN_OR_RAISE(auto matcher, PlainSubstringMatcher::Make(options));
    return MatchSubstringImpl<Type, PlainSubstringMatcher>::Exec(ctx, batch, out,
                                                                 matcher.get());
  }
};

template <typename Type>
struct MatchSubstring<Type, PlainStartsWithMatcher> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    auto options = MatchSubstringState::Get(ctx);
    if (options.ignore_case) {
#ifdef ARROW_WITH_RE2
      MatchSubstringOptions converted_options = options;
      converted_options.pattern = "^" + RE2::QuoteMeta(options.pattern);
      ARROW_ASSIGN_OR_RAISE(auto matcher, RegexSubstringMatcher::Make(converted_options));
      return MatchSubstringImpl<Type, RegexSubstringMatcher>::Exec(ctx, batch, out,
                                                                   matcher.get());
#else
      return Status::NotImplemented("ignore_case requires RE2");
#endif
    }
    ARROW_ASSIGN_OR_RAISE(auto matcher, PlainStartsWithMatcher::Make(options));
    return MatchSubstringImpl<Type, PlainStartsWithMatcher>::Exec(ctx, batch, out,
                                                                  matcher.get());
  }
};

template <typename Type>
struct MatchSubstring<Type, PlainEndsWithMatcher> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    auto options = MatchSubstringState::Get(ctx);
    if (options.ignore_case) {
#ifdef ARROW_WITH_RE2
      MatchSubstringOptions converted_options = options;
      converted_options.pattern = RE2::QuoteMeta(options.pattern) + "$";
      ARROW_ASSIGN_OR_RAISE(auto matcher, RegexSubstringMatcher::Make(converted_options));
      return MatchSubstringImpl<Type, RegexSubstringMatcher>::Exec(ctx, batch, out,
                                                                   matcher.get());
#else
      return Status::NotImplemented("ignore_case requires RE2");
#endif
    }
    ARROW_ASSIGN_OR_RAISE(auto matcher, PlainEndsWithMatcher::Make(options));
    return MatchSubstringImpl<Type, PlainEndsWithMatcher>::Exec(ctx, batch, out,
                                                                matcher.get());
  }
};

const FunctionDoc match_substring_doc(
    "Match strings against literal pattern",
    ("For each string in `strings`, emit true iff it contains a given pattern.\n"
     "Null inputs emit null.  The pattern must be given in MatchSubstringOptions. "
     "If ignore_case is set, only simple case folding is performed."),
    {"strings"}, "MatchSubstringOptions");

const FunctionDoc starts_with_doc(
    "Check if strings start with a literal pattern",
    ("For each string in `strings`, emit true iff it starts with a given pattern.\n"
     "Null inputs emit null.  The pattern must be given in MatchSubstringOptions. "
     "If ignore_case is set, only simple case folding is performed."),
    {"strings"}, "MatchSubstringOptions");

const FunctionDoc ends_with_doc(
    "Check if strings end with a literal pattern",
    ("For each string in `strings`, emit true iff it ends with a given pattern.\n"
     "Null inputs emit null.  The pattern must be given in MatchSubstringOptions. "
     "If ignore_case is set, only simple case folding is performed."),
    {"strings"}, "MatchSubstringOptions");

#ifdef ARROW_WITH_RE2
const FunctionDoc match_substring_regex_doc(
    "Match strings against regex pattern",
    ("For each string in `strings`, emit true iff it matches a given pattern at any "
     "position.\n"
     "Null inputs emit null.  The pattern must be given in MatchSubstringOptions. "
     "If ignore_case is set, only simple case folding is performed."),
    {"strings"}, "MatchSubstringOptions");

// SQL LIKE match

/// Convert a SQL-style LIKE pattern (using '%' and '_') into a regex pattern
std::string MakeLikeRegex(const MatchSubstringOptions& options) {
  // Allow . to match \n
  std::string like_pattern = "(?s:^";
  like_pattern.reserve(options.pattern.size() + 7);
  bool escaped = false;
  for (const char c : options.pattern) {
    if (!escaped && c == '%') {
      like_pattern.append(".*");
    } else if (!escaped && c == '_') {
      like_pattern.append(".");
    } else if (!escaped && c == '\\') {
      escaped = true;
    } else {
      switch (c) {
        case '.':
        case '?':
        case '+':
        case '*':
        case '^':
        case '$':
        case '\\':
        case '[':
        case '{':
        case '(':
        case ')':
        case '|': {
          like_pattern.push_back('\\');
          like_pattern.push_back(c);
          escaped = false;
          break;
        }
        default: {
          like_pattern.push_back(c);
          escaped = false;
          break;
        }
      }
    }
  }
  like_pattern.append("$)");
  return like_pattern;
}

// Evaluate a SQL-like LIKE pattern by translating it to a regexp or
// substring search as appropriate. See what Apache Impala does:
// https://github.com/apache/impala/blob/9c38568657d62b6f6d7b10aa1c721ba843374dd8/be/src/exprs/like-predicate.cc
template <typename StringType>
struct MatchLike {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // NOTE: avoid making those constants global to avoid compiling regexes at startup
    // A LIKE pattern matching this regex can be translated into a substring search.
    static const RE2 kLikePatternIsSubstringMatch(R"(%+([^%_]*[^\\%_])?%+)");
    // A LIKE pattern matching this regex can be translated into a prefix search.
    static const RE2 kLikePatternIsStartsWith(R"(([^%_]*[^\\%_])?%+)");
    // A LIKE pattern matching this regex can be translated into a suffix search.
    static const RE2 kLikePatternIsEndsWith(R"(%+([^%_]*))");

    auto original_options = MatchSubstringState::Get(ctx);
    auto original_state = ctx->state();

    Status status;
    std::string pattern;
    if (!original_options.ignore_case &&
        re2::RE2::FullMatch(original_options.pattern, kLikePatternIsSubstringMatch,
                            &pattern)) {
      MatchSubstringOptions converted_options{pattern, original_options.ignore_case};
      MatchSubstringState converted_state(converted_options);
      ctx->SetState(&converted_state);
      status = MatchSubstring<StringType, PlainSubstringMatcher>::Exec(ctx, batch, out);
    } else if (!original_options.ignore_case &&
               re2::RE2::FullMatch(original_options.pattern, kLikePatternIsStartsWith,
                                   &pattern)) {
      MatchSubstringOptions converted_options{pattern, original_options.ignore_case};
      MatchSubstringState converted_state(converted_options);
      ctx->SetState(&converted_state);
      status = MatchSubstring<StringType, PlainStartsWithMatcher>::Exec(ctx, batch, out);
    } else if (!original_options.ignore_case &&
               re2::RE2::FullMatch(original_options.pattern, kLikePatternIsEndsWith,
                                   &pattern)) {
      MatchSubstringOptions converted_options{pattern, original_options.ignore_case};
      MatchSubstringState converted_state(converted_options);
      ctx->SetState(&converted_state);
      status = MatchSubstring<StringType, PlainEndsWithMatcher>::Exec(ctx, batch, out);
    } else {
      MatchSubstringOptions converted_options{MakeLikeRegex(original_options),
                                              original_options.ignore_case};
      MatchSubstringState converted_state(converted_options);
      ctx->SetState(&converted_state);
      status = MatchSubstring<StringType, RegexSubstringMatcher>::Exec(ctx, batch, out);
    }
    ctx->SetState(original_state);
    return status;
  }
};

const FunctionDoc match_like_doc(
    "Match strings against SQL-style LIKE pattern",
    ("For each string in `strings`, emit true iff it fully matches a given pattern "
     "at any position. That is, '%' will match any number of characters, '_' will "
     "match exactly one character, and any other character matches itself. To "
     "match a literal '%', '_', or '\\', precede the character with a backslash.\n"
     "Null inputs emit null.  The pattern must be given in MatchSubstringOptions."),
    {"strings"}, "MatchSubstringOptions");

#endif

void AddMatchSubstring(FunctionRegistry* registry) {
  {
    auto func = std::make_shared<ScalarFunction>("match_substring", Arity::Unary(),
                                                 &match_substring_doc);
    auto exec_32 = MatchSubstring<StringType, PlainSubstringMatcher>::Exec;
    auto exec_64 = MatchSubstring<LargeStringType, PlainSubstringMatcher>::Exec;
    DCHECK_OK(func->AddKernel({utf8()}, boolean(), exec_32, MatchSubstringState::Init));
    DCHECK_OK(
        func->AddKernel({large_utf8()}, boolean(), exec_64, MatchSubstringState::Init));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<ScalarFunction>("starts_with", Arity::Unary(),
                                                 &match_substring_doc);
    auto exec_32 = MatchSubstring<StringType, PlainStartsWithMatcher>::Exec;
    auto exec_64 = MatchSubstring<LargeStringType, PlainStartsWithMatcher>::Exec;
    DCHECK_OK(func->AddKernel({utf8()}, boolean(), exec_32, MatchSubstringState::Init));
    DCHECK_OK(
        func->AddKernel({large_utf8()}, boolean(), exec_64, MatchSubstringState::Init));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<ScalarFunction>("ends_with", Arity::Unary(),
                                                 &match_substring_doc);
    auto exec_32 = MatchSubstring<StringType, PlainEndsWithMatcher>::Exec;
    auto exec_64 = MatchSubstring<LargeStringType, PlainEndsWithMatcher>::Exec;
    DCHECK_OK(func->AddKernel({utf8()}, boolean(), exec_32, MatchSubstringState::Init));
    DCHECK_OK(
        func->AddKernel({large_utf8()}, boolean(), exec_64, MatchSubstringState::Init));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
#ifdef ARROW_WITH_RE2
  {
    auto func = std::make_shared<ScalarFunction>("match_substring_regex", Arity::Unary(),
                                                 &match_substring_regex_doc);
    auto exec_32 = MatchSubstring<StringType, RegexSubstringMatcher>::Exec;
    auto exec_64 = MatchSubstring<LargeStringType, RegexSubstringMatcher>::Exec;
    DCHECK_OK(func->AddKernel({utf8()}, boolean(), exec_32, MatchSubstringState::Init));
    DCHECK_OK(
        func->AddKernel({large_utf8()}, boolean(), exec_64, MatchSubstringState::Init));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func =
        std::make_shared<ScalarFunction>("match_like", Arity::Unary(), &match_like_doc);
    auto exec_32 = MatchLike<StringType>::Exec;
    auto exec_64 = MatchLike<LargeStringType>::Exec;
    DCHECK_OK(func->AddKernel({utf8()}, boolean(), exec_32, MatchSubstringState::Init));
    DCHECK_OK(
        func->AddKernel({large_utf8()}, boolean(), exec_64, MatchSubstringState::Init));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
#endif
}

// Substring find - lfind/index/etc.

struct FindSubstring {
  const PlainSubstringMatcher matcher_;

  explicit FindSubstring(PlainSubstringMatcher matcher) : matcher_(std::move(matcher)) {}

  template <typename OutValue, typename... Ignored>
  OutValue Call(KernelContext*, util::string_view val, Status*) const {
    return static_cast<OutValue>(matcher_.Find(val));
  }
};

#ifdef ARROW_WITH_RE2
struct FindSubstringRegex {
  std::unique_ptr<RE2> regex_match_;

  explicit FindSubstringRegex(const MatchSubstringOptions& options,
                              bool literal = false) {
    std::string regex = "(";
    regex.reserve(options.pattern.length() + 2);
    regex += literal ? RE2::QuoteMeta(options.pattern) : options.pattern;
    regex += ")";
    regex_match_.reset(new RE2(std::move(regex), RegexSubstringMatcher::MakeRE2Options(
                                                     options, /*literal=*/false)));
  }

  template <typename OutValue, typename... Ignored>
  OutValue Call(KernelContext*, util::string_view val, Status*) const {
    re2::StringPiece piece(val.data(), val.length());
    re2::StringPiece match;
    if (re2::RE2::PartialMatch(piece, *regex_match_, &match)) {
      return static_cast<OutValue>(match.data() - piece.data());
    }
    return -1;
  }
};
#endif

template <typename InputType>
struct FindSubstringExec {
  using OffsetType = typename TypeTraits<InputType>::OffsetType;
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const MatchSubstringOptions& options = MatchSubstringState::Get(ctx);
    if (options.ignore_case) {
#ifdef ARROW_WITH_RE2
      applicator::ScalarUnaryNotNullStateful<OffsetType, InputType, FindSubstringRegex>
          kernel{FindSubstringRegex(options, /*literal=*/true)};
      return kernel.Exec(ctx, batch, out);
#endif
      return Status::NotImplemented("ignore_case requires RE2");
    }
    applicator::ScalarUnaryNotNullStateful<OffsetType, InputType, FindSubstring> kernel{
        FindSubstring(PlainSubstringMatcher(options))};
    return kernel.Exec(ctx, batch, out);
  }
};

const FunctionDoc find_substring_doc(
    "Find first occurrence of substring",
    ("For each string in `strings`, emit the index of the first occurrence of the given "
     "pattern, or -1 if not found.\n"
     "Null inputs emit null. The pattern must be given in MatchSubstringOptions."),
    {"strings"}, "MatchSubstringOptions");

#ifdef ARROW_WITH_RE2
template <typename InputType>
struct FindSubstringRegexExec {
  using OffsetType = typename TypeTraits<InputType>::OffsetType;
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const MatchSubstringOptions& options = MatchSubstringState::Get(ctx);
    applicator::ScalarUnaryNotNullStateful<OffsetType, InputType, FindSubstringRegex>
        kernel{FindSubstringRegex(options, /*literal=*/false)};
    return kernel.Exec(ctx, batch, out);
  }
};

const FunctionDoc find_substring_regex_doc(
    "Find location of first match of regex pattern",
    ("For each string in `strings`, emit the index of the first match of the given "
     "pattern, or -1 if not found.\n"
     "Null inputs emit null. The pattern must be given in MatchSubstringOptions."),
    {"strings"}, "MatchSubstringOptions");
#endif

void AddFindSubstring(FunctionRegistry* registry) {
  {
    auto func = std::make_shared<ScalarFunction>("find_substring", Arity::Unary(),
                                                 &find_substring_doc);
    for (const auto& ty : BaseBinaryTypes()) {
      auto offset_type = offset_bit_width(ty->id()) == 64 ? int64() : int32();
      DCHECK_OK(func->AddKernel({ty}, offset_type,
                                GenerateTypeAgnosticVarBinaryBase<FindSubstringExec>(ty),
                                MatchSubstringState::Init));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
#ifdef ARROW_WITH_RE2
  {
    auto func = std::make_shared<ScalarFunction>("find_substring_regex", Arity::Unary(),
                                                 &find_substring_regex_doc);
    for (const auto& ty : BaseBinaryTypes()) {
      auto offset_type = offset_bit_width(ty->id()) == 64 ? int64() : int32();
      DCHECK_OK(
          func->AddKernel({ty}, offset_type,
                          GenerateTypeAgnosticVarBinaryBase<FindSubstringRegexExec>(ty),
                          MatchSubstringState::Init));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
#endif
}

// Substring count

struct CountSubstring {
  const PlainSubstringMatcher matcher_;

  explicit CountSubstring(PlainSubstringMatcher matcher) : matcher_(std::move(matcher)) {}

  template <typename OutValue, typename... Ignored>
  OutValue Call(KernelContext*, util::string_view val, Status*) const {
    OutValue count = 0;
    uint64_t start = 0;
    const auto pattern_size = std::max<uint64_t>(1, matcher_.options_.pattern.size());
    while (start <= val.size()) {
      const int64_t index = matcher_.Find(val.substr(start));
      if (index >= 0) {
        count++;
        start += index + pattern_size;
      } else {
        break;
      }
    }
    return count;
  }
};

#ifdef ARROW_WITH_RE2
struct CountSubstringRegex {
  std::unique_ptr<RE2> regex_match_;

  explicit CountSubstringRegex(const MatchSubstringOptions& options, bool literal = false)
      : regex_match_(new RE2(options.pattern,
                             RegexSubstringMatcher::MakeRE2Options(options, literal))) {}

  static Result<CountSubstringRegex> Make(const MatchSubstringOptions& options,
                                          bool literal = false) {
    CountSubstringRegex counter(options, literal);
    RETURN_NOT_OK(RegexStatus(*counter.regex_match_));
    return std::move(counter);
  }

  template <typename OutValue, typename... Ignored>
  OutValue Call(KernelContext*, util::string_view val, Status*) const {
    OutValue count = 0;
    re2::StringPiece input(val.data(), val.size());
    auto last_size = input.size();
    while (re2::RE2::FindAndConsume(&input, *regex_match_)) {
      count++;
      if (last_size == input.size()) {
        // 0-length match
        if (input.size() > 0) {
          input.remove_prefix(1);
        } else {
          break;
        }
      }
      last_size = input.size();
    }
    return count;
  }
};

template <typename InputType>
struct CountSubstringRegexExec {
  using OffsetType = typename TypeTraits<InputType>::OffsetType;
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const MatchSubstringOptions& options = MatchSubstringState::Get(ctx);
    ARROW_ASSIGN_OR_RAISE(auto counter, CountSubstringRegex::Make(options));
    applicator::ScalarUnaryNotNullStateful<OffsetType, InputType, CountSubstringRegex>
        kernel{std::move(counter)};
    return kernel.Exec(ctx, batch, out);
  }
};
#endif

template <typename InputType>
struct CountSubstringExec {
  using OffsetType = typename TypeTraits<InputType>::OffsetType;
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const MatchSubstringOptions& options = MatchSubstringState::Get(ctx);
    if (options.ignore_case) {
#ifdef ARROW_WITH_RE2
      ARROW_ASSIGN_OR_RAISE(auto counter,
                            CountSubstringRegex::Make(options, /*literal=*/true));
      applicator::ScalarUnaryNotNullStateful<OffsetType, InputType, CountSubstringRegex>
          kernel{std::move(counter)};
      return kernel.Exec(ctx, batch, out);
#else
      return Status::NotImplemented("ignore_case requires RE2");
#endif
    }
    applicator::ScalarUnaryNotNullStateful<OffsetType, InputType, CountSubstring> kernel{
        CountSubstring(PlainSubstringMatcher(options))};
    return kernel.Exec(ctx, batch, out);
  }
};

const FunctionDoc count_substring_doc(
    "Count occurrences of substring",
    ("For each string in `strings`, emit the number of occurrences of the given "
     "pattern.\n"
     "Null inputs emit null. The pattern must be given in MatchSubstringOptions."),
    {"strings"}, "MatchSubstringOptions");

#ifdef ARROW_WITH_RE2
const FunctionDoc count_substring_regex_doc(
    "Count occurrences of substring",
    ("For each string in `strings`, emit the number of occurrences of the given "
     "regex pattern.\n"
     "Null inputs emit null. The pattern must be given in MatchSubstringOptions."),
    {"strings"}, "MatchSubstringOptions");
#endif

void AddCountSubstring(FunctionRegistry* registry) {
  {
    auto func = std::make_shared<ScalarFunction>("count_substring", Arity::Unary(),
                                                 &count_substring_doc);
    for (const auto& ty : BaseBinaryTypes()) {
      auto offset_type = offset_bit_width(ty->id()) == 64 ? int64() : int32();
      DCHECK_OK(func->AddKernel({ty}, offset_type,
                                GenerateTypeAgnosticVarBinaryBase<CountSubstringExec>(ty),
                                MatchSubstringState::Init));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
#ifdef ARROW_WITH_RE2
  {
    auto func = std::make_shared<ScalarFunction>("count_substring_regex", Arity::Unary(),
                                                 &count_substring_regex_doc);
    for (const auto& ty : BaseBinaryTypes()) {
      auto offset_type = offset_bit_width(ty->id()) == 64 ? int64() : int32();
      DCHECK_OK(
          func->AddKernel({ty}, offset_type,
                          GenerateTypeAgnosticVarBinaryBase<CountSubstringRegexExec>(ty),
                          MatchSubstringState::Init));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
#endif
}

// Slicing

struct SliceTransformBase : public StringTransformBase {
  using State = OptionsWrapper<SliceOptions>;

  const SliceOptions* options;

  Status PreExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) override {
    options = &State::Get(ctx);
    if (options->step == 0) {
      return Status::Invalid("Slice step cannot be zero");
    }
    return Status::OK();
  }
};

struct SliceCodeunitsTransform : SliceTransformBase {
  int64_t MaxCodeunits(int64_t ninputs, int64_t input_ncodeunits) override {
    const SliceOptions& opt = *this->options;
    if ((opt.start >= 0) != (opt.stop >= 0)) {
      // If start and stop don't have the same sign, we can't guess an upper bound
      // on the resulting slice lengths, so return a worst case estimate.
      return input_ncodeunits;
    }
    int64_t max_slice_codepoints = (opt.stop - opt.start + opt.step - 1) / opt.step;
    // The maximum UTF8 byte size of a codepoint is 4
    return std::min(input_ncodeunits,
                    4 * ninputs * std::max<int64_t>(0, max_slice_codepoints));
  }

  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    if (options->step >= 1) {
      return SliceForward(input, input_string_ncodeunits, output);
    }
    return SliceBackward(input, input_string_ncodeunits, output);
  }

#define RETURN_IF_UTF8_ERROR(expr)    \
  do {                                \
    if (ARROW_PREDICT_FALSE(!expr)) { \
      return kTransformError;         \
    }                                 \
  } while (0)

  int64_t SliceForward(const uint8_t* input, int64_t input_string_ncodeunits,
                       uint8_t* output) {
    // Slice in forward order (step > 0)
    const SliceOptions& opt = *this->options;
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* begin_sliced = begin;
    const uint8_t* end_sliced = end;

    // First, compute begin_sliced and end_sliced
    if (opt.start >= 0) {
      // start counting from the left
      RETURN_IF_UTF8_ERROR(
          arrow::util::UTF8AdvanceCodepoints(begin, end, &begin_sliced, opt.start));
      if (opt.stop > opt.start) {
        // continue counting from begin_sliced
        const int64_t length = opt.stop - opt.start;
        RETURN_IF_UTF8_ERROR(
            arrow::util::UTF8AdvanceCodepoints(begin_sliced, end, &end_sliced, length));
      } else if (opt.stop < 0) {
        // or from the end (but we will never need to < begin_sliced)
        RETURN_IF_UTF8_ERROR(arrow::util::UTF8AdvanceCodepointsReverse(
            begin_sliced, end, &end_sliced, -opt.stop));
      } else {
        // zero length slice
        return 0;
      }
    } else {
      // start counting from the right
      RETURN_IF_UTF8_ERROR(arrow::util::UTF8AdvanceCodepointsReverse(
          begin, end, &begin_sliced, -opt.start));
      if (opt.stop > 0) {
        // continue counting from the left, we cannot start from begin_sliced because we
        // don't know how many codepoints are between begin and begin_sliced
        RETURN_IF_UTF8_ERROR(
            arrow::util::UTF8AdvanceCodepoints(begin, end, &end_sliced, opt.stop));
        // and therefore we also needs this
        if (end_sliced <= begin_sliced) {
          // zero length slice
          return 0;
        }
      } else if ((opt.stop < 0) && (opt.stop > opt.start)) {
        // stop is negative, but larger than start, so we count again from the right
        // in some cases we can optimize this, depending on the shortest path (from end
        // or begin_sliced), but begin_sliced and opt.start can be 'out of sync',
        // for instance when start=-100, when the string length is only 10.
        RETURN_IF_UTF8_ERROR(arrow::util::UTF8AdvanceCodepointsReverse(
            begin_sliced, end, &end_sliced, -opt.stop));
      } else {
        // zero length slice
        return 0;
      }
    }

    // Second, copy computed slice to output
    DCHECK(begin_sliced <= end_sliced);
    if (opt.step == 1) {
      // fast case, where we simply can finish with a memcpy
      std::copy(begin_sliced, end_sliced, output);
      return end_sliced - begin_sliced;
    }
    uint8_t* dest = output;
    const uint8_t* i = begin_sliced;

    while (i < end_sliced) {
      uint32_t codepoint = 0;
      // write a single codepoint
      RETURN_IF_UTF8_ERROR(arrow::util::UTF8Decode(&i, &codepoint));
      dest = arrow::util::UTF8Encode(dest, codepoint);
      // and skip the remainder
      int64_t skips = opt.step - 1;
      while ((skips--) && (i < end_sliced)) {
        RETURN_IF_UTF8_ERROR(arrow::util::UTF8Decode(&i, &codepoint));
      }
    }
    return dest - output;
  }

  int64_t SliceBackward(const uint8_t* input, int64_t input_string_ncodeunits,
                        uint8_t* output) {
    // Slice in reverse order (step < 0)
    const SliceOptions& opt = *this->options;
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* begin_sliced = begin;
    const uint8_t* end_sliced = end;

    // Serious +1 -1 kung fu because begin_sliced and end_sliced act like
    // reverse iterators.
    if (opt.start >= 0) {
      // +1 because begin_sliced acts as as the end of a reverse iterator
      RETURN_IF_UTF8_ERROR(
          arrow::util::UTF8AdvanceCodepoints(begin, end, &begin_sliced, opt.start + 1));
    } else {
      // -1 because start=-1 means the last codeunit, which is 0 advances
      RETURN_IF_UTF8_ERROR(arrow::util::UTF8AdvanceCodepointsReverse(
          begin, end, &begin_sliced, -opt.start - 1));
    }
    // make it point at the last codeunit of the previous codeunit
    begin_sliced--;

    // similar to opt.start
    if (opt.stop >= 0) {
      RETURN_IF_UTF8_ERROR(
          arrow::util::UTF8AdvanceCodepoints(begin, end, &end_sliced, opt.stop + 1));
    } else {
      RETURN_IF_UTF8_ERROR(arrow::util::UTF8AdvanceCodepointsReverse(
          begin, end, &end_sliced, -opt.stop - 1));
    }
    end_sliced--;

    // Copy computed slice to output
    uint8_t* dest = output;
    const uint8_t* i = begin_sliced;
    while (i > end_sliced) {
      uint32_t codepoint = 0;
      // write a single codepoint
      RETURN_IF_UTF8_ERROR(arrow::util::UTF8DecodeReverse(&i, &codepoint));
      dest = arrow::util::UTF8Encode(dest, codepoint);
      // and skip the remainder
      int64_t skips = -opt.step - 1;
      while ((skips--) && (i > end_sliced)) {
        RETURN_IF_UTF8_ERROR(arrow::util::UTF8DecodeReverse(&i, &codepoint));
      }
    }
    return dest - output;
  }

#undef RETURN_IF_UTF8_ERROR
};

template <typename Type>
using SliceCodeunits = StringTransformExec<Type, SliceCodeunitsTransform>;

const FunctionDoc utf8_slice_codeunits_doc(
    "Slice string ",
    ("For each string in `strings`, slice into a substring defined by\n"
     "`start`, `stop`, `step`) as given by `SliceOptions` where `start` is inclusive\n"
     "and `stop` is exclusive and are measured in codeunits. If step is negative, the\n"
     "string will be advanced in reversed order. A `step` of zero is considered an\n"
     "error.\n"
     "Null inputs emit null."),
    {"strings"}, "SliceOptions");

void AddSlice(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("utf8_slice_codeunits", Arity::Unary(),
                                               &utf8_slice_codeunits_doc);
  using t32 = SliceCodeunits<StringType>;
  using t64 = SliceCodeunits<LargeStringType>;
  DCHECK_OK(
      func->AddKernel({utf8()}, utf8(), t32::Exec, SliceCodeunitsTransform::State::Init));
  DCHECK_OK(func->AddKernel({large_utf8()}, large_utf8(), t64::Exec,
                            SliceCodeunitsTransform::State::Init));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

// IsAlpha/Digit etc

#ifdef ARROW_WITH_UTF8PROC

static inline bool HasAnyUnicodeGeneralCategory(uint32_t codepoint, uint32_t mask) {
  utf8proc_category_t general_category = codepoint <= kMaxCodepointLookup
                                             ? lut_category[codepoint]
                                             : utf8proc_category(codepoint);
  uint32_t general_category_bit = 1 << general_category;
  // for e.g. undefined (but valid) codepoints, general_category == 0 ==
  // UTF8PROC_CATEGORY_CN
  return (general_category != UTF8PROC_CATEGORY_CN) &&
         ((general_category_bit & mask) != 0);
}

template <typename... Categories>
static inline bool HasAnyUnicodeGeneralCategory(uint32_t codepoint, uint32_t mask,
                                                utf8proc_category_t category,
                                                Categories... categories) {
  return HasAnyUnicodeGeneralCategory(codepoint, mask | (1 << category), categories...);
}

template <typename... Categories>
static inline bool HasAnyUnicodeGeneralCategory(uint32_t codepoint,
                                                utf8proc_category_t category,
                                                Categories... categories) {
  return HasAnyUnicodeGeneralCategory(codepoint, static_cast<uint32_t>(1u << category),
                                      categories...);
}

static inline bool IsCasedCharacterUnicode(uint32_t codepoint) {
  return HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_LU,
                                      UTF8PROC_CATEGORY_LL, UTF8PROC_CATEGORY_LT) ||
         ((static_cast<uint32_t>(utf8proc_toupper(codepoint)) != codepoint) ||
          (static_cast<uint32_t>(utf8proc_tolower(codepoint)) != codepoint));
}

static inline bool IsLowerCaseCharacterUnicode(uint32_t codepoint) {
  // although this trick seems to work for upper case, this is not enough for lower case
  // testing, see https://github.com/JuliaStrings/utf8proc/issues/195 . But currently the
  // best we can do
  return (HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_LL) ||
          ((static_cast<uint32_t>(utf8proc_toupper(codepoint)) != codepoint) &&
           (static_cast<uint32_t>(utf8proc_tolower(codepoint)) == codepoint))) &&
         !HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_LT);
}

static inline bool IsUpperCaseCharacterUnicode(uint32_t codepoint) {
  // this seems to be a good workaround for utf8proc not having case information
  // https://github.com/JuliaStrings/utf8proc/issues/195
  return (HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_LU) ||
          ((static_cast<uint32_t>(utf8proc_toupper(codepoint)) == codepoint) &&
           (static_cast<uint32_t>(utf8proc_tolower(codepoint)) != codepoint))) &&
         !HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_LT);
}

static inline bool IsAlphaNumericCharacterUnicode(uint32_t codepoint) {
  return HasAnyUnicodeGeneralCategory(
      codepoint, UTF8PROC_CATEGORY_LU, UTF8PROC_CATEGORY_LL, UTF8PROC_CATEGORY_LT,
      UTF8PROC_CATEGORY_LM, UTF8PROC_CATEGORY_LO, UTF8PROC_CATEGORY_ND,
      UTF8PROC_CATEGORY_NL, UTF8PROC_CATEGORY_NO);
}

static inline bool IsAlphaCharacterUnicode(uint32_t codepoint) {
  return HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_LU,
                                      UTF8PROC_CATEGORY_LL, UTF8PROC_CATEGORY_LT,
                                      UTF8PROC_CATEGORY_LM, UTF8PROC_CATEGORY_LO);
}

static inline bool IsDecimalCharacterUnicode(uint32_t codepoint) {
  return HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_ND);
}

static inline bool IsDigitCharacterUnicode(uint32_t codepoint) {
  // Python defines this as Numeric_Type=Digit or Numeric_Type=Decimal.
  // utf8proc has no support for this, this is the best we can do:
  return HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_ND);
}

static inline bool IsNumericCharacterUnicode(uint32_t codepoint) {
  // Formally this is not correct, but utf8proc does not allow us to query for Numerical
  // properties, e.g. Numeric_Value and Numeric_Type
  // Python defines Numeric as Numeric_Type=Digit, Numeric_Type=Decimal or
  // Numeric_Type=Numeric.
  return HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_ND,
                                      UTF8PROC_CATEGORY_NL, UTF8PROC_CATEGORY_NO);
}

static inline bool IsSpaceCharacterUnicode(uint32_t codepoint) {
  auto property = utf8proc_get_property(codepoint);
  return HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_ZS) ||
         property->bidi_class == UTF8PROC_BIDI_CLASS_WS ||
         property->bidi_class == UTF8PROC_BIDI_CLASS_B ||
         property->bidi_class == UTF8PROC_BIDI_CLASS_S;
}

static inline bool IsPrintableCharacterUnicode(uint32_t codepoint) {
  uint32_t general_category = utf8proc_category(codepoint);
  return (general_category != UTF8PROC_CATEGORY_CN) &&
         !HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_CC,
                                       UTF8PROC_CATEGORY_CF, UTF8PROC_CATEGORY_CS,
                                       UTF8PROC_CATEGORY_CO, UTF8PROC_CATEGORY_ZS,
                                       UTF8PROC_CATEGORY_ZL, UTF8PROC_CATEGORY_ZP);
}

#endif

static inline bool IsLowerCaseCharacterAscii(uint8_t ascii_character) {
  return (ascii_character >= 'a') && (ascii_character <= 'z');
}

static inline bool IsUpperCaseCharacterAscii(uint8_t ascii_character) {
  return (ascii_character >= 'A') && (ascii_character <= 'Z');
}

static inline bool IsCasedCharacterAscii(uint8_t ascii_character) {
  return IsLowerCaseCharacterAscii(ascii_character) ||
         IsUpperCaseCharacterAscii(ascii_character);
}

static inline bool IsAlphaCharacterAscii(uint8_t ascii_character) {
  return IsCasedCharacterAscii(ascii_character);  // same
}

static inline bool IsAlphaNumericCharacterAscii(uint8_t ascii_character) {
  return ((ascii_character >= '0') && (ascii_character <= '9')) ||
         ((ascii_character >= 'a') && (ascii_character <= 'z')) ||
         ((ascii_character >= 'A') && (ascii_character <= 'Z'));
}

static inline bool IsDecimalCharacterAscii(uint8_t ascii_character) {
  return ((ascii_character >= '0') && (ascii_character <= '9'));
}

static inline bool IsSpaceCharacterAscii(uint8_t ascii_character) {
  return ((ascii_character >= 0x09) && (ascii_character <= 0x0D)) ||
         (ascii_character == ' ');
}

static inline bool IsPrintableCharacterAscii(uint8_t ascii_character) {
  return ((ascii_character >= ' ') && (ascii_character <= '~'));
}

template <typename Derived, bool allow_empty = false>
struct CharacterPredicateUnicode {
  static bool Call(KernelContext*, const uint8_t* input, size_t input_string_ncodeunits,
                   Status* st) {
    if (allow_empty && input_string_ncodeunits == 0) {
      return true;
    }
    bool all;
    bool any = false;
    if (!ARROW_PREDICT_TRUE(arrow::util::UTF8AllOf(
            input, input + input_string_ncodeunits, &all, [&any](uint32_t codepoint) {
              any |= Derived::PredicateCharacterAny(codepoint);
              return Derived::PredicateCharacterAll(codepoint);
            }))) {
      *st = Status::Invalid("Invalid UTF8 sequence in input");
      return false;
    }
    return all & any;
  }

  static inline bool PredicateCharacterAny(uint32_t) {
    return true;  // default condition make sure there is at least 1 charachter
  }
};

template <typename Derived, bool allow_empty = false>
struct CharacterPredicateAscii {
  static bool Call(KernelContext*, const uint8_t* input, size_t input_string_ncodeunits,
                   Status*) {
    if (allow_empty && input_string_ncodeunits == 0) {
      return true;
    }
    bool any = false;
    // MB: A simple for loops seems 8% faster on gcc 9.3, running the IsAlphaNumericAscii
    // benchmark. I don't consider that worth it.
    bool all = std::all_of(input, input + input_string_ncodeunits,
                           [&any](uint8_t ascii_character) {
                             any |= Derived::PredicateCharacterAny(ascii_character);
                             return Derived::PredicateCharacterAll(ascii_character);
                           });
    return all & any;
  }

  static inline bool PredicateCharacterAny(uint8_t) {
    return true;  // default condition make sure there is at least 1 charachter
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsAlphaNumericUnicode : CharacterPredicateUnicode<IsAlphaNumericUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return IsAlphaNumericCharacterUnicode(codepoint);
  }
};
#endif

struct IsAlphaNumericAscii : CharacterPredicateAscii<IsAlphaNumericAscii> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    return IsAlphaNumericCharacterAscii(ascii_character);
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsAlphaUnicode : CharacterPredicateUnicode<IsAlphaUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return IsAlphaCharacterUnicode(codepoint);
  }
};
#endif

struct IsAlphaAscii : CharacterPredicateAscii<IsAlphaAscii> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    return IsAlphaCharacterAscii(ascii_character);
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsDecimalUnicode : CharacterPredicateUnicode<IsDecimalUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return IsDecimalCharacterUnicode(codepoint);
  }
};
#endif

struct IsDecimalAscii : CharacterPredicateAscii<IsDecimalAscii> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    return IsDecimalCharacterAscii(ascii_character);
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsDigitUnicode : CharacterPredicateUnicode<IsDigitUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return IsDigitCharacterUnicode(codepoint);
  }
};

struct IsNumericUnicode : CharacterPredicateUnicode<IsNumericUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return IsNumericCharacterUnicode(codepoint);
  }
};
#endif

struct IsAscii {
  static bool Call(KernelContext*, const uint8_t* input,
                   size_t input_string_nascii_characters, Status*) {
    return std::all_of(input, input + input_string_nascii_characters,
                       IsAsciiCharacter<uint8_t>);
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsLowerUnicode : CharacterPredicateUnicode<IsLowerUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    // Only for cased character it needs to be lower case
    return !IsCasedCharacterUnicode(codepoint) || IsLowerCaseCharacterUnicode(codepoint);
  }
  static inline bool PredicateCharacterAny(uint32_t codepoint) {
    return IsCasedCharacterUnicode(codepoint);  // at least 1 cased character
  }
};
#endif

struct IsLowerAscii : CharacterPredicateAscii<IsLowerAscii> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    // Only for cased character it needs to be lower case
    return !IsCasedCharacterAscii(ascii_character) ||
           IsLowerCaseCharacterAscii(ascii_character);
  }
  static inline bool PredicateCharacterAny(uint8_t ascii_character) {
    return IsCasedCharacterAscii(ascii_character);  // at least 1 cased character
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsPrintableUnicode
    : CharacterPredicateUnicode<IsPrintableUnicode, /*allow_empty=*/true> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return codepoint == ' ' || IsPrintableCharacterUnicode(codepoint);
  }
};
#endif

struct IsPrintableAscii
    : CharacterPredicateAscii<IsPrintableAscii, /*allow_empty=*/true> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    return IsPrintableCharacterAscii(ascii_character);
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsSpaceUnicode : CharacterPredicateUnicode<IsSpaceUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return IsSpaceCharacterUnicode(codepoint);
  }
};
#endif

struct IsSpaceAscii : CharacterPredicateAscii<IsSpaceAscii> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    return IsSpaceCharacterAscii(ascii_character);
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsTitleUnicode {
  static bool Call(KernelContext*, const uint8_t* input, size_t input_string_ncodeunits,
                   Status* st) {
    // rules:
    // * 1: lower case follows cased
    // * 2: upper case follows uncased
    // * 3: at least 1 cased character (which logically should be upper/title)
    bool rules_1_and_2;
    bool previous_cased = false;  // in LL, LU or LT
    bool rule_3 = false;
    bool status =
        arrow::util::UTF8AllOf(input, input + input_string_ncodeunits, &rules_1_and_2,
                               [&previous_cased, &rule_3](uint32_t codepoint) {
                                 if (IsLowerCaseCharacterUnicode(codepoint)) {
                                   if (!previous_cased) return false;  // rule 1 broken
                                   previous_cased = true;
                                 } else if (IsCasedCharacterUnicode(codepoint)) {
                                   if (previous_cased) return false;  // rule 2 broken
                                   // next should be a lower case or uncased
                                   previous_cased = true;
                                   rule_3 = true;  // rule 3 obeyed
                                 } else {
                                   // a non-cased char, like _ or 1
                                   // next should be upper case or more uncased
                                   previous_cased = false;
                                 }
                                 return true;
                               });
    if (!ARROW_PREDICT_TRUE(status)) {
      *st = Status::Invalid("Invalid UTF8 sequence in input");
      return false;
    }
    return rules_1_and_2 & rule_3;
  }
};
#endif

struct IsTitleAscii {
  static bool Call(KernelContext*, const uint8_t* input, size_t input_string_ncodeunits,
                   Status*) {
    // rules:
    // * 1: lower case follows cased
    // * 2: upper case follows uncased
    // * 3: at least 1 cased character (which logically should be upper/title)
    bool rules_1_and_2 = true;
    bool previous_cased = false;  // in LL, LU or LT
    bool rule_3 = false;
    // we cannot rely on std::all_of because we need guaranteed order
    for (const uint8_t* c = input; c < input + input_string_ncodeunits; ++c) {
      if (IsLowerCaseCharacterAscii(*c)) {
        if (!previous_cased) {
          // rule 1 broken
          rules_1_and_2 = false;
          break;
        }
        previous_cased = true;
      } else if (IsCasedCharacterAscii(*c)) {
        if (previous_cased) {
          // rule 2 broken
          rules_1_and_2 = false;
          break;
        }
        // next should be a lower case or uncased
        previous_cased = true;
        rule_3 = true;  // rule 3 obeyed
      } else {
        // a non-cased char, like _ or 1
        // next should be upper case or more uncased
        previous_cased = false;
      }
    }
    return rules_1_and_2 & rule_3;
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsUpperUnicode : CharacterPredicateUnicode<IsUpperUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    // Only for cased character it needs to be lower case
    return !IsCasedCharacterUnicode(codepoint) || IsUpperCaseCharacterUnicode(codepoint);
  }
  static inline bool PredicateCharacterAny(uint32_t codepoint) {
    return IsCasedCharacterUnicode(codepoint);  // at least 1 cased character
  }
};
#endif

struct IsUpperAscii : CharacterPredicateAscii<IsUpperAscii> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    // Only for cased character it needs to be lower case
    return !IsCasedCharacterAscii(ascii_character) ||
           IsUpperCaseCharacterAscii(ascii_character);
  }
  static inline bool PredicateCharacterAny(uint8_t ascii_character) {
    return IsCasedCharacterAscii(ascii_character);  // at least 1 cased character
  }
};

// splitting

template <typename Options>
struct SplitFinderBase {
  virtual Status PreExec(const Options& options) { return Status::OK(); }

  // Derived classes should also define these methods:
  //   static bool Find(const uint8_t* begin, const uint8_t* end,
  //                    const uint8_t** separator_begin,
  //                    const uint8_t** separator_end,
  //                    const SplitPatternOptions& options);
  //
  //   static bool FindReverse(const uint8_t* begin, const uint8_t* end,
  //                           const uint8_t** separator_begin,
  //                           const uint8_t** separator_end,
  //                           const SplitPatternOptions& options);
};

template <typename Type, typename ListType, typename SplitFinder,
          typename Options = typename SplitFinder::Options>
struct SplitExec {
  using string_offset_type = typename Type::offset_type;
  using list_offset_type = typename ListType::offset_type;
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using ArrayListType = typename TypeTraits<ListType>::ArrayType;
  using ListScalarType = typename TypeTraits<ListType>::ScalarType;
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  using ListOffsetsBuilderType = TypedBufferBuilder<list_offset_type>;
  using State = OptionsWrapper<Options>;

  // Keep the temporary storage accross individual values, to minimize reallocations
  std::vector<util::string_view> parts;
  Options options;

  explicit SplitExec(const Options& options) : options(options) {}

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return SplitExec{State::Get(ctx)}.Execute(ctx, batch, out);
  }

  Status Execute(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    SplitFinder finder;
    RETURN_NOT_OK(finder.PreExec(options));
    if (batch[0].kind() == Datum::ARRAY) {
      return Execute(ctx, &finder, batch[0].array(), out);
    }
    DCHECK_EQ(batch[0].kind(), Datum::SCALAR);
    return Execute(ctx, &finder, batch[0].scalar(), out);
  }

  Status Execute(KernelContext* ctx, SplitFinder* finder,
                 const std::shared_ptr<ArrayData>& data, Datum* out) {
    const ArrayType input(data);

    BuilderType builder(input.type(), ctx->memory_pool());
    // A slight overestimate of the data needed
    RETURN_NOT_OK(builder.ReserveData(input.total_values_length()));
    // The minimum amount of strings needed
    RETURN_NOT_OK(builder.Resize(input.length() - input.null_count()));

    ArrayData* output_list = out->mutable_array();
    // List offsets were preallocated
    auto* list_offsets = output_list->GetMutableValues<list_offset_type>(1);
    DCHECK_NE(list_offsets, nullptr);
    // Initial value
    *list_offsets++ = 0;
    for (int64_t i = 0; i < input.length(); ++i) {
      if (!input.IsNull(i)) {
        RETURN_NOT_OK(SplitString(input.GetView(i), finder, &builder));
        if (ARROW_PREDICT_FALSE(builder.length() >
                                std::numeric_limits<list_offset_type>::max())) {
          return Status::CapacityError("List offset does not fit into 32 bit");
        }
      }
      *list_offsets++ = static_cast<list_offset_type>(builder.length());
    }
    // Assign string array to list child data
    std::shared_ptr<Array> string_array;
    RETURN_NOT_OK(builder.Finish(&string_array));
    output_list->child_data.push_back(string_array->data());
    return Status::OK();
  }

  Status Execute(KernelContext* ctx, SplitFinder* finder,
                 const std::shared_ptr<Scalar>& scalar, Datum* out) {
    const auto& input = checked_cast<const ScalarType&>(*scalar);
    auto result = checked_cast<ListScalarType*>(out->scalar().get());
    if (input.is_valid) {
      result->is_valid = true;
      BuilderType builder(input.type, ctx->memory_pool());
      util::string_view s(*input.value);
      RETURN_NOT_OK(SplitString(s, finder, &builder));
      RETURN_NOT_OK(builder.Finish(&result->value));
    }
    return Status::OK();
  }

  Status SplitString(const util::string_view& s, SplitFinder* finder,
                     BuilderType* builder) {
    const uint8_t* begin = reinterpret_cast<const uint8_t*>(s.data());
    const uint8_t* end = begin + s.length();

    int64_t max_splits = options.max_splits;
    // if there is no max splits, reversing does not make sense (and is probably less
    // efficient), but is useful for testing
    if (options.reverse) {
      // note that i points 1 further than the 'current'
      const uint8_t* i = end;
      // we will record the parts in reverse order
      parts.clear();
      if (max_splits > -1) {
        parts.reserve(max_splits + 1);
      }
      while (max_splits != 0) {
        const uint8_t *separator_begin, *separator_end;
        // find with whatever algo the part we will 'cut out'
        if (finder->FindReverse(begin, i, &separator_begin, &separator_end, options)) {
          parts.emplace_back(reinterpret_cast<const char*>(separator_end),
                             i - separator_end);
          i = separator_begin;
          max_splits--;
        } else {
          // if we cannot find a separator, we're done
          break;
        }
      }
      parts.emplace_back(reinterpret_cast<const char*>(begin), i - begin);
      // now we do the copying
      for (auto it = parts.rbegin(); it != parts.rend(); ++it) {
        RETURN_NOT_OK(builder->Append(*it));
      }
    } else {
      const uint8_t* i = begin;
      while (max_splits != 0) {
        const uint8_t *separator_begin, *separator_end;
        // find with whatever algo the part we will 'cut out'
        if (finder->Find(i, end, &separator_begin, &separator_end, options)) {
          // the part till the beginning of the 'cut'
          RETURN_NOT_OK(
              builder->Append(i, static_cast<string_offset_type>(separator_begin - i)));
          i = separator_end;
          max_splits--;
        } else {
          // if we cannot find a separator, we're done
          break;
        }
      }
      // trailing part
      RETURN_NOT_OK(builder->Append(i, static_cast<string_offset_type>(end - i)));
    }
    return Status::OK();
  }
};

struct SplitPatternFinder : public SplitFinderBase<SplitPatternOptions> {
  using Options = SplitPatternOptions;

  Status PreExec(const SplitPatternOptions& options) override {
    if (options.pattern.length() == 0) {
      return Status::Invalid("Empty separator");
    }
    return Status::OK();
  }

  static bool Find(const uint8_t* begin, const uint8_t* end,
                   const uint8_t** separator_begin, const uint8_t** separator_end,
                   const SplitPatternOptions& options) {
    const uint8_t* pattern = reinterpret_cast<const uint8_t*>(options.pattern.c_str());
    const int64_t pattern_length = options.pattern.length();
    const uint8_t* i = begin;
    // this is O(n*m) complexity, we could use the Knuth-Morris-Pratt algorithm used in
    // the match kernel
    while ((i + pattern_length <= end)) {
      i = std::search(i, end, pattern, pattern + pattern_length);
      if (i != end) {
        *separator_begin = i;
        *separator_end = i + pattern_length;
        return true;
      }
    }
    return false;
  }

  static bool FindReverse(const uint8_t* begin, const uint8_t* end,
                          const uint8_t** separator_begin, const uint8_t** separator_end,
                          const SplitPatternOptions& options) {
    const uint8_t* pattern = reinterpret_cast<const uint8_t*>(options.pattern.c_str());
    const int64_t pattern_length = options.pattern.length();
    // this is O(n*m) complexity, we could use the Knuth-Morris-Pratt algorithm used in
    // the match kernel
    std::reverse_iterator<const uint8_t*> ri(end);
    std::reverse_iterator<const uint8_t*> rend(begin);
    std::reverse_iterator<const uint8_t*> pattern_rbegin(pattern + pattern_length);
    std::reverse_iterator<const uint8_t*> pattern_rend(pattern);
    while (begin <= ri.base() - pattern_length) {
      ri = std::search(ri, rend, pattern_rbegin, pattern_rend);
      if (ri != rend) {
        *separator_begin = ri.base() - pattern_length;
        *separator_end = ri.base();
        return true;
      }
    }
    return false;
  }
};

template <typename Type, typename ListType>
using SplitPatternExec = SplitExec<Type, ListType, SplitPatternFinder>;

const FunctionDoc split_pattern_doc(
    "Split string according to separator",
    ("Split each string according to the exact `pattern` defined in\n"
     "SplitPatternOptions.  The output for each string input is a list\n"
     "of strings.\n"
     "\n"
     "The maximum number of splits and direction of splitting\n"
     "(forward, reverse) can optionally be defined in SplitPatternOptions."),
    {"strings"}, "SplitPatternOptions");

const FunctionDoc ascii_split_whitespace_doc(
    "Split string according to any ASCII whitespace",
    ("Split each string according any non-zero length sequence of ASCII\n"
     "whitespace characters.  The output for each string input is a list\n"
     "of strings.\n"
     "\n"
     "The maximum number of splits and direction of splitting\n"
     "(forward, reverse) can optionally be defined in SplitOptions."),
    {"strings"}, "SplitOptions");

const FunctionDoc utf8_split_whitespace_doc(
    "Split string according to any Unicode whitespace",
    ("Split each string according any non-zero length sequence of Unicode\n"
     "whitespace characters.  The output for each string input is a list\n"
     "of strings.\n"
     "\n"
     "The maximum number of splits and direction of splitting\n"
     "(forward, reverse) can optionally be defined in SplitOptions."),
    {"strings"}, "SplitOptions");

void AddSplitPattern(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("split_pattern", Arity::Unary(),
                                               &split_pattern_doc);
  using t32 = SplitPatternExec<StringType, ListType>;
  using t64 = SplitPatternExec<LargeStringType, ListType>;
  DCHECK_OK(func->AddKernel({utf8()}, {list(utf8())}, t32::Exec, t32::State::Init));
  DCHECK_OK(
      func->AddKernel({large_utf8()}, {list(large_utf8())}, t64::Exec, t64::State::Init));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

struct SplitWhitespaceAsciiFinder : public SplitFinderBase<SplitOptions> {
  using Options = SplitOptions;

  static bool Find(const uint8_t* begin, const uint8_t* end,
                   const uint8_t** separator_begin, const uint8_t** separator_end,
                   const SplitOptions& options) {
    const uint8_t* i = begin;
    while (i < end) {
      if (IsSpaceCharacterAscii(*i)) {
        *separator_begin = i;
        do {
          i++;
        } while (IsSpaceCharacterAscii(*i) && i < end);
        *separator_end = i;
        return true;
      }
      i++;
    }
    return false;
  }

  static bool FindReverse(const uint8_t* begin, const uint8_t* end,
                          const uint8_t** separator_begin, const uint8_t** separator_end,
                          const SplitOptions& options) {
    const uint8_t* i = end - 1;
    while ((i >= begin)) {
      if (IsSpaceCharacterAscii(*i)) {
        *separator_end = i + 1;
        do {
          i--;
        } while (IsSpaceCharacterAscii(*i) && i >= begin);
        *separator_begin = i + 1;
        return true;
      }
      i--;
    }
    return false;
  }
};

template <typename Type, typename ListType>
using SplitWhitespaceAsciiExec = SplitExec<Type, ListType, SplitWhitespaceAsciiFinder>;

void AddSplitWhitespaceAscii(FunctionRegistry* registry) {
  static const SplitOptions default_options{};
  auto func =
      std::make_shared<ScalarFunction>("ascii_split_whitespace", Arity::Unary(),
                                       &ascii_split_whitespace_doc, &default_options);
  using t32 = SplitWhitespaceAsciiExec<StringType, ListType>;
  using t64 = SplitWhitespaceAsciiExec<LargeStringType, ListType>;
  DCHECK_OK(func->AddKernel({utf8()}, {list(utf8())}, t32::Exec, t32::State::Init));
  DCHECK_OK(
      func->AddKernel({large_utf8()}, {list(large_utf8())}, t64::Exec, t64::State::Init));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

#ifdef ARROW_WITH_UTF8PROC
struct SplitWhitespaceUtf8Finder : public SplitFinderBase<SplitOptions> {
  using Options = SplitOptions;

  Status PreExec(const SplitOptions& options) override {
    EnsureLookupTablesFilled();
    return Status::OK();
  }

  bool Find(const uint8_t* begin, const uint8_t* end, const uint8_t** separator_begin,
            const uint8_t** separator_end, const SplitOptions& options) {
    const uint8_t* i = begin;
    while ((i < end)) {
      uint32_t codepoint = 0;
      *separator_begin = i;
      if (ARROW_PREDICT_FALSE(!arrow::util::UTF8Decode(&i, &codepoint))) {
        return false;
      }
      if (IsSpaceCharacterUnicode(codepoint)) {
        do {
          *separator_end = i;
          if (ARROW_PREDICT_FALSE(!arrow::util::UTF8Decode(&i, &codepoint))) {
            return false;
          }
        } while (IsSpaceCharacterUnicode(codepoint) && i < end);
        return true;
      }
    }
    return false;
  }

  bool FindReverse(const uint8_t* begin, const uint8_t* end,
                   const uint8_t** separator_begin, const uint8_t** separator_end,
                   const SplitOptions& options) {
    const uint8_t* i = end - 1;
    while ((i >= begin)) {
      uint32_t codepoint = 0;
      *separator_end = i + 1;
      if (ARROW_PREDICT_FALSE(!arrow::util::UTF8DecodeReverse(&i, &codepoint))) {
        return false;
      }
      if (IsSpaceCharacterUnicode(codepoint)) {
        do {
          *separator_begin = i + 1;
          if (ARROW_PREDICT_FALSE(!arrow::util::UTF8DecodeReverse(&i, &codepoint))) {
            return false;
          }
        } while (IsSpaceCharacterUnicode(codepoint) && i >= begin);
        return true;
      }
    }
    return false;
  }
};

template <typename Type, typename ListType>
using SplitWhitespaceUtf8Exec = SplitExec<Type, ListType, SplitWhitespaceUtf8Finder>;

void AddSplitWhitespaceUTF8(FunctionRegistry* registry) {
  static const SplitOptions default_options{};
  auto func =
      std::make_shared<ScalarFunction>("utf8_split_whitespace", Arity::Unary(),
                                       &utf8_split_whitespace_doc, &default_options);
  using t32 = SplitWhitespaceUtf8Exec<StringType, ListType>;
  using t64 = SplitWhitespaceUtf8Exec<LargeStringType, ListType>;
  DCHECK_OK(func->AddKernel({utf8()}, {list(utf8())}, t32::Exec, t32::State::Init));
  DCHECK_OK(
      func->AddKernel({large_utf8()}, {list(large_utf8())}, t64::Exec, t64::State::Init));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}
#endif  // ARROW_WITH_UTF8PROC

#ifdef ARROW_WITH_RE2
struct SplitRegexFinder : public SplitFinderBase<SplitPatternOptions> {
  using Options = SplitPatternOptions;

  util::optional<RE2> regex_split;

  Status PreExec(const SplitPatternOptions& options) override {
    if (options.reverse) {
      return Status::NotImplemented("Cannot split in reverse with regex");
    }
    // RE2 does *not* give you the full match! Must wrap the regex in a capture group
    // There is FindAndConsume, but it would give only the end of the separator
    std::string pattern = "(";
    pattern.reserve(options.pattern.size() + 2);
    pattern += options.pattern;
    pattern += ')';
    regex_split.emplace(std::move(pattern));
    return RegexStatus(*regex_split);
  }

  bool Find(const uint8_t* begin, const uint8_t* end, const uint8_t** separator_begin,
            const uint8_t** separator_end, const SplitPatternOptions& options) {
    re2::StringPiece piece(reinterpret_cast<const char*>(begin),
                           std::distance(begin, end));
    // "StringPiece is mutated to point to matched piece"
    re2::StringPiece result;
    if (!re2::RE2::PartialMatch(piece, *regex_split, &result)) {
      return false;
    }
    *separator_begin = reinterpret_cast<const uint8_t*>(result.data());
    *separator_end = reinterpret_cast<const uint8_t*>(result.data() + result.size());
    return true;
  }

  bool FindReverse(const uint8_t* begin, const uint8_t* end,
                   const uint8_t** separator_begin, const uint8_t** separator_end,
                   const SplitPatternOptions& options) {
    // Unsupported (see PreExec)
    return false;
  }
};

template <typename Type, typename ListType>
using SplitRegexExec = SplitExec<Type, ListType, SplitRegexFinder>;

const FunctionDoc split_pattern_regex_doc(
    "Split string according to regex pattern",
    ("Split each string according to the regex `pattern` defined in\n"
     "SplitPatternOptions.  The output for each string input is a list\n"
     "of strings.\n"
     "\n"
     "The maximum number of splits and direction of splitting\n"
     "(forward, reverse) can optionally be defined in SplitPatternOptions."),
    {"strings"}, "SplitPatternOptions");

void AddSplitRegex(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("split_pattern_regex", Arity::Unary(),
                                               &split_pattern_regex_doc);
  using t32 = SplitRegexExec<StringType, ListType>;
  using t64 = SplitRegexExec<LargeStringType, ListType>;
  DCHECK_OK(func->AddKernel({utf8()}, {list(utf8())}, t32::Exec, t32::State::Init));
  DCHECK_OK(
      func->AddKernel({large_utf8()}, {list(large_utf8())}, t64::Exec, t64::State::Init));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}
#endif  // ARROW_WITH_RE2

void AddSplit(FunctionRegistry* registry) {
  AddSplitPattern(registry);
  AddSplitWhitespaceAscii(registry);
#ifdef ARROW_WITH_UTF8PROC
  AddSplitWhitespaceUTF8(registry);
#endif
#ifdef ARROW_WITH_RE2
  AddSplitRegex(registry);
#endif
}

// ----------------------------------------------------------------------
// Replace substring (plain, regex)

template <typename Type, typename Replacer>
struct ReplaceSubString {
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  using offset_type = typename Type::offset_type;
  using ValueDataBuilder = TypedBufferBuilder<uint8_t>;
  using OffsetBuilder = TypedBufferBuilder<offset_type>;
  using State = OptionsWrapper<ReplaceSubstringOptions>;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // TODO Cache replacer across invocations (for regex compilation)
    ARROW_ASSIGN_OR_RAISE(auto replacer, Replacer::Make(State::Get(ctx)));
    return Replace(ctx, batch, *replacer, out);
  }

  static Status Replace(KernelContext* ctx, const ExecBatch& batch,
                        const Replacer& replacer, Datum* out) {
    ValueDataBuilder value_data_builder(ctx->memory_pool());
    OffsetBuilder offset_builder(ctx->memory_pool());

    if (batch[0].kind() == Datum::ARRAY) {
      // We already know how many strings we have, so we can use Reserve/UnsafeAppend
      RETURN_NOT_OK(offset_builder.Reserve(batch[0].array()->length + 1));
      offset_builder.UnsafeAppend(0);  // offsets start at 0

      const ArrayData& input = *batch[0].array();
      RETURN_NOT_OK(VisitArrayDataInline<Type>(
          input,
          [&](util::string_view s) {
            RETURN_NOT_OK(replacer.ReplaceString(s, &value_data_builder));
            offset_builder.UnsafeAppend(
                static_cast<offset_type>(value_data_builder.length()));
            return Status::OK();
          },
          [&]() {
            // offset for null value
            offset_builder.UnsafeAppend(
                static_cast<offset_type>(value_data_builder.length()));
            return Status::OK();
          }));
      ArrayData* output = out->mutable_array();
      RETURN_NOT_OK(value_data_builder.Finish(&output->buffers[2]));
      RETURN_NOT_OK(offset_builder.Finish(&output->buffers[1]));
    } else {
      const auto& input = checked_cast<const ScalarType&>(*batch[0].scalar());
      auto result = std::make_shared<ScalarType>();
      if (input.is_valid) {
        util::string_view s = static_cast<util::string_view>(*input.value);
        RETURN_NOT_OK(replacer.ReplaceString(s, &value_data_builder));
        RETURN_NOT_OK(value_data_builder.Finish(&result->value));
        result->is_valid = true;
      }
      out->value = result;
    }

    return Status::OK();
  }
};

struct PlainSubStringReplacer {
  const ReplaceSubstringOptions& options_;

  static Result<std::unique_ptr<PlainSubStringReplacer>> Make(
      const ReplaceSubstringOptions& options) {
    return arrow::internal::make_unique<PlainSubStringReplacer>(options);
  }

  explicit PlainSubStringReplacer(const ReplaceSubstringOptions& options)
      : options_(options) {}

  Status ReplaceString(util::string_view s, TypedBufferBuilder<uint8_t>* builder) const {
    const char* i = s.begin();
    const char* end = s.end();
    int64_t max_replacements = options_.max_replacements;
    while ((i < end) && (max_replacements != 0)) {
      const char* pos =
          std::search(i, end, options_.pattern.begin(), options_.pattern.end());
      if (pos == end) {
        RETURN_NOT_OK(builder->Append(reinterpret_cast<const uint8_t*>(i),
                                      static_cast<int64_t>(end - i)));
        i = end;
      } else {
        // the string before the pattern
        RETURN_NOT_OK(builder->Append(reinterpret_cast<const uint8_t*>(i),
                                      static_cast<int64_t>(pos - i)));
        // the replacement
        RETURN_NOT_OK(
            builder->Append(reinterpret_cast<const uint8_t*>(options_.replacement.data()),
                            options_.replacement.length()));
        // skip pattern
        i = pos + options_.pattern.length();
        max_replacements--;
      }
    }
    // if we exited early due to max_replacements, add the trailing part
    return builder->Append(reinterpret_cast<const uint8_t*>(i),
                           static_cast<int64_t>(end - i));
  }
};

#ifdef ARROW_WITH_RE2
struct RegexSubStringReplacer {
  const ReplaceSubstringOptions& options_;
  const RE2 regex_find_;
  const RE2 regex_replacement_;

  static Result<std::unique_ptr<RegexSubStringReplacer>> Make(
      const ReplaceSubstringOptions& options) {
    auto replacer = arrow::internal::make_unique<RegexSubStringReplacer>(options);

    RETURN_NOT_OK(RegexStatus(replacer->regex_find_));
    RETURN_NOT_OK(RegexStatus(replacer->regex_replacement_));

    std::string replacement_error;
    if (!replacer->regex_replacement_.CheckRewriteString(replacer->options_.replacement,
                                                         &replacement_error)) {
      return Status::Invalid("Invalid replacement string: ",
                             std::move(replacement_error));
    }

    return std::move(replacer);
  }

  // Using RE2::FindAndConsume we can only find the pattern if it is a group, therefore
  // we have 2 regexes, one with () around it, one without.
  explicit RegexSubStringReplacer(const ReplaceSubstringOptions& options)
      : options_(options),
        regex_find_("(" + options_.pattern + ")", RE2::Quiet),
        regex_replacement_(options_.pattern, RE2::Quiet) {}

  Status ReplaceString(util::string_view s, TypedBufferBuilder<uint8_t>* builder) const {
    re2::StringPiece replacement(options_.replacement);

    if (options_.max_replacements == -1) {
      std::string s_copy(s.to_string());
      re2::RE2::GlobalReplace(&s_copy, regex_replacement_, replacement);
      return builder->Append(reinterpret_cast<const uint8_t*>(s_copy.data()),
                             s_copy.length());
    }

    // Since RE2 does not have the concept of max_replacements, we have to do some work
    // ourselves.
    // We might do this faster similar to RE2::GlobalReplace using Match and Rewrite
    const char* i = s.begin();
    const char* end = s.end();
    re2::StringPiece piece(s.data(), s.length());

    int64_t max_replacements = options_.max_replacements;
    while ((i < end) && (max_replacements != 0)) {
      std::string found;
      if (!re2::RE2::FindAndConsume(&piece, regex_find_, &found)) {
        RETURN_NOT_OK(builder->Append(reinterpret_cast<const uint8_t*>(i),
                                      static_cast<int64_t>(end - i)));
        i = end;
      } else {
        // wind back to the beginning of the match
        const char* pos = piece.begin() - found.length();
        // the string before the pattern
        RETURN_NOT_OK(builder->Append(reinterpret_cast<const uint8_t*>(i),
                                      static_cast<int64_t>(pos - i)));
        // replace the pattern in what we found
        if (!re2::RE2::Replace(&found, regex_replacement_, replacement)) {
          return Status::Invalid("Regex found, but replacement failed");
        }
        RETURN_NOT_OK(builder->Append(reinterpret_cast<const uint8_t*>(found.data()),
                                      static_cast<int64_t>(found.length())));
        // skip pattern
        i = piece.begin();
        max_replacements--;
      }
    }
    // If we exited early due to max_replacements, add the trailing part
    return builder->Append(reinterpret_cast<const uint8_t*>(i),
                           static_cast<int64_t>(end - i));
  }
};
#endif

template <typename Type>
using ReplaceSubStringPlain = ReplaceSubString<Type, PlainSubStringReplacer>;

const FunctionDoc replace_substring_doc(
    "Replace non-overlapping substrings that match pattern by replacement",
    ("For each string in `strings`, replace non-overlapping substrings that match\n"
     "`pattern` by `replacement`. If `max_replacements != -1`, it determines the\n"
     "maximum amount of replacements made, counting from the left. Null values emit\n"
     "null."),
    {"strings"}, "ReplaceSubstringOptions");

#ifdef ARROW_WITH_RE2
template <typename Type>
using ReplaceSubStringRegex = ReplaceSubString<Type, RegexSubStringReplacer>;

const FunctionDoc replace_substring_regex_doc(
    "Replace non-overlapping substrings that match regex `pattern` by `replacement`",
    ("For each string in `strings`, replace non-overlapping substrings that match the\n"
     "regular expression `pattern` by `replacement` using the Google RE2 library.\n"
     "If `max_replacements != -1`, it determines the maximum amount of replacements\n"
     "made, counting from the left. Note that if the pattern contains groups,\n"
     "backreferencing macan be used. Null values emit null."),
    {"strings"}, "ReplaceSubstringOptions");
#endif

// ----------------------------------------------------------------------
// Replace slice

struct ReplaceSliceTransformBase : public StringTransformBase {
  using State = OptionsWrapper<ReplaceSliceOptions>;

  const ReplaceSliceOptions* options;

  explicit ReplaceSliceTransformBase(const ReplaceSliceOptions& options)
      : options{&options} {}

  int64_t MaxCodeunits(int64_t ninputs, int64_t input_ncodeunits) override {
    return ninputs * options->replacement.size() + input_ncodeunits;
  }
};

struct BinaryReplaceSliceTransform : ReplaceSliceTransformBase {
  using ReplaceSliceTransformBase::ReplaceSliceTransformBase;
  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    const auto& opts = *options;
    int64_t before_slice = 0;
    int64_t after_slice = 0;
    uint8_t* output_start = output;

    if (opts.start >= 0) {
      // Count from left
      before_slice = std::min<int64_t>(input_string_ncodeunits, opts.start);
    } else {
      // Count from right
      before_slice = std::max<int64_t>(0, input_string_ncodeunits + opts.start);
    }
    // Mimic Pandas: if stop would be before start, treat as 0-length slice
    if (opts.stop >= 0) {
      // Count from left
      after_slice =
          std::min<int64_t>(input_string_ncodeunits, std::max(before_slice, opts.stop));
    } else {
      // Count from right
      after_slice = std::max<int64_t>(before_slice, input_string_ncodeunits + opts.stop);
    }
    output = std::copy(input, input + before_slice, output);
    output = std::copy(opts.replacement.begin(), opts.replacement.end(), output);
    output = std::copy(input + after_slice, input + input_string_ncodeunits, output);
    return output - output_start;
  }
};

struct Utf8ReplaceSliceTransform : ReplaceSliceTransformBase {
  using ReplaceSliceTransformBase::ReplaceSliceTransformBase;
  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    const auto& opts = *options;
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t *begin_sliced, *end_sliced;
    uint8_t* output_start = output;

    // Mimic Pandas: if stop would be before start, treat as 0-length slice
    if (opts.start >= 0) {
      // Count from left
      if (!arrow::util::UTF8AdvanceCodepoints(begin, end, &begin_sliced, opts.start)) {
        return kTransformError;
      }
      if (opts.stop > options->start) {
        // Continue counting from left
        const int64_t length = opts.stop - options->start;
        if (!arrow::util::UTF8AdvanceCodepoints(begin_sliced, end, &end_sliced, length)) {
          return kTransformError;
        }
      } else if (opts.stop < 0) {
        // Count from right
        if (!arrow::util::UTF8AdvanceCodepointsReverse(begin_sliced, end, &end_sliced,
                                                       -opts.stop)) {
          return kTransformError;
        }
      } else {
        // Zero-length slice
        end_sliced = begin_sliced;
      }
    } else {
      // Count from right
      if (!arrow::util::UTF8AdvanceCodepointsReverse(begin, end, &begin_sliced,
                                                     -opts.start)) {
        return kTransformError;
      }
      if (opts.stop >= 0) {
        // Restart counting from left
        if (!arrow::util::UTF8AdvanceCodepoints(begin, end, &end_sliced, opts.stop)) {
          return kTransformError;
        }
        if (end_sliced <= begin_sliced) {
          // Zero-length slice
          end_sliced = begin_sliced;
        }
      } else if ((opts.stop < 0) && (options->stop > options->start)) {
        // Count from right
        if (!arrow::util::UTF8AdvanceCodepointsReverse(begin_sliced, end, &end_sliced,
                                                       -opts.stop)) {
          return kTransformError;
        }
      } else {
        // zero-length slice
        end_sliced = begin_sliced;
      }
    }
    output = std::copy(begin, begin_sliced, output);
    output = std::copy(opts.replacement.begin(), options->replacement.end(), output);
    output = std::copy(end_sliced, end, output);
    return output - output_start;
  }
};

template <typename Type>
using BinaryReplaceSlice =
    StringTransformExecWithState<Type, BinaryReplaceSliceTransform>;
template <typename Type>
using Utf8ReplaceSlice = StringTransformExecWithState<Type, Utf8ReplaceSliceTransform>;

const FunctionDoc binary_replace_slice_doc(
    "Replace a slice of a binary string with `replacement`",
    ("For each string in `strings`, replace a slice of the string defined by `start`"
     "and `stop` with `replacement`. `start` is inclusive and `stop` is exclusive, "
     "and both are measured in bytes.\n"
     "Null values emit null."),
    {"strings"}, "ReplaceSliceOptions");

const FunctionDoc utf8_replace_slice_doc(
    "Replace a slice of a string with `replacement`",
    ("For each string in `strings`, replace a slice of the string defined by `start`"
     "and `stop` with `replacement`. `start` is inclusive and `stop` is exclusive, "
     "and both are measured in codeunits.\n"
     "Null values emit null."),
    {"strings"}, "ReplaceSliceOptions");

void AddReplaceSlice(FunctionRegistry* registry) {
  {
    auto func = std::make_shared<ScalarFunction>("binary_replace_slice", Arity::Unary(),
                                                 &binary_replace_slice_doc);
    for (const auto& ty : BaseBinaryTypes()) {
      DCHECK_OK(func->AddKernel({ty}, ty,
                                GenerateTypeAgnosticVarBinaryBase<BinaryReplaceSlice>(ty),
                                ReplaceSliceTransformBase::State::Init));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<ScalarFunction>("utf8_replace_slice", Arity::Unary(),
                                                 &utf8_replace_slice_doc);
    DCHECK_OK(func->AddKernel({utf8()}, utf8(), Utf8ReplaceSlice<StringType>::Exec,
                              ReplaceSliceTransformBase::State::Init));
    DCHECK_OK(func->AddKernel({large_utf8()}, large_utf8(),
                              Utf8ReplaceSlice<LargeStringType>::Exec,
                              ReplaceSliceTransformBase::State::Init));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
}

// ----------------------------------------------------------------------
// Extract with regex

#ifdef ARROW_WITH_RE2

// TODO cache this once per ExtractRegexOptions
struct ExtractRegexData {
  // Use unique_ptr<> because RE2 is non-movable
  std::unique_ptr<RE2> regex;
  std::vector<std::string> group_names;

  static Result<ExtractRegexData> Make(const ExtractRegexOptions& options) {
    ExtractRegexData data(options.pattern);
    RETURN_NOT_OK(RegexStatus(*data.regex));

    const int group_count = data.regex->NumberOfCapturingGroups();
    const auto& name_map = data.regex->CapturingGroupNames();
    data.group_names.reserve(group_count);

    for (int i = 0; i < group_count; i++) {
      auto item = name_map.find(i + 1);  // re2 starts counting from 1
      if (item == name_map.end()) {
        // XXX should we instead just create fields with an empty name?
        return Status::Invalid("Regular expression contains unnamed groups");
      }
      data.group_names.emplace_back(item->second);
    }
    return std::move(data);
  }

  Result<ValueDescr> ResolveOutputType(const std::vector<ValueDescr>& args) const {
    const auto& input_type = args[0].type;
    if (input_type == nullptr) {
      // No input type specified => propagate shape
      return args[0];
    }
    // Input type is either String or LargeString and is also the type of each
    // field in the output struct type.
    DCHECK(input_type->id() == Type::STRING || input_type->id() == Type::LARGE_STRING);
    FieldVector fields;
    fields.reserve(group_names.size());
    std::transform(group_names.begin(), group_names.end(), std::back_inserter(fields),
                   [&](const std::string& name) { return field(name, input_type); });
    return struct_(std::move(fields));
  }

 private:
  explicit ExtractRegexData(const std::string& pattern)
      : regex(new RE2(pattern, RE2::Quiet)) {}
};

Result<ValueDescr> ResolveExtractRegexOutput(KernelContext* ctx,
                                             const std::vector<ValueDescr>& args) {
  using State = OptionsWrapper<ExtractRegexOptions>;
  ExtractRegexOptions options = State::Get(ctx);
  ARROW_ASSIGN_OR_RAISE(auto data, ExtractRegexData::Make(options));
  return data.ResolveOutputType(args);
}

struct ExtractRegexBase {
  const ExtractRegexData& data;
  const int group_count;
  std::vector<re2::StringPiece> found_values;
  std::vector<re2::RE2::Arg> args;
  std::vector<const re2::RE2::Arg*> args_pointers;
  const re2::RE2::Arg** args_pointers_start;
  const re2::RE2::Arg* null_arg = nullptr;

  explicit ExtractRegexBase(const ExtractRegexData& data)
      : data(data),
        group_count(static_cast<int>(data.group_names.size())),
        found_values(group_count) {
    args.reserve(group_count);
    args_pointers.reserve(group_count);

    for (int i = 0; i < group_count; i++) {
      args.emplace_back(&found_values[i]);
      // Since we reserved capacity, we're guaranteed the pointer remains valid
      args_pointers.push_back(&args[i]);
    }
    // Avoid null pointer if there is no capture group
    args_pointers_start = (group_count > 0) ? args_pointers.data() : &null_arg;
  }

  bool Match(util::string_view s) {
    return re2::RE2::PartialMatchN(ToStringPiece(s), *data.regex, args_pointers_start,
                                   group_count);
  }
};

template <typename Type>
struct ExtractRegex : public ExtractRegexBase {
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  using State = OptionsWrapper<ExtractRegexOptions>;

  using ExtractRegexBase::ExtractRegexBase;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ExtractRegexOptions options = State::Get(ctx);
    ARROW_ASSIGN_OR_RAISE(auto data, ExtractRegexData::Make(options));
    return ExtractRegex{data}.Extract(ctx, batch, out);
  }

  Status Extract(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ARROW_ASSIGN_OR_RAISE(auto descr, data.ResolveOutputType(batch.GetDescriptors()));
    DCHECK_NE(descr.type, nullptr);
    const auto& type = descr.type;

    if (batch[0].kind() == Datum::ARRAY) {
      std::unique_ptr<ArrayBuilder> array_builder;
      RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), type, &array_builder));
      StructBuilder* struct_builder = checked_cast<StructBuilder*>(array_builder.get());

      std::vector<BuilderType*> field_builders;
      field_builders.reserve(group_count);
      for (int i = 0; i < group_count; i++) {
        field_builders.push_back(
            checked_cast<BuilderType*>(struct_builder->field_builder(i)));
      }

      auto visit_null = [&]() { return struct_builder->AppendNull(); };
      auto visit_value = [&](util::string_view s) {
        if (Match(s)) {
          for (int i = 0; i < group_count; i++) {
            RETURN_NOT_OK(field_builders[i]->Append(ToStringView(found_values[i])));
          }
          return struct_builder->Append();
        } else {
          return struct_builder->AppendNull();
        }
      };
      const ArrayData& input = *batch[0].array();
      RETURN_NOT_OK(VisitArrayDataInline<Type>(input, visit_value, visit_null));

      std::shared_ptr<Array> out_array;
      RETURN_NOT_OK(struct_builder->Finish(&out_array));
      *out = std::move(out_array);
    } else {
      const auto& input = checked_cast<const ScalarType&>(*batch[0].scalar());
      auto result = std::make_shared<StructScalar>(type);
      if (input.is_valid && Match(util::string_view(*input.value))) {
        result->value.reserve(group_count);
        for (int i = 0; i < group_count; i++) {
          result->value.push_back(
              std::make_shared<ScalarType>(std::string{found_values[i]}));
        }
        result->is_valid = true;
      } else {
        result->is_valid = false;
      }
      out->value = std::move(result);
    }

    return Status::OK();
  }
};

const FunctionDoc extract_regex_doc(
    "Extract substrings captured by a regex pattern",
    ("For each string in `strings`, match the regular expression and, if\n"
     "successful, emit a struct with field names and values coming from the\n"
     "regular expression's named capture groups. If the input is null or the\n"
     "regular expression fails matching, a null output value is emitted.\n"
     "\n"
     "Regular expression matching is done using the Google RE2 library."),
    {"strings"}, "ExtractRegexOptions");

void AddExtractRegex(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("extract_regex", Arity::Unary(),
                                               &extract_regex_doc);
  using t32 = ExtractRegex<StringType>;
  using t64 = ExtractRegex<LargeStringType>;
  OutputType out_ty(ResolveExtractRegexOutput);
  ScalarKernel kernel;

  // Null values will be computed based on regex match or not
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  kernel.signature.reset(new KernelSignature({utf8()}, out_ty));
  kernel.exec = t32::Exec;
  kernel.init = t32::State::Init;
  DCHECK_OK(func->AddKernel(kernel));
  kernel.signature.reset(new KernelSignature({large_utf8()}, out_ty));
  kernel.exec = t64::Exec;
  kernel.init = t64::State::Init;
  DCHECK_OK(func->AddKernel(kernel));

  DCHECK_OK(registry->AddFunction(std::move(func)));
}
#endif  // ARROW_WITH_RE2

// ----------------------------------------------------------------------
// strptime string parsing

using StrptimeState = OptionsWrapper<StrptimeOptions>;

struct ParseStrptime {
  explicit ParseStrptime(const StrptimeOptions& options)
      : parser(TimestampParser::MakeStrptime(options.format)), unit(options.unit) {}

  template <typename... Ignored>
  int64_t Call(KernelContext*, util::string_view val, Status* st) const {
    int64_t result = 0;
    if (!(*parser)(val.data(), val.size(), unit, &result)) {
      *st = Status::Invalid("Failed to parse string: '", val, "' as a scalar of type ",
                            TimestampType(unit).ToString());
    }
    return result;
  }

  std::shared_ptr<TimestampParser> parser;
  TimeUnit::type unit;
};

template <typename InputType>
Status StrptimeExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  applicator::ScalarUnaryNotNullStateful<TimestampType, InputType, ParseStrptime> kernel{
      ParseStrptime(StrptimeState::Get(ctx))};
  return kernel.Exec(ctx, batch, out);
}

Result<ValueDescr> StrptimeResolve(KernelContext* ctx, const std::vector<ValueDescr>&) {
  if (ctx->state()) {
    return ::arrow::timestamp(StrptimeState::Get(ctx).unit);
  }

  return Status::Invalid("strptime does not provide default StrptimeOptions");
}

// ----------------------------------------------------------------------
// string padding

template <bool PadLeft, bool PadRight>
struct AsciiPadTransform : public StringTransformBase {
  using State = OptionsWrapper<PadOptions>;

  const PadOptions& options_;

  explicit AsciiPadTransform(const PadOptions& options) : options_(options) {}

  Status PreExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) override {
    if (options_.padding.size() != 1) {
      return Status::Invalid("Padding must be one byte, got '", options_.padding, "'");
    }
    return Status::OK();
  }

  int64_t MaxCodeunits(int64_t ninputs, int64_t input_ncodeunits) override {
    // This is likely very overallocated but hard to do better without
    // actually looking at each string (because of strings that may be
    // longer than the given width)
    return input_ncodeunits + ninputs * options_.width;
  }

  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    if (input_string_ncodeunits >= options_.width) {
      std::copy(input, input + input_string_ncodeunits, output);
      return input_string_ncodeunits;
    }
    const int64_t spaces = options_.width - input_string_ncodeunits;
    int64_t left = 0;
    int64_t right = 0;
    if (PadLeft && PadRight) {
      // If odd number of spaces, put the extra space on the right
      left = spaces / 2;
      right = spaces - left;
    } else if (PadLeft) {
      left = spaces;
    } else if (PadRight) {
      right = spaces;
    } else {
      DCHECK(false) << "unreachable";
      return 0;
    }
    std::fill(output, output + left, options_.padding[0]);
    output += left;
    output = std::copy(input, input + input_string_ncodeunits, output);
    std::fill(output, output + right, options_.padding[0]);
    return options_.width;
  }
};

template <bool PadLeft, bool PadRight>
struct Utf8PadTransform : public StringTransformBase {
  using State = OptionsWrapper<PadOptions>;

  const PadOptions& options_;

  explicit Utf8PadTransform(const PadOptions& options) : options_(options) {}

  Status PreExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) override {
    auto str = reinterpret_cast<const uint8_t*>(options_.padding.data());
    auto strlen = options_.padding.size();
    if (util::UTF8Length(str, str + strlen) != 1) {
      return Status::Invalid("Padding must be one codepoint, got '", options_.padding,
                             "'");
    }
    return Status::OK();
  }

  int64_t MaxCodeunits(int64_t ninputs, int64_t input_ncodeunits) override {
    // This is likely very overallocated but hard to do better without
    // actually looking at each string (because of strings that may be
    // longer than the given width)
    // One codepoint may be up to 4 bytes
    return input_ncodeunits + 4 * ninputs * options_.width;
  }

  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    const int64_t input_width = util::UTF8Length(input, input + input_string_ncodeunits);
    if (input_width >= options_.width) {
      std::copy(input, input + input_string_ncodeunits, output);
      return input_string_ncodeunits;
    }
    const int64_t spaces = options_.width - input_width;
    int64_t left = 0;
    int64_t right = 0;
    if (PadLeft && PadRight) {
      // If odd number of spaces, put the extra space on the right
      left = spaces / 2;
      right = spaces - left;
    } else if (PadLeft) {
      left = spaces;
    } else if (PadRight) {
      right = spaces;
    } else {
      DCHECK(false) << "unreachable";
      return 0;
    }
    uint8_t* start = output;
    while (left) {
      output = std::copy(options_.padding.begin(), options_.padding.end(), output);
      left--;
    }
    output = std::copy(input, input + input_string_ncodeunits, output);
    while (right) {
      output = std::copy(options_.padding.begin(), options_.padding.end(), output);
      right--;
    }
    return output - start;
  }
};

template <typename Type>
using AsciiLPad = StringTransformExecWithState<Type, AsciiPadTransform<true, false>>;
template <typename Type>
using AsciiRPad = StringTransformExecWithState<Type, AsciiPadTransform<false, true>>;
template <typename Type>
using AsciiCenter = StringTransformExecWithState<Type, AsciiPadTransform<true, true>>;
template <typename Type>
using Utf8LPad = StringTransformExecWithState<Type, Utf8PadTransform<true, false>>;
template <typename Type>
using Utf8RPad = StringTransformExecWithState<Type, Utf8PadTransform<false, true>>;
template <typename Type>
using Utf8Center = StringTransformExecWithState<Type, Utf8PadTransform<true, true>>;

// ----------------------------------------------------------------------
// string trimming

#ifdef ARROW_WITH_UTF8PROC

template <bool TrimLeft, bool TrimRight>
struct UTF8TrimWhitespaceTransform : public StringTransformBase {
  Status PreExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) override {
    EnsureLookupTablesFilled();
    return Status::OK();
  }

  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* end_trimmed = end;
    const uint8_t* begin_trimmed = begin;

    auto predicate = [](uint32_t c) { return !IsSpaceCharacterUnicode(c); };
    if (TrimLeft && !ARROW_PREDICT_TRUE(
                        arrow::util::UTF8FindIf(begin, end, predicate, &begin_trimmed))) {
      return kTransformError;
    }
    if (TrimRight && begin_trimmed < end) {
      if (!ARROW_PREDICT_TRUE(arrow::util::UTF8FindIfReverse(begin_trimmed, end,
                                                             predicate, &end_trimmed))) {
        return kTransformError;
      }
    }
    std::copy(begin_trimmed, end_trimmed, output);
    return end_trimmed - begin_trimmed;
  }
};

template <typename Type>
using UTF8TrimWhitespace =
    StringTransformExec<Type, UTF8TrimWhitespaceTransform<true, true>>;

template <typename Type>
using UTF8LTrimWhitespace =
    StringTransformExec<Type, UTF8TrimWhitespaceTransform<true, false>>;

template <typename Type>
using UTF8RTrimWhitespace =
    StringTransformExec<Type, UTF8TrimWhitespaceTransform<false, true>>;

struct UTF8TrimState {
  TrimOptions options_;
  std::vector<bool> codepoints_;
  Status status_ = Status::OK();

  explicit UTF8TrimState(KernelContext* ctx, TrimOptions options)
      : options_(std::move(options)) {
    if (!ARROW_PREDICT_TRUE(
            arrow::util::UTF8ForEach(options_.characters, [&](uint32_t c) {
              codepoints_.resize(
                  std::max(c + 1, static_cast<uint32_t>(codepoints_.size())));
              codepoints_.at(c) = true;
            }))) {
      status_ = Status::Invalid("Invalid UTF8 sequence in input");
    }
  }
};

template <bool TrimLeft, bool TrimRight>
struct UTF8TrimTransform : public StringTransformBase {
  using State = KernelStateFromFunctionOptions<UTF8TrimState, TrimOptions>;

  const UTF8TrimState& state_;

  explicit UTF8TrimTransform(const UTF8TrimState& state) : state_(state) {}

  Status PreExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) override {
    return state_.status_;
  }

  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* end_trimmed = end;
    const uint8_t* begin_trimmed = begin;

    auto predicate = [&](uint32_t c) { return !state_.codepoints_[c]; };
    if (TrimLeft && !ARROW_PREDICT_TRUE(
                        arrow::util::UTF8FindIf(begin, end, predicate, &begin_trimmed))) {
      return kTransformError;
    }
    if (TrimRight && begin_trimmed < end) {
      if (!ARROW_PREDICT_TRUE(arrow::util::UTF8FindIfReverse(begin_trimmed, end,
                                                             predicate, &end_trimmed))) {
        return kTransformError;
      }
    }
    std::copy(begin_trimmed, end_trimmed, output);
    return end_trimmed - begin_trimmed;
  }
};

template <typename Type>
using UTF8Trim = StringTransformExecWithState<Type, UTF8TrimTransform<true, true>>;

template <typename Type>
using UTF8LTrim = StringTransformExecWithState<Type, UTF8TrimTransform<true, false>>;

template <typename Type>
using UTF8RTrim = StringTransformExecWithState<Type, UTF8TrimTransform<false, true>>;

#endif

template <bool TrimLeft, bool TrimRight>
struct AsciiTrimWhitespaceTransform : public StringTransformBase {
  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* end_trimmed = end;
    const uint8_t* begin_trimmed = begin;

    auto predicate = [](unsigned char c) { return !IsSpaceCharacterAscii(c); };
    if (TrimLeft) {
      begin_trimmed = std::find_if(begin, end, predicate);
    }
    if (TrimRight && begin_trimmed < end) {
      std::reverse_iterator<const uint8_t*> rbegin(end);
      std::reverse_iterator<const uint8_t*> rend(begin_trimmed);
      end_trimmed = std::find_if(rbegin, rend, predicate).base();
    }
    std::copy(begin_trimmed, end_trimmed, output);
    return end_trimmed - begin_trimmed;
  }
};

template <typename Type>
using AsciiTrimWhitespace =
    StringTransformExec<Type, AsciiTrimWhitespaceTransform<true, true>>;

template <typename Type>
using AsciiLTrimWhitespace =
    StringTransformExec<Type, AsciiTrimWhitespaceTransform<true, false>>;

template <typename Type>
using AsciiRTrimWhitespace =
    StringTransformExec<Type, AsciiTrimWhitespaceTransform<false, true>>;

struct AsciiTrimState {
  TrimOptions options_;
  std::vector<bool> characters_;

  explicit AsciiTrimState(KernelContext* ctx, TrimOptions options)
      : options_(std::move(options)), characters_(256) {
    for (const auto c : options_.characters) {
      characters_[static_cast<unsigned char>(c)] = true;
    }
  }
};

template <bool TrimLeft, bool TrimRight>
struct AsciiTrimTransform : public StringTransformBase {
  using State = KernelStateFromFunctionOptions<AsciiTrimState, TrimOptions>;

  const AsciiTrimState& state_;

  explicit AsciiTrimTransform(const AsciiTrimState& state) : state_(state) {}

  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* end_trimmed = end;
    const uint8_t* begin_trimmed = begin;

    auto predicate = [&](uint8_t c) { return !state_.characters_[c]; };
    if (TrimLeft) {
      begin_trimmed = std::find_if(begin, end, predicate);
    }
    if (TrimRight && begin_trimmed < end) {
      std::reverse_iterator<const uint8_t*> rbegin(end);
      std::reverse_iterator<const uint8_t*> rend(begin_trimmed);
      end_trimmed = std::find_if(rbegin, rend, predicate).base();
    }
    std::copy(begin_trimmed, end_trimmed, output);
    return end_trimmed - begin_trimmed;
  }
};

template <typename Type>
using AsciiTrim = StringTransformExecWithState<Type, AsciiTrimTransform<true, true>>;

template <typename Type>
using AsciiLTrim = StringTransformExecWithState<Type, AsciiTrimTransform<true, false>>;

template <typename Type>
using AsciiRTrim = StringTransformExecWithState<Type, AsciiTrimTransform<false, true>>;

const FunctionDoc utf8_center_doc(
    "Center strings by padding with a given character",
    ("For each string in `strings`, emit a centered string by padding both sides \n"
     "with the given UTF8 codeunit.\nNull values emit null."),
    {"strings"}, "PadOptions");

const FunctionDoc utf8_lpad_doc(
    "Right-align strings by padding with a given character",
    ("For each string in `strings`, emit a right-aligned string by prepending \n"
     "the given UTF8 codeunit.\nNull values emit null."),
    {"strings"}, "PadOptions");

const FunctionDoc utf8_rpad_doc(
    "Left-align strings by padding with a given character",
    ("For each string in `strings`, emit a left-aligned string by appending \n"
     "the given UTF8 codeunit.\nNull values emit null."),
    {"strings"}, "PadOptions");

const FunctionDoc ascii_center_doc(
    utf8_center_doc.description + "",
    ("For each string in `strings`, emit a centered string by padding both sides \n"
     "with the given ASCII character.\nNull values emit null."),
    {"strings"}, "PadOptions");

const FunctionDoc ascii_lpad_doc(
    utf8_lpad_doc.description + "",
    ("For each string in `strings`, emit a right-aligned string by prepending \n"
     "the given ASCII character.\nNull values emit null."),
    {"strings"}, "PadOptions");

const FunctionDoc ascii_rpad_doc(
    utf8_rpad_doc.description + "",
    ("For each string in `strings`, emit a left-aligned string by appending \n"
     "the given ASCII character.\nNull values emit null."),
    {"strings"}, "PadOptions");

const FunctionDoc utf8_trim_whitespace_doc(
    "Trim leading and trailing whitespace characters",
    ("For each string in `strings`, emit a string with leading and trailing whitespace\n"
     "characters removed, where whitespace characters are defined by the Unicode\n"
     "standard.  Null values emit null."),
    {"strings"});

const FunctionDoc utf8_ltrim_whitespace_doc(
    "Trim leading whitespace characters",
    ("For each string in `strings`, emit a string with leading whitespace\n"
     "characters removed, where whitespace characters are defined by the Unicode\n"
     "standard.  Null values emit null."),
    {"strings"});

const FunctionDoc utf8_rtrim_whitespace_doc(
    "Trim trailing whitespace characters",
    ("For each string in `strings`, emit a string with trailing whitespace\n"
     "characters removed, where whitespace characters are defined by the Unicode\n"
     "standard.  Null values emit null."),
    {"strings"});

const FunctionDoc ascii_trim_whitespace_doc(
    "Trim leading and trailing ASCII whitespace characters",
    ("For each string in `strings`, emit a string with leading and trailing ASCII\n"
     "whitespace characters removed. Use `utf8_trim_whitespace` to trim Unicode\n"
     "whitespace characters. Null values emit null."),
    {"strings"});

const FunctionDoc ascii_ltrim_whitespace_doc(
    "Trim leading ASCII whitespace characters",
    ("For each string in `strings`, emit a string with leading ASCII whitespace\n"
     "characters removed.  Use `utf8_ltrim_whitespace` to trim leading Unicode\n"
     "whitespace characters. Null values emit null."),
    {"strings"});

const FunctionDoc ascii_rtrim_whitespace_doc(
    "Trim trailing ASCII whitespace characters",
    ("For each string in `strings`, emit a string with trailing ASCII whitespace\n"
     "characters removed. Use `utf8_rtrim_whitespace` to trim trailing Unicode\n"
     "whitespace characters. Null values emit null."),
    {"strings"});

const FunctionDoc utf8_trim_doc(
    "Trim leading and trailing characters present in the `characters` arguments",
    ("For each string in `strings`, emit a string with leading and trailing\n"
     "characters removed that are present in the `characters` argument.  Null values\n"
     "emit null."),
    {"strings"}, "TrimOptions");

const FunctionDoc utf8_ltrim_doc(
    "Trim leading characters present in the `characters` arguments",
    ("For each string in `strings`, emit a string with leading\n"
     "characters removed that are present in the `characters` argument.  Null values\n"
     "emit null."),
    {"strings"}, "TrimOptions");

const FunctionDoc utf8_rtrim_doc(
    "Trim trailing characters present in the `characters` arguments",
    ("For each string in `strings`, emit a string with leading "
     "characters removed that are present in the `characters` argument.  Null values\n"
     "emit null."),
    {"strings"}, "TrimOptions");

const FunctionDoc ascii_trim_doc(
    utf8_trim_doc.summary + "",
    utf8_trim_doc.description +
        ("\nBoth the input string as the `characters` argument are interepreted as\n"
         "ASCII characters, to trim non-ASCII characters, use `utf8_trim`."),
    {"strings"}, "TrimOptions");

const FunctionDoc ascii_ltrim_doc(
    utf8_ltrim_doc.summary + "",
    utf8_ltrim_doc.description +
        ("\nBoth the input string as the `characters` argument are interepreted as\n"
         "ASCII characters, to trim non-ASCII characters, use `utf8_trim`."),
    {"strings"}, "TrimOptions");

const FunctionDoc ascii_rtrim_doc(
    utf8_rtrim_doc.summary + "",
    utf8_rtrim_doc.description +
        ("\nBoth the input string as the `characters` argument are interepreted as\n"
         "ASCII characters, to trim non-ASCII characters, use `utf8_trim`."),
    {"strings"}, "TrimOptions");

const FunctionDoc strptime_doc(
    "Parse timestamps",
    ("For each string in `strings`, parse it as a timestamp.\n"
     "The timestamp unit and the expected string pattern must be given\n"
     "in StrptimeOptions.  Null inputs emit null.  If a non-null string\n"
     "fails parsing, an error is returned."),
    {"strings"}, "StrptimeOptions");

const FunctionDoc binary_length_doc(
    "Compute string lengths",
    ("For each string in `strings`, emit the number of bytes.  Null values emit null."),
    {"strings"});

const FunctionDoc utf8_length_doc("Compute UTF8 string lengths",
                                  ("For each string in `strings`, emit the number of "
                                   "UTF8 characters.  Null values emit null."),
                                  {"strings"});

void AddStrptime(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("strptime", Arity::Unary(), &strptime_doc);
  DCHECK_OK(func->AddKernel({utf8()}, OutputType(StrptimeResolve),
                            StrptimeExec<StringType>, StrptimeState::Init));
  DCHECK_OK(func->AddKernel({large_utf8()}, OutputType(StrptimeResolve),
                            StrptimeExec<LargeStringType>, StrptimeState::Init));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

void AddBinaryLength(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("binary_length", Arity::Unary(),
                                               &binary_length_doc);
  ArrayKernelExec exec_offset_32 =
      applicator::ScalarUnaryNotNull<Int32Type, StringType, BinaryLength>::Exec;
  ArrayKernelExec exec_offset_64 =
      applicator::ScalarUnaryNotNull<Int64Type, LargeStringType, BinaryLength>::Exec;
  for (const auto& input_type : {binary(), utf8()}) {
    DCHECK_OK(func->AddKernel({input_type}, int32(), exec_offset_32));
  }
  for (const auto& input_type : {large_binary(), large_utf8()}) {
    DCHECK_OK(func->AddKernel({input_type}, int64(), exec_offset_64));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

void AddUtf8Length(FunctionRegistry* registry) {
  auto func =
      std::make_shared<ScalarFunction>("utf8_length", Arity::Unary(), &utf8_length_doc);

  ArrayKernelExec exec_offset_32 =
      applicator::ScalarUnaryNotNull<Int32Type, StringType, Utf8Length>::Exec;
  DCHECK_OK(func->AddKernel({utf8()}, int32(), std::move(exec_offset_32)));

  ArrayKernelExec exec_offset_64 =
      applicator::ScalarUnaryNotNull<Int64Type, LargeStringType, Utf8Length>::Exec;
  DCHECK_OK(func->AddKernel({large_utf8()}, int64(), std::move(exec_offset_64)));

  DCHECK_OK(registry->AddFunction(std::move(func)));
}

template <typename BinaryType, typename ListType>
struct BinaryJoin {
  using ArrayType = typename TypeTraits<BinaryType>::ArrayType;
  using ListArrayType = typename TypeTraits<ListType>::ArrayType;
  using ListScalarType = typename TypeTraits<ListType>::ScalarType;
  using ListOffsetType = typename ListArrayType::offset_type;
  using BuilderType = typename TypeTraits<BinaryType>::BuilderType;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].kind() == Datum::SCALAR) {
      if (batch[1].kind() == Datum::SCALAR) {
        return ExecScalarScalar(ctx, *batch[0].scalar(), *batch[1].scalar(), out);
      }
      DCHECK_EQ(batch[1].kind(), Datum::ARRAY);
      return ExecScalarArray(ctx, *batch[0].scalar(), batch[1].array(), out);
    }
    DCHECK_EQ(batch[0].kind(), Datum::ARRAY);
    if (batch[1].kind() == Datum::SCALAR) {
      return ExecArrayScalar(ctx, batch[0].array(), *batch[1].scalar(), out);
    }
    DCHECK_EQ(batch[1].kind(), Datum::ARRAY);
    return ExecArrayArray(ctx, batch[0].array(), batch[1].array(), out);
  }

  struct ListScalarOffsetLookup {
    const ArrayType& values;

    int64_t GetStart(int64_t i) { return 0; }
    int64_t GetStop(int64_t i) { return values.length(); }
    bool IsNull(int64_t i) { return false; }
  };

  struct ListArrayOffsetLookup {
    explicit ListArrayOffsetLookup(const ListArrayType& lists)
        : lists_(lists), offsets_(lists.raw_value_offsets()) {}

    int64_t GetStart(int64_t i) { return offsets_[i]; }
    int64_t GetStop(int64_t i) { return offsets_[i + 1]; }
    bool IsNull(int64_t i) { return lists_.IsNull(i); }

   private:
    const ListArrayType& lists_;
    const ListOffsetType* offsets_;
  };

  struct SeparatorScalarLookup {
    const util::string_view separator;

    bool IsNull(int64_t i) { return false; }
    util::string_view GetView(int64_t i) { return separator; }
  };

  struct SeparatorArrayLookup {
    const ArrayType& separators;

    bool IsNull(int64_t i) { return separators.IsNull(i); }
    util::string_view GetView(int64_t i) { return separators.GetView(i); }
  };

  // Scalar, scalar -> scalar
  static Status ExecScalarScalar(KernelContext* ctx, const Scalar& left,
                                 const Scalar& right, Datum* out) {
    const auto& list = checked_cast<const ListScalarType&>(left);
    const auto& separator_scalar = checked_cast<const BaseBinaryScalar&>(right);
    if (!list.is_valid || !separator_scalar.is_valid) {
      return Status::OK();
    }
    util::string_view separator(*separator_scalar.value);

    const auto& strings = checked_cast<const ArrayType&>(*list.value);
    if (strings.null_count() > 0) {
      out->scalar()->is_valid = false;
      return Status::OK();
    }

    TypedBufferBuilder<uint8_t> builder(ctx->memory_pool());
    auto Append = [&](util::string_view value) {
      return builder.Append(reinterpret_cast<const uint8_t*>(value.data()),
                            static_cast<int64_t>(value.size()));
    };
    if (strings.length() > 0) {
      auto data_length =
          strings.total_values_length() + (strings.length() - 1) * separator.length();
      RETURN_NOT_OK(builder.Reserve(data_length));
      RETURN_NOT_OK(Append(strings.GetView(0)));
      for (int64_t j = 1; j < strings.length(); j++) {
        RETURN_NOT_OK(Append(separator));
        RETURN_NOT_OK(Append(strings.GetView(j)));
      }
    }
    auto out_scalar = checked_cast<BaseBinaryScalar*>(out->scalar().get());
    return builder.Finish(&out_scalar->value);
  }

  // Scalar, array -> array
  static Status ExecScalarArray(KernelContext* ctx, const Scalar& left,
                                const std::shared_ptr<ArrayData>& right, Datum* out) {
    const auto& list_scalar = checked_cast<const BaseListScalar&>(left);
    if (!list_scalar.is_valid) {
      ARROW_ASSIGN_OR_RAISE(
          auto nulls, MakeArrayOfNull(right->type, right->length, ctx->memory_pool()));
      *out = *nulls->data();
      return Status::OK();
    }
    const auto& strings = checked_cast<const ArrayType&>(*list_scalar.value);
    if (strings.null_count() != 0) {
      ARROW_ASSIGN_OR_RAISE(
          auto nulls, MakeArrayOfNull(right->type, right->length, ctx->memory_pool()));
      *out = *nulls->data();
      return Status::OK();
    }
    const ArrayType separators(right);

    BuilderType builder(ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(separators.length()));

    // Presize data to avoid multiple reallocations when joining strings
    int64_t total_data_length = 0;
    const int64_t list_length = strings.length();
    if (list_length) {
      const int64_t string_length = strings.total_values_length();
      total_data_length +=
          string_length * (separators.length() - separators.null_count());
      for (int64_t i = 0; i < separators.length(); ++i) {
        if (separators.IsNull(i)) {
          continue;
        }
        total_data_length += (list_length - 1) * separators.value_length(i);
      }
    }
    RETURN_NOT_OK(builder.ReserveData(total_data_length));

    return JoinStrings(separators.length(), strings, ListScalarOffsetLookup{strings},
                       SeparatorArrayLookup{separators}, &builder, out);
  }

  // Array, scalar -> array
  static Status ExecArrayScalar(KernelContext* ctx,
                                const std::shared_ptr<ArrayData>& left,
                                const Scalar& right, Datum* out) {
    const ListArrayType lists(left);
    const auto& separator_scalar = checked_cast<const BaseBinaryScalar&>(right);

    if (!separator_scalar.is_valid) {
      ARROW_ASSIGN_OR_RAISE(
          auto nulls,
          MakeArrayOfNull(lists.value_type(), lists.length(), ctx->memory_pool()));
      *out = *nulls->data();
      return Status::OK();
    }

    util::string_view separator(*separator_scalar.value);
    const auto& strings = checked_cast<const ArrayType&>(*lists.values());
    const auto list_offsets = lists.raw_value_offsets();

    BuilderType builder(ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(lists.length()));

    // Presize data to avoid multiple reallocations when joining strings
    int64_t total_data_length = strings.total_values_length();
    for (int64_t i = 0; i < lists.length(); ++i) {
      const auto start = list_offsets[i], end = list_offsets[i + 1];
      if (end > start && !ValuesContainNull(strings, start, end)) {
        total_data_length += (end - start - 1) * separator.length();
      }
    }
    RETURN_NOT_OK(builder.ReserveData(total_data_length));

    return JoinStrings(lists.length(), strings, ListArrayOffsetLookup{lists},
                       SeparatorScalarLookup{separator}, &builder, out);
  }

  // Array, array -> array
  static Status ExecArrayArray(KernelContext* ctx, const std::shared_ptr<ArrayData>& left,
                               const std::shared_ptr<ArrayData>& right, Datum* out) {
    const ListArrayType lists(left);
    const auto& strings = checked_cast<const ArrayType&>(*lists.values());
    const auto list_offsets = lists.raw_value_offsets();
    const auto string_offsets = strings.raw_value_offsets();
    const ArrayType separators(right);

    BuilderType builder(ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(lists.length()));

    // Presize data to avoid multiple reallocations when joining strings
    int64_t total_data_length = 0;
    for (int64_t i = 0; i < lists.length(); ++i) {
      if (separators.IsNull(i)) {
        continue;
      }
      const auto start = list_offsets[i], end = list_offsets[i + 1];
      if (end > start && !ValuesContainNull(strings, start, end)) {
        total_data_length += string_offsets[end] - string_offsets[start];
        total_data_length += (end - start - 1) * separators.value_length(i);
      }
    }
    RETURN_NOT_OK(builder.ReserveData(total_data_length));

    struct SeparatorLookup {
      const ArrayType& separators;

      bool IsNull(int64_t i) { return separators.IsNull(i); }
      util::string_view GetView(int64_t i) { return separators.GetView(i); }
    };
    return JoinStrings(lists.length(), strings, ListArrayOffsetLookup{lists},
                       SeparatorArrayLookup{separators}, &builder, out);
  }

  template <typename ListOffsetLookup, typename SeparatorLookup>
  static Status JoinStrings(int64_t length, const ArrayType& strings,
                            ListOffsetLookup&& list_offsets, SeparatorLookup&& separators,
                            BuilderType* builder, Datum* out) {
    for (int64_t i = 0; i < length; ++i) {
      if (list_offsets.IsNull(i) || separators.IsNull(i)) {
        builder->UnsafeAppendNull();
        continue;
      }
      const auto j_start = list_offsets.GetStart(i), j_end = list_offsets.GetStop(i);
      if (j_start == j_end) {
        builder->UnsafeAppendEmptyValue();
        continue;
      }
      if (ValuesContainNull(strings, j_start, j_end)) {
        builder->UnsafeAppendNull();
        continue;
      }
      builder->UnsafeAppend(strings.GetView(j_start));
      for (int64_t j = j_start + 1; j < j_end; ++j) {
        builder->UnsafeExtendCurrent(separators.GetView(i));
        builder->UnsafeExtendCurrent(strings.GetView(j));
      }
    }

    std::shared_ptr<Array> string_array;
    RETURN_NOT_OK(builder->Finish(&string_array));
    *out = *string_array->data();
    // Correct the output type based on the input
    out->mutable_array()->type = strings.type();
    return Status::OK();
  }

  static bool ValuesContainNull(const ArrayType& values, int64_t start, int64_t end) {
    if (values.null_count() == 0) {
      return false;
    }
    for (int64_t i = start; i < end; ++i) {
      if (values.IsNull(i)) {
        return true;
      }
    }
    return false;
  }
};

using BinaryJoinElementWiseState = OptionsWrapper<JoinOptions>;

template <typename Type>
struct BinaryJoinElementWise {
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  using offset_type = typename Type::offset_type;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    JoinOptions options = BinaryJoinElementWiseState::Get(ctx);
    // Last argument is the separator (for consistency with binary_join)
    if (std::all_of(batch.values.begin(), batch.values.end(),
                    [](const Datum& d) { return d.is_scalar(); })) {
      return ExecOnlyScalar(ctx, options, batch, out);
    }
    return ExecContainingArrays(ctx, options, batch, out);
  }

  static Status ExecOnlyScalar(KernelContext* ctx, const JoinOptions& options,
                               const ExecBatch& batch, Datum* out) {
    BaseBinaryScalar* output = checked_cast<BaseBinaryScalar*>(out->scalar().get());
    const size_t num_args = batch.values.size();
    if (num_args == 1) {
      // Only separator, no values
      ARROW_ASSIGN_OR_RAISE(output->value, ctx->Allocate(0));
      output->is_valid = batch.values[0].scalar()->is_valid;
      return Status::OK();
    }

    int64_t final_size = CalculateRowSize(options, batch, 0);
    if (final_size < 0) {
      ARROW_ASSIGN_OR_RAISE(output->value, ctx->Allocate(0));
      output->is_valid = false;
      return Status::OK();
    }
    ARROW_ASSIGN_OR_RAISE(output->value, ctx->Allocate(final_size));
    const auto separator = UnboxScalar<Type>::Unbox(*batch.values.back().scalar());
    uint8_t* buf = output->value->mutable_data();
    bool first = true;
    for (size_t i = 0; i < num_args - 1; i++) {
      const Scalar& scalar = *batch[i].scalar();
      util::string_view s;
      if (scalar.is_valid) {
        s = UnboxScalar<Type>::Unbox(scalar);
      } else {
        switch (options.null_handling) {
          case JoinOptions::EMIT_NULL:
            // Handled by CalculateRowSize
            DCHECK(false) << "unreachable";
            break;
          case JoinOptions::SKIP:
            continue;
          case JoinOptions::REPLACE:
            s = options.null_replacement;
            break;
        }
      }
      if (!first) {
        buf = std::copy(separator.begin(), separator.end(), buf);
      }
      first = false;
      buf = std::copy(s.begin(), s.end(), buf);
    }
    output->is_valid = true;
    DCHECK_EQ(final_size, buf - output->value->mutable_data());
    return Status::OK();
  }

  static Status ExecContainingArrays(KernelContext* ctx, const JoinOptions& options,
                                     const ExecBatch& batch, Datum* out) {
    // Presize data to avoid reallocations
    int64_t final_size = 0;
    for (int64_t i = 0; i < batch.length; i++) {
      auto size = CalculateRowSize(options, batch, i);
      if (size > 0) final_size += size;
    }
    BuilderType builder(ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(batch.length));
    RETURN_NOT_OK(builder.ReserveData(final_size));

    std::vector<util::string_view> valid_cols(batch.values.size());
    for (size_t row = 0; row < static_cast<size_t>(batch.length); row++) {
      size_t num_valid = 0;  // Not counting separator
      for (size_t col = 0; col < batch.values.size(); col++) {
        if (batch[col].is_scalar()) {
          const auto& scalar = *batch[col].scalar();
          if (scalar.is_valid) {
            valid_cols[col] = UnboxScalar<Type>::Unbox(scalar);
            if (col < batch.values.size() - 1) num_valid++;
          } else {
            valid_cols[col] = util::string_view();
          }
        } else {
          const ArrayData& array = *batch[col].array();
          if (!array.MayHaveNulls() ||
              BitUtil::GetBit(array.buffers[0]->data(), array.offset + row)) {
            const offset_type* offsets = array.GetValues<offset_type>(1);
            const uint8_t* data = array.GetValues<uint8_t>(2, /*absolute_offset=*/0);
            const int64_t length = offsets[row + 1] - offsets[row];
            valid_cols[col] = util::string_view(
                reinterpret_cast<const char*>(data + offsets[row]), length);
            if (col < batch.values.size() - 1) num_valid++;
          } else {
            valid_cols[col] = util::string_view();
          }
        }
      }

      if (!valid_cols.back().data()) {
        // Separator is null
        builder.UnsafeAppendNull();
        continue;
      } else if (batch.values.size() == 1) {
        // Only given separator
        builder.UnsafeAppendEmptyValue();
        continue;
      } else if (num_valid < batch.values.size() - 1) {
        // We had some nulls
        if (options.null_handling == JoinOptions::EMIT_NULL) {
          builder.UnsafeAppendNull();
          continue;
        }
      }
      const auto separator = valid_cols.back();
      bool first = true;
      for (size_t col = 0; col < batch.values.size() - 1; col++) {
        util::string_view value = valid_cols[col];
        if (!value.data()) {
          switch (options.null_handling) {
            case JoinOptions::EMIT_NULL:
              DCHECK(false) << "unreachable";
              break;
            case JoinOptions::SKIP:
              continue;
            case JoinOptions::REPLACE:
              value = options.null_replacement;
              break;
          }
        }
        if (first) {
          builder.UnsafeAppend(value);
          first = false;
          continue;
        }
        builder.UnsafeExtendCurrent(separator);
        builder.UnsafeExtendCurrent(value);
      }
    }

    std::shared_ptr<Array> string_array;
    RETURN_NOT_OK(builder.Finish(&string_array));
    *out = *string_array->data();
    out->mutable_array()->type = batch[0].type();
    DCHECK_EQ(batch.length, out->array()->length);
    DCHECK_EQ(final_size,
              checked_cast<const ArrayType&>(*string_array).total_values_length());
    return Status::OK();
  }

  // Compute the length of the output for the given position, or -1 if it would be null.
  static int64_t CalculateRowSize(const JoinOptions& options, const ExecBatch& batch,
                                  const int64_t index) {
    const auto num_args = batch.values.size();
    int64_t final_size = 0;
    int64_t num_non_null_args = 0;
    for (size_t i = 0; i < num_args; i++) {
      int64_t element_size = 0;
      bool valid = true;
      if (batch[i].is_scalar()) {
        const Scalar& scalar = *batch[i].scalar();
        valid = scalar.is_valid;
        element_size = UnboxScalar<Type>::Unbox(scalar).size();
      } else {
        const ArrayData& array = *batch[i].array();
        valid = !array.MayHaveNulls() ||
                BitUtil::GetBit(array.buffers[0]->data(), array.offset + index);
        const offset_type* offsets = array.GetValues<offset_type>(1);
        element_size = offsets[index + 1] - offsets[index];
      }
      if (i == num_args - 1) {
        if (!valid) return -1;
        if (num_non_null_args > 1) {
          // Add separator size (only if there were values to join)
          final_size += (num_non_null_args - 1) * element_size;
        }
        break;
      }
      if (!valid) {
        switch (options.null_handling) {
          case JoinOptions::EMIT_NULL:
            return -1;
          case JoinOptions::SKIP:
            continue;
          case JoinOptions::REPLACE:
            element_size = options.null_replacement.size();
            break;
        }
      }
      num_non_null_args++;
      final_size += element_size;
    }
    return final_size;
  }
};

const FunctionDoc binary_join_doc(
    "Join a list of strings together with a `separator` to form a single string",
    ("Insert `separator` between `list` elements, and concatenate them.\n"
     "Any null input and any null `list` element emits a null output.\n"),
    {"list", "separator"});

const FunctionDoc binary_join_element_wise_doc(
    "Join string arguments into one, using the last argument as the separator",
    ("Insert the last argument of `strings` between the rest of the elements, "
     "and concatenate them.\n"
     "Any null separator element emits a null output. Null elements either "
     "emit a null (the default), are skipped, or replaced with a given string.\n"),
    {"*strings"}, "JoinOptions");

const auto kDefaultJoinOptions = JoinOptions::Defaults();

template <typename ListType>
void AddBinaryJoinForListType(ScalarFunction* func) {
  for (const std::shared_ptr<DataType>& ty : BaseBinaryTypes()) {
    auto exec = GenerateTypeAgnosticVarBinaryBase<BinaryJoin, ListType>(*ty);
    auto list_ty = std::make_shared<ListType>(ty);
    DCHECK_OK(func->AddKernel({InputType(list_ty), InputType(ty)}, ty, exec));
  }
}

void AddBinaryJoin(FunctionRegistry* registry) {
  {
    auto func = std::make_shared<ScalarFunction>("binary_join", Arity::Binary(),
                                                 &binary_join_doc);
    AddBinaryJoinForListType<ListType>(func.get());
    AddBinaryJoinForListType<LargeListType>(func.get());
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<ScalarFunction>(
        "binary_join_element_wise", Arity::VarArgs(/*min_args=*/1),
        &binary_join_element_wise_doc, &kDefaultJoinOptions);
    for (const auto& ty : BaseBinaryTypes()) {
      ScalarKernel kernel{KernelSignature::Make({InputType(ty)}, ty, /*is_varargs=*/true),
                          GenerateTypeAgnosticVarBinaryBase<BinaryJoinElementWise>(ty),
                          BinaryJoinElementWiseState::Init};
      kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
      kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
      DCHECK_OK(func->AddKernel(std::move(kernel)));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
}

template <template <typename> class ExecFunctor>
void MakeUnaryStringBatchKernel(
    std::string name, FunctionRegistry* registry, const FunctionDoc* doc,
    MemAllocation::type mem_allocation = MemAllocation::PREALLOCATE) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);
  {
    auto exec_32 = ExecFunctor<StringType>::Exec;
    ScalarKernel kernel{{utf8()}, utf8(), exec_32};
    kernel.mem_allocation = mem_allocation;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  {
    auto exec_64 = ExecFunctor<LargeStringType>::Exec;
    ScalarKernel kernel{{large_utf8()}, large_utf8(), exec_64};
    kernel.mem_allocation = mem_allocation;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

template <template <typename> class ExecFunctor>
void MakeUnaryStringBatchKernelWithState(
    std::string name, FunctionRegistry* registry, const FunctionDoc* doc,
    MemAllocation::type mem_allocation = MemAllocation::PREALLOCATE) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);
  {
    using t32 = ExecFunctor<StringType>;
    ScalarKernel kernel{{utf8()}, utf8(), t32::Exec, t32::State::Init};
    kernel.mem_allocation = mem_allocation;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  {
    using t64 = ExecFunctor<LargeStringType>;
    ScalarKernel kernel{{large_utf8()}, large_utf8(), t64::Exec, t64::State::Init};
    kernel.mem_allocation = mem_allocation;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

#ifdef ARROW_WITH_UTF8PROC

template <template <typename> class Transformer>
void MakeUnaryStringUTF8TransformKernel(std::string name, FunctionRegistry* registry,
                                        const FunctionDoc* doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);
  ArrayKernelExec exec_32 = Transformer<StringType>::Exec;
  ArrayKernelExec exec_64 = Transformer<LargeStringType>::Exec;
  DCHECK_OK(func->AddKernel({utf8()}, utf8(), exec_32));
  DCHECK_OK(func->AddKernel({large_utf8()}, large_utf8(), exec_64));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

#endif

// NOTE: Predicate should only populate 'status' with errors,
//       leave it unmodified to indicate Status::OK()
using StringPredicate =
    std::function<bool(KernelContext*, const uint8_t*, size_t, Status*)>;

template <typename Type>
Status ApplyPredicate(KernelContext* ctx, const ExecBatch& batch,
                      StringPredicate predicate, Datum* out) {
  Status st = Status::OK();
  EnsureLookupTablesFilled();
  if (batch[0].kind() == Datum::ARRAY) {
    const ArrayData& input = *batch[0].array();
    ArrayIterator<Type> input_it(input);
    ArrayData* out_arr = out->mutable_array();
    ::arrow::internal::GenerateBitsUnrolled(
        out_arr->buffers[1]->mutable_data(), out_arr->offset, input.length,
        [&]() -> bool {
          util::string_view val = input_it();
          return predicate(ctx, reinterpret_cast<const uint8_t*>(val.data()), val.size(),
                           &st);
        });
  } else {
    const auto& input = checked_cast<const BaseBinaryScalar&>(*batch[0].scalar());
    if (input.is_valid) {
      bool boolean_result = predicate(ctx, input.value->data(),
                                      static_cast<size_t>(input.value->size()), &st);
      // UTF decoding can lead to issues
      if (st.ok()) {
        out->value = std::make_shared<BooleanScalar>(boolean_result);
      }
    }
  }
  return st;
}

template <typename Predicate>
void AddUnaryStringPredicate(std::string name, FunctionRegistry* registry,
                             const FunctionDoc* doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);
  auto exec_32 = [](KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return ApplyPredicate<StringType>(ctx, batch, Predicate::Call, out);
  };
  auto exec_64 = [](KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return ApplyPredicate<LargeStringType>(ctx, batch, Predicate::Call, out);
  };
  DCHECK_OK(func->AddKernel({utf8()}, boolean(), std::move(exec_32)));
  DCHECK_OK(func->AddKernel({large_utf8()}, boolean(), std::move(exec_64)));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

FunctionDoc StringPredicateDoc(std::string summary, std::string description) {
  return FunctionDoc{std::move(summary), std::move(description), {"strings"}};
}

FunctionDoc StringClassifyDoc(std::string class_summary, std::string class_desc,
                              bool non_empty) {
  std::string summary, description;
  {
    std::stringstream ss;
    ss << "Classify strings as " << class_summary;
    summary = ss.str();
  }
  {
    std::stringstream ss;
    if (non_empty) {
      ss
          << ("For each string in `strings`, emit true iff the string is non-empty\n"
              "and consists only of ");
    } else {
      ss
          << ("For each string in `strings`, emit true iff the string consists only\n"
              "of ");
    }
    ss << class_desc << ".  Null strings emit null.";
    description = ss.str();
  }
  return StringPredicateDoc(std::move(summary), std::move(description));
}

const auto string_is_ascii_doc = StringClassifyDoc("ASCII", "ASCII characters", false);

const auto ascii_is_alnum_doc =
    StringClassifyDoc("ASCII alphanumeric", "alphanumeric ASCII characters", true);
const auto ascii_is_alpha_doc =
    StringClassifyDoc("ASCII alphabetic", "alphabetic ASCII characters", true);
const auto ascii_is_decimal_doc =
    StringClassifyDoc("ASCII decimal", "decimal ASCII characters", true);
const auto ascii_is_lower_doc =
    StringClassifyDoc("ASCII lowercase", "lowercase ASCII characters", true);
const auto ascii_is_printable_doc =
    StringClassifyDoc("ASCII printable", "printable ASCII characters", true);
const auto ascii_is_space_doc =
    StringClassifyDoc("ASCII whitespace", "whitespace ASCII characters", true);
const auto ascii_is_upper_doc =
    StringClassifyDoc("ASCII uppercase", "uppercase ASCII characters", true);

const auto ascii_is_title_doc = StringPredicateDoc(
    "Classify strings as ASCII titlecase",
    ("For each string in `strings`, emit true iff the string is title-cased,\n"
     "i.e. it has at least one cased character, each uppercase character\n"
     "follows a non-cased character, and each lowercase character follows\n"
     "an uppercase character.\n"));

const auto utf8_is_alnum_doc =
    StringClassifyDoc("alphanumeric", "alphanumeric Unicode characters", true);
const auto utf8_is_alpha_doc =
    StringClassifyDoc("alphabetic", "alphabetic Unicode characters", true);
const auto utf8_is_decimal_doc =
    StringClassifyDoc("decimal", "decimal Unicode characters", true);
const auto utf8_is_digit_doc = StringClassifyDoc("digits", "Unicode digits", true);
const auto utf8_is_lower_doc =
    StringClassifyDoc("lowercase", "lowercase Unicode characters", true);
const auto utf8_is_numeric_doc =
    StringClassifyDoc("numeric", "numeric Unicode characters", true);
const auto utf8_is_printable_doc =
    StringClassifyDoc("printable", "printable Unicode characters", true);
const auto utf8_is_space_doc =
    StringClassifyDoc("whitespace", "whitespace Unicode characters", true);
const auto utf8_is_upper_doc =
    StringClassifyDoc("uppercase", "uppercase Unicode characters", true);

const auto utf8_is_title_doc = StringPredicateDoc(
    "Classify strings as titlecase",
    ("For each string in `strings`, emit true iff the string is title-cased,\n"
     "i.e. it has at least one cased character, each uppercase character\n"
     "follows a non-cased character, and each lowercase character follows\n"
     "an uppercase character.\n"));

const FunctionDoc ascii_upper_doc(
    "Transform ASCII input to uppercase",
    ("For each string in `strings`, return an uppercase version.\n\n"
     "This function assumes the input is fully ASCII.  It it may contain\n"
     "non-ASCII characters, use \"utf8_upper\" instead."),
    {"strings"});

const FunctionDoc ascii_lower_doc(
    "Transform ASCII input to lowercase",
    ("For each string in `strings`, return a lowercase version.\n\n"
     "This function assumes the input is fully ASCII.  If it may contain\n"
     "non-ASCII characters, use \"utf8_lower\" instead."),
    {"strings"});

const FunctionDoc utf8_upper_doc(
    "Transform input to uppercase",
    ("For each string in `strings`, return an uppercase version."), {"strings"});

const FunctionDoc utf8_lower_doc(
    "Transform input to lowercase",
    ("For each string in `strings`, return a lowercase version."), {"strings"});

const FunctionDoc ascii_reverse_doc(
    "Reverse ASCII input",
    ("For each ASCII string in `strings`, return a reversed version.\n\n"
     "This function assumes the input is fully ASCII.  If it may contain\n"
     "non-ASCII characters, use \"utf8_reverse\" instead."),
    {"strings"});

const FunctionDoc utf8_reverse_doc(
    "Reverse utf8 input",
    ("For each utf8 string in `strings`, return a reversed version.\n\n"
     "This function operates on codepoints/UTF-8 code units, not grapheme\n"
     "clusters. Hence, it will not correctly reverse grapheme clusters\n"
     "composed of multiple codepoints."),
    {"strings"});

}  // namespace

void RegisterScalarStringAscii(FunctionRegistry* registry) {
  // ascii_upper and ascii_lower are able to reuse the original offsets buffer,
  // so don't preallocate them in the output.
  MakeUnaryStringBatchKernel<AsciiUpper>("ascii_upper", registry, &ascii_upper_doc,
                                         MemAllocation::NO_PREALLOCATE);
  MakeUnaryStringBatchKernel<AsciiLower>("ascii_lower", registry, &ascii_lower_doc,
                                         MemAllocation::NO_PREALLOCATE);
  MakeUnaryStringBatchKernel<AsciiTrimWhitespace>("ascii_trim_whitespace", registry,
                                                  &ascii_trim_whitespace_doc);
  MakeUnaryStringBatchKernel<AsciiLTrimWhitespace>("ascii_ltrim_whitespace", registry,
                                                   &ascii_ltrim_whitespace_doc);
  MakeUnaryStringBatchKernel<AsciiRTrimWhitespace>("ascii_rtrim_whitespace", registry,
                                                   &ascii_rtrim_whitespace_doc);
  MakeUnaryStringBatchKernel<AsciiReverse>("ascii_reverse", registry, &ascii_reverse_doc);
  MakeUnaryStringBatchKernel<Utf8Reverse>("utf8_reverse", registry, &utf8_reverse_doc);

  MakeUnaryStringBatchKernelWithState<AsciiCenter>("ascii_center", registry,
                                                   &ascii_center_doc);
  MakeUnaryStringBatchKernelWithState<AsciiLPad>("ascii_lpad", registry, &ascii_lpad_doc);
  MakeUnaryStringBatchKernelWithState<AsciiRPad>("ascii_rpad", registry, &ascii_rpad_doc);
  MakeUnaryStringBatchKernelWithState<Utf8Center>("utf8_center", registry,
                                                  &utf8_center_doc);
  MakeUnaryStringBatchKernelWithState<Utf8LPad>("utf8_lpad", registry, &utf8_lpad_doc);
  MakeUnaryStringBatchKernelWithState<Utf8RPad>("utf8_rpad", registry, &utf8_rpad_doc);

  MakeUnaryStringBatchKernelWithState<AsciiTrim>("ascii_trim", registry, &ascii_trim_doc);
  MakeUnaryStringBatchKernelWithState<AsciiLTrim>("ascii_ltrim", registry,
                                                  &ascii_ltrim_doc);
  MakeUnaryStringBatchKernelWithState<AsciiRTrim>("ascii_rtrim", registry,
                                                  &ascii_rtrim_doc);

  AddUnaryStringPredicate<IsAscii>("string_is_ascii", registry, &string_is_ascii_doc);

  AddUnaryStringPredicate<IsAlphaNumericAscii>("ascii_is_alnum", registry,
                                               &ascii_is_alnum_doc);
  AddUnaryStringPredicate<IsAlphaAscii>("ascii_is_alpha", registry, &ascii_is_alpha_doc);
  AddUnaryStringPredicate<IsDecimalAscii>("ascii_is_decimal", registry,
                                          &ascii_is_decimal_doc);
  // no is_digit for ascii, since it is the same as is_decimal
  AddUnaryStringPredicate<IsLowerAscii>("ascii_is_lower", registry, &ascii_is_lower_doc);
  // no is_numeric for ascii, since it is the same as is_decimal
  AddUnaryStringPredicate<IsPrintableAscii>("ascii_is_printable", registry,
                                            &ascii_is_printable_doc);
  AddUnaryStringPredicate<IsSpaceAscii>("ascii_is_space", registry, &ascii_is_space_doc);
  AddUnaryStringPredicate<IsTitleAscii>("ascii_is_title", registry, &ascii_is_title_doc);
  AddUnaryStringPredicate<IsUpperAscii>("ascii_is_upper", registry, &ascii_is_upper_doc);

#ifdef ARROW_WITH_UTF8PROC
  MakeUnaryStringUTF8TransformKernel<UTF8Upper>("utf8_upper", registry, &utf8_upper_doc);
  MakeUnaryStringUTF8TransformKernel<UTF8Lower>("utf8_lower", registry, &utf8_lower_doc);
  MakeUnaryStringBatchKernel<UTF8TrimWhitespace>("utf8_trim_whitespace", registry,
                                                 &utf8_trim_whitespace_doc);
  MakeUnaryStringBatchKernel<UTF8LTrimWhitespace>("utf8_ltrim_whitespace", registry,
                                                  &utf8_ltrim_whitespace_doc);
  MakeUnaryStringBatchKernel<UTF8RTrimWhitespace>("utf8_rtrim_whitespace", registry,
                                                  &utf8_rtrim_whitespace_doc);
  MakeUnaryStringBatchKernelWithState<UTF8Trim>("utf8_trim", registry, &utf8_trim_doc);
  MakeUnaryStringBatchKernelWithState<UTF8LTrim>("utf8_ltrim", registry, &utf8_ltrim_doc);
  MakeUnaryStringBatchKernelWithState<UTF8RTrim>("utf8_rtrim", registry, &utf8_rtrim_doc);

  AddUnaryStringPredicate<IsAlphaNumericUnicode>("utf8_is_alnum", registry,
                                                 &utf8_is_alnum_doc);
  AddUnaryStringPredicate<IsAlphaUnicode>("utf8_is_alpha", registry, &utf8_is_alpha_doc);
  AddUnaryStringPredicate<IsDecimalUnicode>("utf8_is_decimal", registry,
                                            &utf8_is_decimal_doc);
  AddUnaryStringPredicate<IsDigitUnicode>("utf8_is_digit", registry, &utf8_is_digit_doc);
  AddUnaryStringPredicate<IsLowerUnicode>("utf8_is_lower", registry, &utf8_is_lower_doc);
  AddUnaryStringPredicate<IsNumericUnicode>("utf8_is_numeric", registry,
                                            &utf8_is_numeric_doc);
  AddUnaryStringPredicate<IsPrintableUnicode>("utf8_is_printable", registry,
                                              &utf8_is_printable_doc);
  AddUnaryStringPredicate<IsSpaceUnicode>("utf8_is_space", registry, &utf8_is_space_doc);
  AddUnaryStringPredicate<IsTitleUnicode>("utf8_is_title", registry, &utf8_is_title_doc);
  AddUnaryStringPredicate<IsUpperUnicode>("utf8_is_upper", registry, &utf8_is_upper_doc);
#endif

  AddBinaryLength(registry);
  AddUtf8Length(registry);
  AddMatchSubstring(registry);
  AddFindSubstring(registry);
  AddCountSubstring(registry);
  MakeUnaryStringBatchKernelWithState<ReplaceSubStringPlain>(
      "replace_substring", registry, &replace_substring_doc,
      MemAllocation::NO_PREALLOCATE);
#ifdef ARROW_WITH_RE2
  MakeUnaryStringBatchKernelWithState<ReplaceSubStringRegex>(
      "replace_substring_regex", registry, &replace_substring_regex_doc,
      MemAllocation::NO_PREALLOCATE);
  AddExtractRegex(registry);
#endif
  AddReplaceSlice(registry);
  AddSlice(registry);
  AddSplit(registry);
  AddStrptime(registry);
  AddBinaryJoin(registry);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
