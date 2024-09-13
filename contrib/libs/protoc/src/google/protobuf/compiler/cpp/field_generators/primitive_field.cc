// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// Author: kenton@google.com (Kenton Varda)
//  Based on original Protocol Buffers design by
//  Sanjay Ghemawat, Jeff Dean, and others.

#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "y_absl/container/flat_hash_map.h"
#include "y_absl/log/absl_log.h"
#include "y_absl/memory/memory.h"
#include "y_absl/types/optional.h"
#include "google/protobuf/compiler/cpp/field_generators/generators.h"
#include "google/protobuf/compiler/cpp/helpers.h"
#include "google/protobuf/compiler/cpp/options.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/io/printer.h"
#include "google/protobuf/wire_format.h"

namespace google {
namespace protobuf {
namespace compiler {
namespace cpp {
namespace {
using ::google::protobuf::internal::WireFormat;
using ::google::protobuf::internal::WireFormatLite;
using Sub = ::google::protobuf::io::Printer::Sub;
using Annotation = ::google::protobuf::GeneratedCodeInfo::Annotation;

// For encodings with fixed sizes, returns that size in bytes.
y_absl::optional<size_t> FixedSize(FieldDescriptor::Type type) {
  switch (type) {
    case FieldDescriptor::TYPE_INT32:
    case FieldDescriptor::TYPE_INT64:
    case FieldDescriptor::TYPE_UINT32:
    case FieldDescriptor::TYPE_UINT64:
    case FieldDescriptor::TYPE_SINT32:
    case FieldDescriptor::TYPE_SINT64:
    case FieldDescriptor::TYPE_ENUM:
    case FieldDescriptor::TYPE_STRING:
    case FieldDescriptor::TYPE_BYTES:
    case FieldDescriptor::TYPE_GROUP:
    case FieldDescriptor::TYPE_MESSAGE:
      return y_absl::nullopt;

    case FieldDescriptor::TYPE_FIXED32:
      return WireFormatLite::kFixed32Size;
    case FieldDescriptor::TYPE_FIXED64:
      return WireFormatLite::kFixed64Size;
    case FieldDescriptor::TYPE_SFIXED32:
      return WireFormatLite::kSFixed32Size;
    case FieldDescriptor::TYPE_SFIXED64:
      return WireFormatLite::kSFixed64Size;
    case FieldDescriptor::TYPE_FLOAT:
      return WireFormatLite::kFloatSize;
    case FieldDescriptor::TYPE_DOUBLE:
      return WireFormatLite::kDoubleSize;
    case FieldDescriptor::TYPE_BOOL:
      return WireFormatLite::kBoolSize;

      // No default because we want the compiler to complain if any new
      // types are added.
  }

  Y_ABSL_LOG(FATAL) << "Can't get here.";
  return y_absl::nullopt;
}

std::vector<Sub> Vars(const FieldDescriptor* field, const Options& options) {
  bool cold = ShouldSplit(field, options);
  return {
      {"Type", PrimitiveTypeName(options, field->cpp_type())},
      {"kDefault", DefaultValue(options, field)},
      {"_field_cached_byte_size_", MakeVarintCachedSizeFieldName(field, cold)},
  };
}

class SingularPrimitive final : public FieldGeneratorBase {
 public:
  SingularPrimitive(const FieldDescriptor* field, const Options& opts)
      : FieldGeneratorBase(field, opts),
        field_(field),
        opts_(&opts),
        is_oneof_(field_->real_containing_oneof() != nullptr) {}
  ~SingularPrimitive() override = default;

  std::vector<Sub> MakeVars() const override { return Vars(field_, *opts_); }

  void GeneratePrivateMembers(io::Printer* p) const override {
    p->Emit(R"cc(
      $Type$ $name$_;
    )cc");
  }

  void GenerateClearingCode(io::Printer* p) const override {
    p->Emit(R"cc(
      $field_$ = $kDefault$;
    )cc");
  }

  void GenerateMergingCode(io::Printer* p) const override {
    p->Emit(R"cc(
      _this->_internal_set_$name$(from._internal_$name$());
    )cc");
  }

  void GenerateSwappingCode(io::Printer* p) const override {
    if (is_oneof_) {
      // Don't print any swapping code. Swapping the union will swap this field.
      return;
    }

    p->Emit(R"cc(
      //~ A `using std::swap;` is already present in this function.
      swap($field_$, other->$field_$);
    )cc");
  }

  void GenerateConstructorCode(io::Printer* p) const override {
    if (!is_oneof_) {
      return;
    }

    p->Emit(R"cc(
      $pkg$::_$Msg$_default_instance_.$field_$ = $kDefault$;
    )cc");
  }

  void GenerateCopyConstructorCode(io::Printer* p) const override {
    p->Emit(R"cc(
      _this->$field_$ = from.$field_$;
    )cc");
  }

  void GenerateConstexprAggregateInitializer(io::Printer* p) const override {
    p->Emit(R"cc(
      /*decltype($field_$)*/ $kDefault$
    )cc");
  }

  void GenerateAggregateInitializer(io::Printer* p) const override {
    p->Emit(R"cc(
      decltype($field_$) { $kDefault$ }
    )cc");
  }

  void GenerateCopyAggregateInitializer(io::Printer* p) const override {
    p->Emit(R"cc(
      decltype($field_$) {}
    )cc");
  }

  void GenerateAccessorDeclarations(io::Printer* p) const override;
  void GenerateInlineAccessorDefinitions(io::Printer* p) const override;
  void GenerateSerializeWithCachedSizesToArray(io::Printer* p) const override;
  void GenerateByteSize(io::Printer* p) const override;

 private:
  const FieldDescriptor* field_;
  const Options* opts_;
  bool is_oneof_;
};

void SingularPrimitive::GenerateAccessorDeclarations(io::Printer* p) const {
  p->Emit(
      {
          Sub("name", p->LookupVar("name")).AnnotatedAs(field_),
          Sub("set_name", y_absl::StrCat("set_", p->LookupVar("name")))
              .AnnotatedAs(field_),
          Sub("_internal_name",
              y_absl::StrCat("_internal_", p->LookupVar("name")))
              .AnnotatedAs(field_),
          Sub("_internal_set_name",
              y_absl::StrCat("_internal_set_", p->LookupVar("name")))
              .AnnotatedAs(field_),
      },
      R"cc(
        $DEPRECATED$ $Type$ $name$() const;
        $DEPRECATED$ void $set_name$($Type$ value);

        private:
        $Type$ $_internal_name$() const;
        void $_internal_set_name$($Type$ value);

        public:
      )cc");
}

void SingularPrimitive::GenerateInlineAccessorDefinitions(
    io::Printer* p) const {
  p->Emit(R"cc(
    inline $Type$ $Msg$::$name$() const {
      $annotate_get$;
      // @@protoc_insertion_point(field_get:$pkg.Msg.field$)
      return _internal_$name$();
    }
    inline void $Msg$::set_$name$($Type$ value) {
      $PrepareSplitMessageForWrite$;
      _internal_set_$name$(value);
      $annotate_set$;
      // @@protoc_insertion_point(field_set:$pkg.Msg.field$)
    }
  )cc");

  if (is_oneof_) {
    p->Emit(R"cc(
      inline $Type$ $Msg$::_internal_$name$() const {
        if ($has_field$) {
          return $field_$;
        }
        return $kDefault$;
      }
      inline void $Msg$::_internal_set_$name$($Type$ value) {
        if ($not_has_field$) {
          clear_$oneof_name$();
          set_has_$name$();
        }
        $field_$ = value;
      }
    )cc");
  } else {
    p->Emit(R"cc(
      inline $Type$ $Msg$::_internal_$name$() const {
        return $field_$;
      }
      inline void $Msg$::_internal_set_$name$($Type$ value) {
        $set_hasbit$;
        $field_$ = value;
      }
    )cc");
  }
}

void SingularPrimitive::GenerateSerializeWithCachedSizesToArray(
    io::Printer* p) const {
  p->Emit(R"cc(
    target = stream->EnsureSpace(target);
    target = ::_pbi::WireFormatLite::Write$DeclaredType$ToArray(
        $number$, this->_internal_$name$(), target);
  )cc");
}

void SingularPrimitive::GenerateByteSize(io::Printer* p) const {
  size_t tag_size = WireFormat::TagSize(field_->number(), field_->type());

  auto fixed_size = FixedSize(field_->type());
  if (fixed_size.has_value()) {
    p->Emit({{"kFixedBytes", tag_size + *fixed_size}}, R"cc(
      total_size += $kFixedBytes$;
    )cc");
    return;
  }

  // Adding one is very common and it turns out it can be done for
  // free inside of WireFormatLite, so we can save an instruction here.
  if (tag_size == 1) {
    p->Emit(R"cc(
      total_size += ::_pbi::WireFormatLite::$DeclaredType$SizePlusOne(
          this->_internal_$name$());
    )cc");
    return;
  }

  p->Emit(R"cc(
    total_size += $kTagBytes$ + ::_pbi::WireFormatLite::$DeclaredType$Size(
                                    this->_internal_$name$());
  )cc");
}

class RepeatedPrimitive final : public FieldGeneratorBase {
 public:
  RepeatedPrimitive(const FieldDescriptor* field, const Options& opts)
      : FieldGeneratorBase(field, opts), field_(field), opts_(&opts) {}
  ~RepeatedPrimitive() override = default;

  std::vector<Sub> MakeVars() const override { return Vars(field_, *opts_); }

  void GenerateClearingCode(io::Printer* p) const override {
    p->Emit(R"cc(
      $field_$.Clear();
    )cc");
  }

  void GenerateMergingCode(io::Printer* p) const override {
    p->Emit(R"cc(
      _this->$field_$.MergeFrom(from.$field_$);
    )cc");
  }

  void GenerateSwappingCode(io::Printer* p) const override {
    p->Emit(R"cc(
      $field_$.InternalSwap(&other->$field_$);
    )cc");
  }

  void GenerateDestructorCode(io::Printer* p) const override {
    p->Emit(R"cc(
      $field_$.~RepeatedField();
    )cc");
  }

  void GenerateConstructorCode(io::Printer* p) const override {}

  void GenerateCopyConstructorCode(io::Printer* p) const override {
    Y_ABSL_CHECK(!ShouldSplit(field_, *opts_));
  }

  void GenerateConstexprAggregateInitializer(io::Printer* p) const override {
    p->Emit(R"cc(
      /*decltype($field_$)*/ {}
    )cc");
    InitCachedSize(p);
  }

  void GenerateAggregateInitializer(io::Printer* p) const override {
    p->Emit(R"cc(
      decltype($field_$) { arena }
    )cc");
    InitCachedSize(p);
  }

  void GenerateCopyAggregateInitializer(io::Printer* p) const override {
    p->Emit(R"cc(
      decltype($field_$) { from.$field_$ }
    )cc");
    InitCachedSize(p);
  }

  void GeneratePrivateMembers(io::Printer* p) const override;
  void GenerateAccessorDeclarations(io::Printer* p) const override;
  void GenerateInlineAccessorDefinitions(io::Printer* p) const override;
  void GenerateSerializeWithCachedSizesToArray(io::Printer* p) const override;
  void GenerateByteSize(io::Printer* p) const override;

 private:
  bool HasCachedSize() const {
    bool is_packed_varint =
        field_->is_packed() && !FixedSize(field_->type()).has_value();
    return is_packed_varint && HasGeneratedMethods(field_->file(), *opts_);
  }

  void InitCachedSize(io::Printer* p) const {
    if (!HasCachedSize()) return;
    // std::atomic has no move constructor, which prevents explicit aggregate
    // initialization pre-C++17.
    p->Emit(R"(
      ,/* $_field_cached_byte_size_$ = */ { 0 }
    )");
  }

  const FieldDescriptor* field_;
  const Options* opts_;
};

void RepeatedPrimitive::GeneratePrivateMembers(io::Printer* p) const {
  p->Emit(R"cc(
    $pb$::RepeatedField<$Type$> $name$_;
  )cc");

  if (HasCachedSize()) {
    p->Emit({{"_cached_size_", MakeVarintCachedSizeName(field_)}},
            R"cc(
              mutable $pbi$::CachedSize $_cached_size_$;
            )cc");
  }
}

void RepeatedPrimitive::GenerateAccessorDeclarations(io::Printer* p) const {
  p->Emit(
      {
          Sub("name", p->LookupVar("name")).AnnotatedAs(field_),
          Sub("set_name", y_absl::StrCat("set_", p->LookupVar("name")))
              .AnnotatedAs(field_),
          Sub("add_name", y_absl::StrCat("add_", p->LookupVar("name")))
              .AnnotatedAs(field_),
          Sub("mutable_name", y_absl::StrCat("mutable_", p->LookupVar("name")))
              .AnnotatedAs(field_),

          Sub("_internal_name",
              y_absl::StrCat("_internal_", p->LookupVar("name")))
              .AnnotatedAs(field_),
          Sub("_internal_add_name",
              y_absl::StrCat("_internal_add_", p->LookupVar("name")))
              .AnnotatedAs(field_),
          Sub("_internal_mutable_name",
              y_absl::StrCat("_internal_mutable_", p->LookupVar("name")))
              .AnnotatedAs(field_),
      },
      R"cc(
        $DEPRECATED$ $Type$ $name$(int index) const;
        $DEPRECATED$ void $set_name$(int index, $Type$ value);
        $DEPRECATED$ void $add_name$($Type$ value);
        $DEPRECATED$ const $pb$::RepeatedField<$Type$>& $name$() const;
        $DEPRECATED$ $pb$::RepeatedField<$Type$>* $mutable_name$();

        private:
        $Type$ $_internal_name$(int index) const;
        void $_internal_add_name$($Type$ value);
        const $pb$::RepeatedField<$Type$>& $_internal_name$() const;
        $pb$::RepeatedField<$Type$>* $_internal_mutable_name$();

        public:
      )cc");
}

void RepeatedPrimitive::GenerateInlineAccessorDefinitions(
    io::Printer* p) const {
  p->Emit(R"cc(
    inline $Type$ $Msg$::$name$(int index) const {
      $annotate_get$;
      // @@protoc_insertion_point(field_get:$pkg.Msg.field$)
      return _internal_$name$(index);
    }
    inline void $Msg$::set_$name$(int index, $Type$ value) {
      $annotate_set$;
      $field_$.Set(index, value);
      // @@protoc_insertion_point(field_set:$pkg.Msg.field$)
    }
    inline void $Msg$::add_$name$($Type$ value) {
      _internal_add_$name$(value);
      $annotate_add$;
      // @@protoc_insertion_point(field_add:$pkg.Msg.field$)
    }
    inline const $pb$::RepeatedField<$Type$>& $Msg$::$name$() const {
      $annotate_list$;
      // @@protoc_insertion_point(field_list:$pkg.Msg.field$)
      return _internal_$name$();
    }
    inline $pb$::RepeatedField<$Type$>* $Msg$::mutable_$name$() {
      $annotate_mutable_list$;
      // @@protoc_insertion_point(field_mutable_list:$pkg.Msg.field$)
      return _internal_mutable_$name$();
    }

    inline $Type$ $Msg$::_internal_$name$(int index) const {
      return $field_$.Get(index);
    }
    inline void $Msg$::_internal_add_$name$($Type$ value) { $field_$.Add(value); }
    inline const $pb$::RepeatedField<$Type$>& $Msg$::_internal_$name$() const {
      return $field_$;
    }
    inline $pb$::RepeatedField<$Type$>* $Msg$::_internal_mutable_$name$() {
      return &$field_$;
    }
  )cc");
}

void RepeatedPrimitive::GenerateSerializeWithCachedSizesToArray(
    io::Printer* p) const {
  if (!field_->is_packed()) {
    p->Emit(R"cc(
      for (int i = 0, n = this->_internal_$name$_size(); i < n; ++i) {
        target = stream->EnsureSpace(target);
        target = ::_pbi::WireFormatLite::Write$DeclaredType$ToArray(
            $number$, this->_internal_$name$(i), target);
      }
    )cc");
    return;
  }

  if (FixedSize(field_->type()).has_value()) {
    p->Emit(R"cc(
      if (this->_internal_$name$_size() > 0) {
        target = stream->WriteFixedPacked($number$, _internal_$name$(), target);
      }
    )cc");
    return;
  }

  p->Emit(R"cc(
    {
      int byte_size = $_field_cached_byte_size_$.Get();
      if (byte_size > 0) {
        target = stream->Write$DeclaredType$Packed($number$, _internal_$name$(),
                                                   byte_size, target);
      }
    }
  )cc");
}

void RepeatedPrimitive::GenerateByteSize(io::Printer* p) const {
  p->Emit(
      {
          Sub{"data_size",
              [&] {
                auto fixed_size = FixedSize(descriptor_->type());
                if (fixed_size.has_value()) {
                  p->Emit({{"kFixed", *fixed_size}}, R"cc(
                    std::size_t{$kFixed$} *
                        ::_pbi::FromIntSize(this->_internal_$name$_size())
                  )cc");
                } else {
                  p->Emit(R"cc(
                    ::_pbi::WireFormatLite::$DeclaredType$Size(this->$field_$)
                  )cc");
                }
              }}  // Here and below, we need to disable the default ;-chomping
                  // that closure substitutions do.
              .WithSuffix(""),
          {"maybe_cache_data_size",
           [&] {
             if (!HasCachedSize()) return;
             p->Emit(R"cc(
               $_field_cached_byte_size_$.Set(::_pbi::ToCachedSize(data_size));
             )cc");
           }},
          Sub{"tag_size",
              [&] {
                if (field_->is_packed()) {
                  p->Emit(R"cc(
                    data_size == 0
                        ? 0
                        : $kTagBytes$ + ::_pbi::WireFormatLite::Int32Size(
                                            static_cast<arc_i32>(data_size))
                  )cc");
                } else {
                  p->Emit(R"cc(
                    std::size_t{$kTagBytes$} *
                        ::_pbi::FromIntSize(this->_internal_$name$_size());
                  )cc");
                }
              }}
              .WithSuffix(""),
      },
      R"cc(
        {
          std::size_t data_size = $data_size$;
          $maybe_cache_data_size$;
          std::size_t tag_size = $tag_size$;
          total_size += tag_size + data_size;
        }
      )cc");
}
}  // namespace

std::unique_ptr<FieldGeneratorBase> MakeSinguarPrimitiveGenerator(
    const FieldDescriptor* desc, const Options& options,
    MessageSCCAnalyzer* scc) {
  return y_absl::make_unique<SingularPrimitive>(desc, options);
}

std::unique_ptr<FieldGeneratorBase> MakeRepeatedPrimitiveGenerator(
    const FieldDescriptor* desc, const Options& options,
    MessageSCCAnalyzer* scc) {
  return y_absl::make_unique<RepeatedPrimitive>(desc, options);
}

}  // namespace cpp
}  // namespace compiler
}  // namespace protobuf
}  // namespace google
