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

#include "google/protobuf/message.h"

#include <iostream>
#include <stack>

#include "y_absl/base/casts.h"
#include "y_absl/container/flat_hash_map.h"
#include "y_absl/container/flat_hash_set.h"
#include "y_absl/log/absl_check.h"
#include "y_absl/log/absl_log.h"
#include "y_absl/strings/str_join.h"
#include "y_absl/strings/string_view.h"
#include "y_absl/synchronization/mutex.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/generated_message_reflection.h"
#include "google/protobuf/generated_message_tctable_impl.h"
#include "google/protobuf/generated_message_util.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/map_field.h"
#include "google/protobuf/map_field_inl.h"
#include "google/protobuf/parse_context.h"
#include "google/protobuf/reflection_internal.h"
#include "google/protobuf/reflection_ops.h"
#include "google/protobuf/unknown_field_set.h"
#include "google/protobuf/wire_format.h"
#include "google/protobuf/wire_format_lite.h"


// Must be included last.
#include "google/protobuf/port_def.inc"

namespace google {
namespace protobuf {
namespace internal {

// TODO(gerbens) make this factorized better. This should not have to hop
// to reflection. Currently uses GeneratedMessageReflection and thus is
// defined in generated_message_reflection.cc
void RegisterFileLevelMetadata(const DescriptorTable* descriptor_table);

}  // namespace internal

using internal::DownCast;
using internal::ReflectionOps;
using internal::WireFormat;
using internal::WireFormatLite;

void Message::MergeFrom(const Message& from) {
  auto* class_to = GetClassData();
  auto* class_from = from.GetClassData();
  auto* merge_to_from = class_to ? class_to->merge_to_from : nullptr;
  if (class_to == nullptr || class_to != class_from) {
    merge_to_from = [](Message& to, const Message& from) {
      ReflectionOps::Merge(from, &to);
    };
  }
  merge_to_from(*this, from);
}

void Message::CheckTypeAndMergeFrom(const MessageLite& other) {
  MergeFrom(*DownCast<const Message*>(&other));
}

void Message::CopyFrom(const Message& from) {
  if (&from == this) return;

  auto* class_to = GetClassData();
  auto* class_from = from.GetClassData();
  auto* copy_to_from = class_to ? class_to->copy_to_from : nullptr;

  if (class_to == nullptr || class_to != class_from) {
    const Descriptor* descriptor = GetDescriptor();
    Y_ABSL_CHECK_EQ(from.GetDescriptor(), descriptor)
        << ": Tried to copy from a message with a different type. "
           "to: "
        << descriptor->full_name()
        << ", "
           "from: "
        << from.GetDescriptor()->full_name();
    copy_to_from = [](Message& to, const Message& from) {
      ReflectionOps::Copy(from, &to);
    };
  }
  copy_to_from(*this, from);
}

void Message::CopyWithSourceCheck(Message& to, const Message& from) {
  // Fail if "from" is a descendant of "to" as such copy is not allowed.
  Y_ABSL_DCHECK(!internal::IsDescendant(to, from))
      << "Source of CopyFrom cannot be a descendant of the target.";

  to.Clear();
  to.GetClassData()->merge_to_from(to, from);
}

TProtoStringType Message::GetTypeName() const {
  return GetDescriptor()->full_name();
}

void Message::Clear() { ReflectionOps::Clear(this); }

bool Message::IsInitialized() const {
  return ReflectionOps::IsInitialized(*this);
}

void Message::FindInitializationErrors(std::vector<TProtoStringType>* errors) const {
  return ReflectionOps::FindInitializationErrors(*this, "", errors);
}

TProtoStringType Message::InitializationErrorString() const {
  std::vector<TProtoStringType> errors;
  FindInitializationErrors(&errors);
  return y_absl::StrJoin(errors, ", ");
}

void Message::CheckInitialized() const {
  Y_ABSL_CHECK(IsInitialized())
      << "Message of type \"" << GetDescriptor()->full_name()
      << "\" is missing required fields: " << InitializationErrorString();
}

void Message::DiscardUnknownFields() {
  return ReflectionOps::DiscardUnknownFields(this);
}

const char* Message::_InternalParse(const char* ptr,
                                    internal::ParseContext* ctx) {
#if defined(PROTOBUF_USE_TABLE_PARSER_ON_REFLECTION)
  auto meta = GetMetadata();
  ptr = internal::TcParser::ParseLoop(this, ptr, ctx,
                                      meta.reflection->GetTcParseTable());

  return ptr;
#else
  return WireFormat::_InternalParse(this, ptr, ctx);
#endif
}

uint8_t* Message::_InternalSerialize(uint8_t* target,
                                     io::EpsCopyOutputStream* stream) const {
  return WireFormat::_InternalSerialize(*this, target, stream);
}

// Yandex-specific
void Message::PrintJSON(IOutputStream& out) const {
  out << "(Something went wrong: no PrintJSON() override provided - are you using a non-styleguided .pb.h?)";
}

bool Message::ParseFromArcadiaStream(IInputStream* input) {
  bool res = false;
  io::TInputStreamProxy proxy(input);
  {
    io::CopyingInputStreamAdaptor stream(&proxy);
    res = ParseFromZeroCopyStream(&stream);
  }
  return res && !proxy.HasError();
}

bool Message::ParsePartialFromArcadiaStream(IInputStream* input) {
  bool res = false;
  io::TInputStreamProxy proxy(input);
  {
    io::CopyingInputStreamAdaptor stream(&proxy);
    res = ParsePartialFromZeroCopyStream(&stream);
  }
  return res && !proxy.HasError();
}

bool Message::SerializeToArcadiaStream(IOutputStream* output) const {
  bool res = false;
  io::TOutputStreamProxy proxy(output);
  {
    io::CopyingOutputStreamAdaptor stream(&proxy);
    res = SerializeToZeroCopyStream(&stream);
  }
  return res && !proxy.HasError();
}

bool Message::SerializePartialToArcadiaStream(IOutputStream* output) const {
  bool res = false;
  io::TOutputStreamProxy proxy(output);
  {
    io::CopyingOutputStreamAdaptor stream(&proxy);
    res = SerializePartialToZeroCopyStream(&stream);
  }
  return res && !proxy.HasError();
}
// End of Yandex-specific

size_t Message::ByteSizeLong() const {
  size_t size = WireFormat::ByteSize(*this);
  SetCachedSize(internal::ToCachedSize(size));
  return size;
}

void Message::SetCachedSize(int /* size */) const {
  Y_ABSL_LOG(FATAL) << "Message class \"" << GetDescriptor()->full_name()
                  << "\" implements neither SetCachedSize() nor ByteSize().  "
                     "Must implement one or the other.";
}

size_t Message::ComputeUnknownFieldsSize(
    size_t total_size, internal::CachedSize* cached_size) const {
  total_size += WireFormat::ComputeUnknownFieldsSize(
      _internal_metadata_.unknown_fields<UnknownFieldSet>(
          UnknownFieldSet::default_instance));
  cached_size->Set(internal::ToCachedSize(total_size));
  return total_size;
}

size_t Message::MaybeComputeUnknownFieldsSize(
    size_t total_size, internal::CachedSize* cached_size) const {
  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    return ComputeUnknownFieldsSize(total_size, cached_size);
  }
  cached_size->Set(internal::ToCachedSize(total_size));
  return total_size;
}

size_t Message::SpaceUsedLong() const {
  return GetReflection()->SpaceUsedLong(*this);
}

arc_ui64 Message::GetInvariantPerBuild(arc_ui64 salt) {
  return salt;
}

namespace internal {
void* CreateSplitMessageGeneric(Arena* arena, const void* default_split,
                                size_t size, const void* message,
                                const void* default_message) {
  Y_ABSL_DCHECK_NE(message, default_message);
  void* split =
      (arena == nullptr) ? ::operator new(size) : arena->AllocateAligned(size);
  memcpy(split, default_split, size);
  return split;
}
}  // namespace internal

// =============================================================================
// MessageFactory

MessageFactory::~MessageFactory() {}

namespace {

class GeneratedMessageFactory final : public MessageFactory {
 public:
  static GeneratedMessageFactory* singleton();

  void RegisterFile(const google::protobuf::internal::DescriptorTable* table);
  void RegisterType(const Descriptor* descriptor, const Message* prototype);

  // implements MessageFactory ---------------------------------------
  const Message* GetPrototype(const Descriptor* type) override;

 private:
  const Message* FindInTypeMap(const Descriptor* type)
      Y_ABSL_SHARED_LOCKS_REQUIRED(mutex_)
  {
    auto it = type_map_.find(type);
    if (it == type_map_.end()) return nullptr;
    return it->second;
  }

  const google::protobuf::internal::DescriptorTable* FindInFileMap(
      y_absl::string_view name) {
    auto it = files_.find(name);
    if (it == files_.end()) return nullptr;
    return *it;
  }

  struct DescriptorByNameHash {
    using is_transparent = void;
    size_t operator()(const google::protobuf::internal::DescriptorTable* t) const {
      return y_absl::HashOf(y_absl::string_view{t->filename});
    }

    size_t operator()(y_absl::string_view name) const {
      return y_absl::HashOf(name);
    }
  };
  struct DescriptorByNameEq {
    using is_transparent = void;
    bool operator()(const google::protobuf::internal::DescriptorTable* lhs,
                    const google::protobuf::internal::DescriptorTable* rhs) const {
      return lhs == rhs || (*this)(lhs->filename, rhs->filename);
    }
    bool operator()(y_absl::string_view lhs,
                    const google::protobuf::internal::DescriptorTable* rhs) const {
      return (*this)(lhs, rhs->filename);
    }
    bool operator()(const google::protobuf::internal::DescriptorTable* lhs,
                    y_absl::string_view rhs) const {
      return (*this)(lhs->filename, rhs);
    }
    bool operator()(y_absl::string_view lhs, y_absl::string_view rhs) const {
      return lhs == rhs;
    }
  };

  // Only written at static init time, so does not require locking.
  y_absl::flat_hash_set<const google::protobuf::internal::DescriptorTable*,
                      DescriptorByNameHash, DescriptorByNameEq>
      files_;

  y_absl::Mutex mutex_;
  y_absl::flat_hash_map<const Descriptor*, const Message*> type_map_
      Y_ABSL_GUARDED_BY(mutex_);
};

GeneratedMessageFactory* GeneratedMessageFactory::singleton() {
  static auto instance =
      internal::OnShutdownDelete(new GeneratedMessageFactory);
  return instance;
}

void GeneratedMessageFactory::RegisterFile(
    const google::protobuf::internal::DescriptorTable* table) {
  if (!files_.insert(table).second) {
    Y_ABSL_LOG(FATAL) << "File is already registered: " << table->filename;
  }
}

void GeneratedMessageFactory::RegisterType(const Descriptor* descriptor,
                                           const Message* prototype) {
  Y_ABSL_DCHECK_EQ(descriptor->file()->pool(), DescriptorPool::generated_pool())
      << "Tried to register a non-generated type with the generated "
         "type registry.";

  // This should only be called as a result of calling a file registration
  // function during GetPrototype(), in which case we already have locked
  // the mutex.
  mutex_.AssertHeld();
  if (!type_map_.try_emplace(descriptor, prototype).second) {
    Y_ABSL_DLOG(FATAL) << "Type is already registered: "
                     << descriptor->full_name();
  }
}


const Message* GeneratedMessageFactory::GetPrototype(const Descriptor* type) {
  {
    y_absl::ReaderMutexLock lock(&mutex_);
    const Message* result = FindInTypeMap(type);
    if (result != nullptr) return result;
  }

  // If the type is not in the generated pool, then we can't possibly handle
  // it.
  if (type->file()->pool() != DescriptorPool::generated_pool()) return nullptr;

  // Apparently the file hasn't been registered yet.  Let's do that now.
  const internal::DescriptorTable* registration_data =
      FindInFileMap(type->file()->name());
  if (registration_data == nullptr) {
    Y_ABSL_DLOG(FATAL) << "File appears to be in generated pool but wasn't "
                        "registered: "
                     << type->file()->name();
    return nullptr;
  }

  y_absl::WriterMutexLock lock(&mutex_);

  // Check if another thread preempted us.
  const Message* result = FindInTypeMap(type);
  if (result == nullptr) {
    // Nope.  OK, register everything.
    internal::RegisterFileLevelMetadata(registration_data);
    // Should be here now.
    result = FindInTypeMap(type);
  }

  if (result == nullptr) {
    Y_ABSL_DLOG(FATAL) << "Type appears to be in generated pool but wasn't "
                     << "registered: " << type->full_name();
  }

  return result;
}

}  // namespace

MessageFactory* MessageFactory::generated_factory() {
  return GeneratedMessageFactory::singleton();
}

void MessageFactory::InternalRegisterGeneratedFile(
    const google::protobuf::internal::DescriptorTable* table) {
  GeneratedMessageFactory::singleton()->RegisterFile(table);
}

void MessageFactory::InternalRegisterGeneratedMessage(
    const Descriptor* descriptor, const Message* prototype) {
  GeneratedMessageFactory::singleton()->RegisterType(descriptor, prototype);
}


namespace {
template <typename T>
T* GetSingleton() {
  static T singleton;
  return &singleton;
}
}  // namespace

const internal::RepeatedFieldAccessor* Reflection::RepeatedFieldAccessor(
    const FieldDescriptor* field) const {
  Y_ABSL_CHECK(field->is_repeated());
  switch (field->cpp_type()) {
#define HANDLE_PRIMITIVE_TYPE(TYPE, type) \
  case FieldDescriptor::CPPTYPE_##TYPE:   \
    return GetSingleton<internal::RepeatedFieldPrimitiveAccessor<type> >();
    HANDLE_PRIMITIVE_TYPE(INT32, arc_i32)
    HANDLE_PRIMITIVE_TYPE(UINT32, arc_ui32)
    HANDLE_PRIMITIVE_TYPE(INT64, arc_i64)
    HANDLE_PRIMITIVE_TYPE(UINT64, arc_ui64)
    HANDLE_PRIMITIVE_TYPE(FLOAT, float)
    HANDLE_PRIMITIVE_TYPE(DOUBLE, double)
    HANDLE_PRIMITIVE_TYPE(BOOL, bool)
    HANDLE_PRIMITIVE_TYPE(ENUM, arc_i32)
#undef HANDLE_PRIMITIVE_TYPE
    case FieldDescriptor::CPPTYPE_STRING:
      switch (field->options().ctype()) {
        default:
        case FieldOptions::STRING:
          return GetSingleton<internal::RepeatedPtrFieldStringAccessor>();
      }
      break;
    case FieldDescriptor::CPPTYPE_MESSAGE:
      if (field->is_map()) {
        return GetSingleton<internal::MapFieldAccessor>();
      } else {
        return GetSingleton<internal::RepeatedPtrFieldMessageAccessor>();
      }
  }
  Y_ABSL_LOG(FATAL) << "Should not reach here.";
  return nullptr;
}

namespace internal {
template <>
#if defined(_MSC_VER) && (_MSC_VER >= 1800)
// Note: force noinline to workaround MSVC compiler bug with /Zc:inline, issue
// #240
PROTOBUF_NOINLINE
#endif
    Message*
    GenericTypeHandler<Message>::NewFromPrototype(const Message* prototype,
                                                  Arena* arena) {
  return prototype->New(arena);
}
template <>
#if defined(_MSC_VER) && (_MSC_VER >= 1800)
// Note: force noinline to workaround MSVC compiler bug with /Zc:inline, issue
// #240
PROTOBUF_NOINLINE
#endif
    Arena*
    GenericTypeHandler<Message>::GetOwningArena(Message* value) {
  return value->GetOwningArena();
}

template void InternalMetadata::DoClear<UnknownFieldSet>();
template void InternalMetadata::DoMergeFrom<UnknownFieldSet>(
    const UnknownFieldSet& other);
template void InternalMetadata::DoSwap<UnknownFieldSet>(UnknownFieldSet* other);
template Arena* InternalMetadata::DeleteOutOfLineHelper<UnknownFieldSet>();
template UnknownFieldSet*
InternalMetadata::mutable_unknown_fields_slow<UnknownFieldSet>();

}  // namespace internal

}  // namespace protobuf
}  // namespace google

#include "google/protobuf/port_undef.inc"
