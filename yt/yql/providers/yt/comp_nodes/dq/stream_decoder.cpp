#include "stream_decoder.h"

#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <flatbuffers/flatbuffers.h>  // IWYU pragma: export

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/extension_type.h>
#include <arrow/io/caching.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/message.h>
#include <arrow/ipc/metadata_internal.h>
#include <arrow/ipc/util.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/sparse_tensor.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/bitmap_ops.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/compression.h>
#include <arrow/util/endian.h>
#include <arrow/util/key_value_metadata.h>
#include <arrow/util/parallel.h>
#include <arrow/util/string.h>
#include <arrow/util/thread_pool.h>
#include <arrow/util/ubsan.h>
#include <arrow/visitor_inline.h>

#include <generated/File.fbs.h>  // IWYU pragma: export
#include <generated/Message.fbs.h>
#include <generated/Schema.fbs.h>
#include <generated/SparseTensor.fbs.h>

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::GetByteWidth;

namespace ipc {

using internal::FileBlock;
using internal::kArrowMagicBytes;

namespace NDqs {
Status MaybeAlignMetadata(std::shared_ptr<Buffer>* metadata) {
  if (reinterpret_cast<uintptr_t>((*metadata)->data()) % 8 != 0) {
    ARROW_ASSIGN_OR_RAISE(*metadata, (*metadata)->CopySlice(0, (*metadata)->size()));
  }
  return Status::OK();
}

Status CheckMetadataAndGetBodyLength(const Buffer& metadata, int64_t* body_length) {
  const flatbuf::Message* fb_message = nullptr;
  RETURN_NOT_OK(internal::VerifyMessage(metadata.data(), metadata.size(), &fb_message));
  *body_length = fb_message->bodyLength();
  if (*body_length < 0) {
    return Status::IOError("Invalid IPC message: negative bodyLength");
  }
  return Status::OK();
}

std::string FormatMessageType(MessageType type) {
  switch (type) {
    case MessageType::SCHEMA:
      return "schema";
    case MessageType::RECORD_BATCH:
      return "record batch";
    case MessageType::DICTIONARY_BATCH:
      return "dictionary";
    case MessageType::TENSOR:
      return "tensor";
    case MessageType::SPARSE_TENSOR:
      return "sparse tensor";
    default:
      break;
  }
  return "unknown";
}

Status WriteMessage(const Buffer& message, const IpcWriteOptions& options,
                    io::OutputStream* file, int32_t* message_length) {
  const int32_t prefix_size = options.write_legacy_ipc_format ? 4 : 8;
  const int32_t flatbuffer_size = static_cast<int32_t>(message.size());

  int32_t padded_message_length = static_cast<int32_t>(
      PaddedLength(flatbuffer_size + prefix_size, options.alignment));

  int32_t padding = padded_message_length - flatbuffer_size - prefix_size;

  *message_length = padded_message_length;

  if (!options.write_legacy_ipc_format) {
    RETURN_NOT_OK(file->Write(&internal::kIpcContinuationToken, sizeof(int32_t)));
  }

  int32_t padded_flatbuffer_size =
      BitUtil::ToLittleEndian(padded_message_length - prefix_size);
  RETURN_NOT_OK(file->Write(&padded_flatbuffer_size, sizeof(int32_t)));

  RETURN_NOT_OK(file->Write(message.data(), flatbuffer_size));
  if (padding > 0) {
    RETURN_NOT_OK(file->Write(kPaddingBytes, padding));
  }

  return Status::OK();
}

static constexpr auto kMessageDecoderNextRequiredSizeInitial = sizeof(int32_t);
static constexpr auto kMessageDecoderNextRequiredSizeMetadataLength = sizeof(int32_t);

class MessageDecoder2::MessageDecoderImpl {
 public:
  explicit MessageDecoderImpl(std::shared_ptr<MessageDecoderListener> listener,
                              State initial_state, int64_t initial_next_required_size,
                              MemoryPool* pool)
      : listener_(std::move(listener)),
        pool_(pool),
        state_(initial_state),
        next_required_size_(initial_next_required_size),
        save_initial_size_(initial_next_required_size),
        chunks_(),
        buffered_size_(0),
        metadata_(nullptr) {}

  Status ConsumeData(const uint8_t* data, int64_t size) {
    if (buffered_size_ == 0) {
      while (size > 0 && size >= next_required_size_) {
        auto used_size = next_required_size_;
        switch (state_) {
          case State::INITIAL:
            RETURN_NOT_OK(ConsumeInitialData(data, next_required_size_));
            break;
          case State::METADATA_LENGTH:
            RETURN_NOT_OK(ConsumeMetadataLengthData(data, next_required_size_));
            break;
          case State::METADATA: {
            auto buffer = std::make_shared<Buffer>(data, next_required_size_);
            RETURN_NOT_OK(ConsumeMetadataBuffer(buffer));
          } break;
          case State::BODY: {
            auto buffer = std::make_shared<Buffer>(data, next_required_size_);
            RETURN_NOT_OK(ConsumeBodyBuffer(buffer));
          } break;
          case State::EOS:
            return Status::OK();
        }
        data += used_size;
        size -= used_size;
      }
    }

    if (size == 0) {
      return Status::OK();
    }

    chunks_.push_back(std::make_shared<Buffer>(data, size));
    buffered_size_ += size;
    return ConsumeChunks();
  }

  Status ConsumeBuffer(std::shared_ptr<Buffer> buffer) {
    if (buffered_size_ == 0) {
      while (buffer->size() >= next_required_size_) {
        auto used_size = next_required_size_;
        switch (state_) {
          case State::INITIAL:
            RETURN_NOT_OK(ConsumeInitialBuffer(buffer));
            break;
          case State::METADATA_LENGTH:
            RETURN_NOT_OK(ConsumeMetadataLengthBuffer(buffer));
            break;
          case State::METADATA:
            if (buffer->size() == next_required_size_) {
              return ConsumeMetadataBuffer(buffer);
            } else {
              auto sliced_buffer = SliceBuffer(buffer, 0, next_required_size_);
              RETURN_NOT_OK(ConsumeMetadataBuffer(sliced_buffer));
            }
            break;
          case State::BODY:
            if (buffer->size() == next_required_size_) {
              return ConsumeBodyBuffer(buffer);
            } else {
              auto sliced_buffer = SliceBuffer(buffer, 0, next_required_size_);
              RETURN_NOT_OK(ConsumeBodyBuffer(sliced_buffer));
            }
            break;
          case State::EOS:
            return Status::OK();
        }
        if (buffer->size() == used_size) {
          return Status::OK();
        }
        buffer = SliceBuffer(buffer, used_size);
      }
    }

    if (buffer->size() == 0) {
      return Status::OK();
    }

    buffered_size_ += buffer->size();
    chunks_.push_back(std::move(buffer));
    return ConsumeChunks();
  }

  int64_t next_required_size() const { return next_required_size_ - buffered_size_; }

  MessageDecoder2::State state() const { return state_; }
  void Reset() {
    state_ = State::INITIAL;
    next_required_size_ = save_initial_size_;
    chunks_.clear();
    buffered_size_ = 0;
    metadata_ = nullptr;
  }

 private:
  Status ConsumeChunks() {
    while (state_ != State::EOS) {
      if (buffered_size_ < next_required_size_) {
        return Status::OK();
      }

      switch (state_) {
        case State::INITIAL:
          RETURN_NOT_OK(ConsumeInitialChunks());
          break;
        case State::METADATA_LENGTH:
          RETURN_NOT_OK(ConsumeMetadataLengthChunks());
          break;
        case State::METADATA:
          RETURN_NOT_OK(ConsumeMetadataChunks());
          break;
        case State::BODY:
          RETURN_NOT_OK(ConsumeBodyChunks());
          break;
        case State::EOS:
          return Status::OK();
      }
    }

    return Status::OK();
  }

  Status ConsumeInitialData(const uint8_t* data, int64_t) {
    return ConsumeInitial(BitUtil::FromLittleEndian(util::SafeLoadAs<int32_t>(data)));
  }

  Status ConsumeInitialBuffer(const std::shared_ptr<Buffer>& buffer) {
    ARROW_ASSIGN_OR_RAISE(auto continuation, ConsumeDataBufferInt32(buffer));
    return ConsumeInitial(BitUtil::FromLittleEndian(continuation));
  }

  Status ConsumeInitialChunks() {
    int32_t continuation = 0;
    RETURN_NOT_OK(ConsumeDataChunks(sizeof(int32_t), &continuation));
    return ConsumeInitial(BitUtil::FromLittleEndian(continuation));
  }

  Status ConsumeInitial(int32_t continuation) {
    if (continuation == internal::kIpcContinuationToken) {
      state_ = State::METADATA_LENGTH;
      next_required_size_ = kMessageDecoderNextRequiredSizeMetadataLength;
      RETURN_NOT_OK(listener_->OnMetadataLength());
      // Valid IPC message, read the message length now
      return Status::OK();
    } else if (continuation == 0) {
      state_ = State::EOS;
      next_required_size_ = 0;
      RETURN_NOT_OK(listener_->OnEOS());
      return Status::OK();
    } else if (continuation > 0) {
      state_ = State::METADATA;
      next_required_size_ = continuation;
      RETURN_NOT_OK(listener_->OnMetadata());
      return Status::OK();
    } else {
      return Status::IOError("Invalid IPC stream: negative continuation token");
    }
  }

  Status ConsumeMetadataLengthData(const uint8_t* data, int64_t) {
    return ConsumeMetadataLength(
        BitUtil::FromLittleEndian(util::SafeLoadAs<int32_t>(data)));
  }

  Status ConsumeMetadataLengthBuffer(const std::shared_ptr<Buffer>& buffer) {
    ARROW_ASSIGN_OR_RAISE(auto metadata_length, ConsumeDataBufferInt32(buffer));
    return ConsumeMetadataLength(BitUtil::FromLittleEndian(metadata_length));
  }

  Status ConsumeMetadataLengthChunks() {
    int32_t metadata_length = 0;
    RETURN_NOT_OK(ConsumeDataChunks(sizeof(int32_t), &metadata_length));
    return ConsumeMetadataLength(BitUtil::FromLittleEndian(metadata_length));
  }

  Status ConsumeMetadataLength(int32_t metadata_length) {
    if (metadata_length == 0) {
      state_ = State::EOS;
      next_required_size_ = 0;
      RETURN_NOT_OK(listener_->OnEOS());
      return Status::OK();
    } else if (metadata_length > 0) {
      state_ = State::METADATA;
      next_required_size_ = metadata_length;
      RETURN_NOT_OK(listener_->OnMetadata());
      return Status::OK();
    } else {
      return Status::IOError("Invalid IPC message: negative metadata length");
    }
  }

  Status ConsumeMetadataBuffer(const std::shared_ptr<Buffer>& buffer) {
    if (buffer->is_cpu()) {
      metadata_ = buffer;
    } else {
      ARROW_ASSIGN_OR_RAISE(metadata_,
                            Buffer::ViewOrCopy(buffer, CPUDevice::memory_manager(pool_)));
    }
    return ConsumeMetadata();
  }

  Status ConsumeMetadataChunks() {
    if (chunks_[0]->size() >= next_required_size_) {
      if (chunks_[0]->size() == next_required_size_) {
        if (chunks_[0]->is_cpu()) {
          metadata_ = std::move(chunks_[0]);
        } else {
          ARROW_ASSIGN_OR_RAISE(
              metadata_,
              Buffer::ViewOrCopy(chunks_[0], CPUDevice::memory_manager(pool_)));
        }
        chunks_.erase(chunks_.begin());
      } else {
        metadata_ = SliceBuffer(chunks_[0], 0, next_required_size_);
        if (!chunks_[0]->is_cpu()) {
          ARROW_ASSIGN_OR_RAISE(
              metadata_, Buffer::ViewOrCopy(metadata_, CPUDevice::memory_manager(pool_)));
        }
        chunks_[0] = SliceBuffer(chunks_[0], next_required_size_);
      }
      buffered_size_ -= next_required_size_;
    } else {
      ARROW_ASSIGN_OR_RAISE(auto metadata, AllocateBuffer(next_required_size_, pool_));
      metadata_ = std::shared_ptr<Buffer>(metadata.release());
      RETURN_NOT_OK(ConsumeDataChunks(next_required_size_, metadata_->mutable_data()));
    }
    return ConsumeMetadata();
  }

  Status ConsumeMetadata() {
    RETURN_NOT_OK(MaybeAlignMetadata(&metadata_));
    int64_t body_length = -1;
    RETURN_NOT_OK(CheckMetadataAndGetBodyLength(*metadata_, &body_length));

    state_ = State::BODY;
    next_required_size_ = body_length;
    RETURN_NOT_OK(listener_->OnBody());
    if (next_required_size_ == 0) {
      ARROW_ASSIGN_OR_RAISE(auto body, AllocateBuffer(0, pool_));
      std::shared_ptr<Buffer> shared_body(body.release());
      return ConsumeBody(&shared_body);
    } else {
      return Status::OK();
    }
  }

  Status ConsumeBodyBuffer(std::shared_ptr<Buffer> buffer) {
    return ConsumeBody(&buffer);
  }

  Status ConsumeBodyChunks() {
    if (chunks_[0]->size() >= next_required_size_) {
      auto used_size = next_required_size_;
      if (chunks_[0]->size() == next_required_size_) {
        RETURN_NOT_OK(ConsumeBody(&chunks_[0]));
        chunks_.erase(chunks_.begin());
      } else {
        auto body = SliceBuffer(chunks_[0], 0, next_required_size_);
        RETURN_NOT_OK(ConsumeBody(&body));
        chunks_[0] = SliceBuffer(chunks_[0], used_size);
      }
      buffered_size_ -= used_size;
      return Status::OK();
    } else {
      ARROW_ASSIGN_OR_RAISE(auto body, AllocateBuffer(next_required_size_, pool_));
      RETURN_NOT_OK(ConsumeDataChunks(next_required_size_, body->mutable_data()));
      std::shared_ptr<Buffer> shared_body(body.release());
      return ConsumeBody(&shared_body);
    }
  }

  Status ConsumeBody(std::shared_ptr<Buffer>* buffer) {
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Message> message,
                          Message::Open(metadata_, *buffer));

    RETURN_NOT_OK(listener_->OnMessageDecoded(std::move(message)));
    state_ = State::INITIAL;
    next_required_size_ = kMessageDecoderNextRequiredSizeInitial;
    RETURN_NOT_OK(listener_->OnInitial());
    return Status::OK();
  }

  Result<int32_t> ConsumeDataBufferInt32(const std::shared_ptr<Buffer>& buffer) {
    if (buffer->is_cpu()) {
      return util::SafeLoadAs<int32_t>(buffer->data());
    } else {
      ARROW_ASSIGN_OR_RAISE(auto cpu_buffer,
                            Buffer::ViewOrCopy(buffer, CPUDevice::memory_manager(pool_)));
      return util::SafeLoadAs<int32_t>(cpu_buffer->data());
    }
  }

  Status ConsumeDataChunks(int64_t nbytes, void* out) {
    size_t offset = 0;
    size_t n_used_chunks = 0;
    auto required_size = nbytes;
    std::shared_ptr<Buffer> last_chunk;
    for (auto& chunk : chunks_) {
      if (!chunk->is_cpu()) {
        ARROW_ASSIGN_OR_RAISE(
            chunk, Buffer::ViewOrCopy(chunk, CPUDevice::memory_manager(pool_)));
      }
      auto data = chunk->data();
      auto data_size = chunk->size();
      auto copy_size = std::min(required_size, data_size);
      memcpy(static_cast<uint8_t*>(out) + offset, data, copy_size);
      n_used_chunks++;
      offset += copy_size;
      required_size -= copy_size;
      if (required_size == 0) {
        if (data_size != copy_size) {
          last_chunk = SliceBuffer(chunk, copy_size);
        }
        break;
      }
    }
    chunks_.erase(chunks_.begin(), chunks_.begin() + n_used_chunks);
    if (last_chunk.get() != nullptr) {
      chunks_.insert(chunks_.begin(), std::move(last_chunk));
    }
    buffered_size_ -= offset;
    return Status::OK();
  }

  std::shared_ptr<MessageDecoderListener> listener_;
  MemoryPool* pool_;
  State state_;
  int64_t next_required_size_, save_initial_size_;
  std::vector<std::shared_ptr<Buffer>> chunks_;
  int64_t buffered_size_;
  std::shared_ptr<Buffer> metadata_;  // Must be CPU buffer
};

MessageDecoder2::MessageDecoder2(std::shared_ptr<MessageDecoderListener> listener,
                               MemoryPool* pool) {
  impl_.reset(new MessageDecoderImpl(std::move(listener), State::INITIAL,
                                     kMessageDecoderNextRequiredSizeInitial, pool));
}

MessageDecoder2::MessageDecoder2(std::shared_ptr<MessageDecoderListener> listener,
                               State initial_state, int64_t initial_next_required_size,
                               MemoryPool* pool) {
  impl_.reset(new MessageDecoderImpl(std::move(listener), initial_state,
                                     initial_next_required_size, pool));
}

MessageDecoder2::~MessageDecoder2() {}

Status MessageDecoder2::Consume(const uint8_t* data, int64_t size) {
  return impl_->ConsumeData(data, size);
}

void MessageDecoder2::Reset() {
  impl_->Reset();
}

Status MessageDecoder2::Consume(std::shared_ptr<Buffer> buffer) {
  return impl_->ConsumeBuffer(buffer);
}

int64_t MessageDecoder2::next_required_size() const { return impl_->next_required_size(); }

MessageDecoder2::State MessageDecoder2::state() const { return impl_->state(); }

enum class DictionaryKind { New, Delta, Replacement };

Status InvalidMessageType(MessageType expected, MessageType actual) {
  return Status::IOError("Expected IPC message of type ", ::arrow::ipc::FormatMessageType(expected),
                         " but got ", ::arrow::ipc::FormatMessageType(actual));
}

#define CHECK_MESSAGE_TYPE(expected, actual)           \
  do {                                                 \
    if ((actual) != (expected)) {                      \
      return InvalidMessageType((expected), (actual)); \
    }                                                  \
  } while (0)

#define CHECK_HAS_BODY(message)                                       \
  do {                                                                \
    if ((message).body() == nullptr) {                                \
      return Status::IOError("Expected body in IPC message of type ", \
                             ::arrow::ipc::FormatMessageType((message).type()));    \
    }                                                                 \
  } while (0)

#define CHECK_HAS_NO_BODY(message)                                      \
  do {                                                                  \
    if ((message).body_length() != 0) {                                 \
      return Status::IOError("Unexpected body in IPC message of type ", \
                             ::arrow::ipc::FormatMessageType((message).type()));      \
    }                                                                   \
  } while (0)
struct IpcReadContext {
  IpcReadContext(DictionaryMemo* memo, const IpcReadOptions& option, bool swap,
                 MetadataVersion version = MetadataVersion::V5,
                 Compression::type kind = Compression::UNCOMPRESSED)
      : dictionary_memo(memo),
        options(option),
        metadata_version(version),
        compression(kind),
        swap_endian(swap) {}

  DictionaryMemo* dictionary_memo;

  const IpcReadOptions& options;

  MetadataVersion metadata_version;

  Compression::type compression;

  const bool swap_endian;
};



Result<std::shared_ptr<Buffer>> DecompressBuffer(const std::shared_ptr<Buffer>& buf,
                                                 const IpcReadOptions& options,
                                                 util::Codec* codec) {
  if (buf == nullptr || buf->size() == 0) {
    return buf;
  }

  if (buf->size() < 8) {
    return Status::Invalid(
        "Likely corrupted message, compressed buffers "
        "are larger than 8 bytes by construction");
  }

  const uint8_t* data = buf->data();
  int64_t compressed_size = buf->size() - sizeof(int64_t);
  int64_t uncompressed_size = BitUtil::FromLittleEndian(util::SafeLoadAs<int64_t>(data));

  ARROW_ASSIGN_OR_RAISE(auto uncompressed,
                        AllocateBuffer(uncompressed_size, options.memory_pool));

  ARROW_ASSIGN_OR_RAISE(
      int64_t actual_decompressed,
      codec->Decompress(compressed_size, data + sizeof(int64_t), uncompressed_size,
                        uncompressed->mutable_data()));
  if (actual_decompressed != uncompressed_size) {
    return Status::Invalid("Failed to fully decompress buffer, expected ",
                           uncompressed_size, " bytes but decompressed ",
                           actual_decompressed);
  }

  return std::move(uncompressed);
}

Status DecompressBuffers(Compression::type compression, const IpcReadOptions& options,
                         ArrayDataVector* fields) {
  struct BufferAccumulator {
    using BufferPtrVector = std::vector<std::shared_ptr<Buffer>*>;

    void AppendFrom(const ArrayDataVector& fields) {
      for (const auto& field : fields) {
        for (auto& buffer : field->buffers) {
          buffers_.push_back(&buffer);
        }
        AppendFrom(field->child_data);
      }
    }

    BufferPtrVector Get(const ArrayDataVector& fields) && {
      AppendFrom(fields);
      return std::move(buffers_);
    }

    BufferPtrVector buffers_;
  };

  auto buffers = BufferAccumulator{}.Get(*fields);

  std::unique_ptr<util::Codec> codec;
  ARROW_ASSIGN_OR_RAISE(codec, util::Codec::Create(compression));

  return ::arrow::internal::OptionalParallelFor(
      options.use_threads, static_cast<int>(buffers.size()), [&](int i) {
        ARROW_ASSIGN_OR_RAISE(*buffers[i],
                              DecompressBuffer(*buffers[i], options, codec.get()));
        return Status::OK();
      });
}
class ArrayLoader {
 public:
  explicit ArrayLoader(const flatbuf::RecordBatch* metadata,
                       MetadataVersion metadata_version, const IpcReadOptions& options,
                       io::RandomAccessFile* file)
      : metadata_(metadata),
        metadata_version_(metadata_version),
        file_(file),
        max_recursion_depth_(options.max_recursion_depth) {}

  Status ReadBuffer(int64_t offset, int64_t length, std::shared_ptr<Buffer>* out) {
    if (skip_io_) {
      return Status::OK();
    }
    if (offset < 0) {
      return Status::Invalid("Negative offset for reading buffer ", buffer_index_);
    }
    if (length < 0) {
      return Status::Invalid("Negative length for reading buffer ", buffer_index_);
    }
    if (!BitUtil::IsMultipleOf8(offset)) {
      return Status::Invalid("Buffer ", buffer_index_,
                             " did not start on 8-byte aligned offset: ", offset);
    }
    return file_->ReadAt(offset, length).Value(out);
  }

  Status LoadType(const DataType& type) { return VisitTypeInline(type, this); }

  Status Load(const Field* field, ArrayData* out) {
    if (max_recursion_depth_ <= 0) {
      return Status::Invalid("Max recursion depth reached");
    }

    field_ = field;
    out_ = out;
    out_->type = field_->type();
    return LoadType(*field_->type());
  }

  Status SkipField(const Field* field) {
    ArrayData dummy;
    skip_io_ = true;
    Status status = Load(field, &dummy);
    skip_io_ = false;
    return status;
  }

  Status GetBuffer(int buffer_index, std::shared_ptr<Buffer>* out) {
    auto buffers = metadata_->buffers();
    CHECK_FLATBUFFERS_NOT_NULL(buffers, "RecordBatch.buffers");
    if (buffer_index >= static_cast<int>(buffers->size())) {
      return Status::IOError("buffer_index out of range.");
    }
    const flatbuf::Buffer* buffer = buffers->Get(buffer_index);
    if (buffer->length() == 0) {
      // Should never return a null buffer here.
      // (zero-sized buffer allocations are cheap)
      return AllocateBuffer(0).Value(out);
    } else {
      return ReadBuffer(buffer->offset(), buffer->length(), out);
    }
  }

  Status GetFieldMetadata(int field_index, ArrayData* out) {
    auto nodes = metadata_->nodes();
    CHECK_FLATBUFFERS_NOT_NULL(nodes, "Table.nodes");
    if (field_index >= static_cast<int>(nodes->size())) {
      return Status::Invalid("Ran out of field metadata, likely malformed");
    }
    const flatbuf::FieldNode* node = nodes->Get(field_index);

    out->length = node->length();
    out->null_count = node->null_count();
    out->offset = 0;
    return Status::OK();
  }

  Status LoadCommon(Type::type type_id) {
    RETURN_NOT_OK(GetFieldMetadata(field_index_++, out_));

    if (internal::HasValidityBitmap(type_id, metadata_version_)) {
      // Extract null_bitmap which is common to all arrays except for unions
      // and nulls.
      if (out_->null_count != 0) {
        RETURN_NOT_OK(GetBuffer(buffer_index_, &out_->buffers[0]));
      }
      buffer_index_++;
    }
    return Status::OK();
  }

  template <typename TYPE>
  Status LoadPrimitive(Type::type type_id) {
    out_->buffers.resize(2);

    RETURN_NOT_OK(LoadCommon(type_id));
    if (out_->length > 0) {
      RETURN_NOT_OK(GetBuffer(buffer_index_++, &out_->buffers[1]));
    } else {
      buffer_index_++;
      out_->buffers[1].reset(new Buffer(nullptr, 0));
    }
    return Status::OK();
  }

  template <typename TYPE>
  Status LoadBinary(Type::type type_id) {
    out_->buffers.resize(3);

    RETURN_NOT_OK(LoadCommon(type_id));
    RETURN_NOT_OK(GetBuffer(buffer_index_++, &out_->buffers[1]));
    return GetBuffer(buffer_index_++, &out_->buffers[2]);
  }

  template <typename TYPE>
  Status LoadList(const TYPE& type) {
    out_->buffers.resize(2);

    RETURN_NOT_OK(LoadCommon(type.id()));
    RETURN_NOT_OK(GetBuffer(buffer_index_++, &out_->buffers[1]));

    const int num_children = type.num_fields();
    if (num_children != 1) {
      return Status::Invalid("Wrong number of children: ", num_children);
    }

    return LoadChildren(type.fields());
  }

  Status LoadChildren(const std::vector<std::shared_ptr<Field>>& child_fields) {
    ArrayData* parent = out_;

    parent->child_data.resize(child_fields.size());
    for (int i = 0; i < static_cast<int>(child_fields.size()); ++i) {
      parent->child_data[i] = std::make_shared<ArrayData>();
      --max_recursion_depth_;
      RETURN_NOT_OK(Load(child_fields[i].get(), parent->child_data[i].get()));
      ++max_recursion_depth_;
    }
    out_ = parent;
    return Status::OK();
  }

  Status Visit(const NullType&) {
    out_->buffers.resize(1);

    return GetFieldMetadata(field_index_++, out_);
  }

  template <typename T>
  enable_if_t<std::is_base_of<FixedWidthType, T>::value &&
                  !std::is_base_of<FixedSizeBinaryType, T>::value &&
                  !std::is_base_of<DictionaryType, T>::value,
              Status>
  Visit(const T& type) {
    return LoadPrimitive<T>(type.id());
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T& type) {
    return LoadBinary<T>(type.id());
  }

  Status Visit(const FixedSizeBinaryType& type) {
    out_->buffers.resize(2);
    RETURN_NOT_OK(LoadCommon(type.id()));
    return GetBuffer(buffer_index_++, &out_->buffers[1]);
  }

  template <typename T>
  enable_if_var_size_list<T, Status> Visit(const T& type) {
    return LoadList(type);
  }

  Status Visit(const MapType& type) {
    RETURN_NOT_OK(LoadList(type));
    return MapArray::ValidateChildData(out_->child_data);
  }

  Status Visit(const FixedSizeListType& type) {
    out_->buffers.resize(1);

    RETURN_NOT_OK(LoadCommon(type.id()));

    const int num_children = type.num_fields();
    if (num_children != 1) {
      return Status::Invalid("Wrong number of children: ", num_children);
    }

    return LoadChildren(type.fields());
  }

  Status Visit(const StructType& type) {
    out_->buffers.resize(1);
    RETURN_NOT_OK(LoadCommon(type.id()));
    return LoadChildren(type.fields());
  }

  Status Visit(const UnionType& type) {
    int n_buffers = type.mode() == UnionMode::SPARSE ? 2 : 3;
    out_->buffers.resize(n_buffers);

    RETURN_NOT_OK(LoadCommon(type.id()));

    if (out_->null_count != 0 && out_->buffers[0] != nullptr) {
      return Status::Invalid(
          "Cannot read pre-1.0.0 Union array with top-level validity bitmap");
    }
    out_->buffers[0] = nullptr;
    out_->null_count = 0;

    if (out_->length > 0) {
      RETURN_NOT_OK(GetBuffer(buffer_index_, &out_->buffers[1]));
      if (type.mode() == UnionMode::DENSE) {
        RETURN_NOT_OK(GetBuffer(buffer_index_ + 1, &out_->buffers[2]));
      }
    }
    buffer_index_ += n_buffers - 1;
    return LoadChildren(type.fields());
  }

  Status Visit(const DictionaryType& type) {
    // out_->dictionary will be filled later in ResolveDictionaries()
    return LoadType(*type.index_type());
  }

  Status Visit(const ExtensionType& type) { return LoadType(*type.storage_type()); }

 private:
  const flatbuf::RecordBatch* metadata_;
  const MetadataVersion metadata_version_;
  io::RandomAccessFile* file_;
  int max_recursion_depth_;
  int buffer_index_ = 0;
  int field_index_ = 0;
  bool skip_io_ = false;

  const Field* field_;
  ArrayData* out_;
};

Result<std::shared_ptr<RecordBatch>> LoadRecordBatchSubset(
    const flatbuf::RecordBatch* metadata, const std::shared_ptr<Schema>& schema,
    const std::vector<bool>* inclusion_mask, const IpcReadContext& context,
    io::RandomAccessFile* file) {
  ArrayLoader loader(metadata, context.metadata_version, context.options, file);

  ArrayDataVector columns(schema->num_fields());
  ArrayDataVector filtered_columns;
  FieldVector filtered_fields;
  std::shared_ptr<Schema> filtered_schema;

  for (int i = 0; i < schema->num_fields(); ++i) {
    const Field& field = *schema->field(i);
    if (!inclusion_mask || (*inclusion_mask)[i]) {
      // Read field
      auto column = std::make_shared<ArrayData>();
      RETURN_NOT_OK(loader.Load(&field, column.get()));
      if (metadata->length() != column->length) {
        return Status::IOError("Array length did not match record batch length");
      }
      columns[i] = std::move(column);
      if (inclusion_mask) {
        filtered_columns.push_back(columns[i]);
        filtered_fields.push_back(schema->field(i));
      }
    } else {
      RETURN_NOT_OK(loader.SkipField(&field));
    }
  }

  RETURN_NOT_OK(ResolveDictionaries(columns, *context.dictionary_memo,
                                    context.options.memory_pool));

  if (inclusion_mask) {
    filtered_schema = ::arrow::schema(std::move(filtered_fields), schema->metadata());
    columns.clear();
  } else {
    filtered_schema = schema;
    filtered_columns = std::move(columns);
  }
  if (context.compression != Compression::UNCOMPRESSED) {
    RETURN_NOT_OK(
        DecompressBuffers(context.compression, context.options, &filtered_columns));
  }

  if (context.swap_endian) {
    for (int i = 0; i < static_cast<int>(filtered_columns.size()); ++i) {
      ARROW_ASSIGN_OR_RAISE(filtered_columns[i],
                            arrow::internal::SwapEndianArrayData(filtered_columns[i]));
    }
  }
  return RecordBatch::Make(std::move(filtered_schema), metadata->length(),
                           std::move(filtered_columns));
}

Result<std::shared_ptr<RecordBatch>> LoadRecordBatch(
    const flatbuf::RecordBatch* metadata, const std::shared_ptr<Schema>& schema,
    const std::vector<bool>& inclusion_mask, const IpcReadContext& context,
    io::RandomAccessFile* file) {
  if (inclusion_mask.size() > 0) {
    return LoadRecordBatchSubset(metadata, schema, &inclusion_mask, context, file);
  } else {
    return LoadRecordBatchSubset(metadata, schema, /*param_name=*/nullptr, context, file);
  }
}

Status GetCompression(const flatbuf::RecordBatch* batch, Compression::type* out) {
  *out = Compression::UNCOMPRESSED;
  const flatbuf::BodyCompression* compression = batch->compression();
  if (compression != nullptr) {
    if (compression->method() != flatbuf::BodyCompressionMethod::BUFFER) {
      return Status::Invalid("This library only supports BUFFER compression method");
    }

    if (compression->codec() == flatbuf::CompressionType::LZ4_FRAME) {
      *out = Compression::LZ4_FRAME;
    } else if (compression->codec() == flatbuf::CompressionType::ZSTD) {
      *out = Compression::ZSTD;
    } else {
      return Status::Invalid("Unsupported codec in RecordBatch::compression metadata");
    }
    return Status::OK();
  }
  return Status::OK();
}

Status GetCompressionExperimental(const flatbuf::Message* message,
                                  Compression::type* out) {
  *out = Compression::UNCOMPRESSED;
  if (message->custom_metadata() != nullptr) {
    std::shared_ptr<KeyValueMetadata> metadata;
    RETURN_NOT_OK(internal::GetKeyValueMetadata(message->custom_metadata(), &metadata));
    int index = metadata->FindKey("ARROW:experimental_compression");
    if (index != -1) {
      auto name = arrow::internal::AsciiToLower(metadata->value(index));
      ARROW_ASSIGN_OR_RAISE(*out, util::Codec::GetCompressionType(name));
    }
    return internal::CheckCompressionSupported(*out);
  }
  return Status::OK();
}

static Status ReadContiguousPayload(io::InputStream* file,
                                    std::unique_ptr<Message>* message) {
  ARROW_ASSIGN_OR_RAISE(*message, ReadMessage(file));
  if (*message == nullptr) {
    return Status::Invalid("Unable to read metadata at offset");
  }
  return Status::OK();
}

Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(
    const std::shared_ptr<Schema>& schema, const DictionaryMemo* dictionary_memo,
    const IpcReadOptions& options, io::InputStream* file) {
  std::unique_ptr<Message> message;
  RETURN_NOT_OK(ReadContiguousPayload(file, &message));
  CHECK_HAS_BODY(*message);
  ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message->body()));
  return ReadRecordBatch(*message->metadata(), schema, dictionary_memo, options,
                         reader.get());
}

Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(
    const Message& message, const std::shared_ptr<Schema>& schema,
    const DictionaryMemo* dictionary_memo, const IpcReadOptions& options) {
  CHECK_MESSAGE_TYPE(MessageType::RECORD_BATCH, message.type());
  CHECK_HAS_BODY(message);
  ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message.body()));
  return ReadRecordBatch(*message.metadata(), schema, dictionary_memo, options,
                         reader.get());
}

Result<std::shared_ptr<RecordBatch>> ReadRecordBatchInternal(
    const Buffer& metadata, const std::shared_ptr<Schema>& schema,
    const std::vector<bool>& inclusion_mask, IpcReadContext& context,
    io::RandomAccessFile* file) {
  const flatbuf::Message* message = nullptr;
  RETURN_NOT_OK(internal::VerifyMessage(metadata.data(), metadata.size(), &message));
  auto batch = message->header_as_RecordBatch();
  if (batch == nullptr) {
    return Status::IOError(
        "Header-type of flatbuffer-encoded Message is not RecordBatch.");
  }

  Compression::type compression;
  RETURN_NOT_OK(GetCompression(batch, &compression));
  if (context.compression == Compression::UNCOMPRESSED &&
      message->version() == flatbuf::MetadataVersion::V4) {
    RETURN_NOT_OK(GetCompressionExperimental(message, &compression));
  }
  context.compression = compression;
  context.metadata_version = internal::GetMetadataVersion(message->version());
  return LoadRecordBatch(batch, schema, inclusion_mask, context, file);
}

Status GetInclusionMaskAndOutSchema(const std::shared_ptr<Schema>& full_schema,
                                    const std::vector<int>& included_indices,
                                    std::vector<bool>* inclusion_mask,
                                    std::shared_ptr<Schema>* out_schema) {
  inclusion_mask->clear();
  if (included_indices.empty()) {
    *out_schema = full_schema;
    return Status::OK();
  }

  inclusion_mask->resize(full_schema->num_fields(), false);

  auto included_indices_sorted = included_indices;
  std::sort(included_indices_sorted.begin(), included_indices_sorted.end());

  FieldVector included_fields;
  for (int i : included_indices_sorted) {
    if (i < 0 || i >= full_schema->num_fields()) {
      return Status::Invalid("Out of bounds field index: ", i);
    }

    if (inclusion_mask->at(i)) continue;

    inclusion_mask->at(i) = true;
    included_fields.push_back(full_schema->field(i));
  }

  *out_schema = schema(std::move(included_fields), full_schema->endianness(),
                       full_schema->metadata());
  return Status::OK();
}

Status UnpackSchemaMessage(const void* opaque_schema, const IpcReadOptions& options,
                           DictionaryMemo* dictionary_memo,
                           std::shared_ptr<Schema>* schema,
                           std::shared_ptr<Schema>* out_schema,
                           std::vector<bool>* field_inclusion_mask, bool* swap_endian) {
  RETURN_NOT_OK(internal::GetSchema(opaque_schema, dictionary_memo, schema));

  RETURN_NOT_OK(GetInclusionMaskAndOutSchema(*schema, options.included_fields,
                                             field_inclusion_mask, out_schema));
  *swap_endian = options.ensure_native_endian && !out_schema->get()->is_native_endian();
  if (*swap_endian) {
    *schema = schema->get()->WithEndianness(Endianness::Native);
    *out_schema = out_schema->get()->WithEndianness(Endianness::Native);
  }
  return Status::OK();
}

Status UnpackSchemaMessage(const Message& message, const IpcReadOptions& options,
                           DictionaryMemo* dictionary_memo,
                           std::shared_ptr<Schema>* schema,
                           std::shared_ptr<Schema>* out_schema,
                           std::vector<bool>* field_inclusion_mask, bool* swap_endian) {
  CHECK_MESSAGE_TYPE(MessageType::SCHEMA, message.type());
  CHECK_HAS_NO_BODY(message);

  return UnpackSchemaMessage(message.header(), options, dictionary_memo, schema,
                             out_schema, field_inclusion_mask, swap_endian);
}

Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(
    const Buffer& metadata, const std::shared_ptr<Schema>& schema,
    const DictionaryMemo* dictionary_memo, const IpcReadOptions& options,
    io::RandomAccessFile* file) {
  std::shared_ptr<Schema> out_schema;
  // Empty means do not use
  std::vector<bool> inclusion_mask;
  IpcReadContext context(const_cast<DictionaryMemo*>(dictionary_memo), options, false);
  RETURN_NOT_OK(GetInclusionMaskAndOutSchema(schema, context.options.included_fields,
                                             &inclusion_mask, &out_schema));
  return ReadRecordBatchInternal(metadata, schema, inclusion_mask, context, file);
}

Status ReadDictionary(const Buffer& metadata, const IpcReadContext& context,
                      DictionaryKind* kind, io::RandomAccessFile* file) {
  const flatbuf::Message* message = nullptr;
  RETURN_NOT_OK(internal::VerifyMessage(metadata.data(), metadata.size(), &message));
  const auto dictionary_batch = message->header_as_DictionaryBatch();
  if (dictionary_batch == nullptr) {
    return Status::IOError(
        "Header-type of flatbuffer-encoded Message is not DictionaryBatch.");
  }

  // The dictionary is embedded in a record batch with a single column
  const auto batch_meta = dictionary_batch->data();

  CHECK_FLATBUFFERS_NOT_NULL(batch_meta, "DictionaryBatch.data");

  Compression::type compression;
  RETURN_NOT_OK(GetCompression(batch_meta, &compression));
  if (compression == Compression::UNCOMPRESSED &&
      message->version() == flatbuf::MetadataVersion::V4) {
    RETURN_NOT_OK(GetCompressionExperimental(message, &compression));
  }

  const int64_t id = dictionary_batch->id();

  ARROW_ASSIGN_OR_RAISE(auto value_type, context.dictionary_memo->GetDictionaryType(id));

  ArrayLoader loader(batch_meta, internal::GetMetadataVersion(message->version()),
                     context.options, file);
  auto dict_data = std::make_shared<ArrayData>();
  const Field dummy_field("", value_type);
  RETURN_NOT_OK(loader.Load(&dummy_field, dict_data.get()));

  if (compression != Compression::UNCOMPRESSED) {
    ArrayDataVector dict_fields{dict_data};
    RETURN_NOT_OK(DecompressBuffers(compression, context.options, &dict_fields));
  }

  if (context.swap_endian) {
    ARROW_ASSIGN_OR_RAISE(dict_data, ::arrow::internal::SwapEndianArrayData(dict_data));
  }

  if (dictionary_batch->isDelta()) {
    if (kind != nullptr) {
      *kind = DictionaryKind::Delta;
    }
    return context.dictionary_memo->AddDictionaryDelta(id, dict_data);
  }
  ARROW_ASSIGN_OR_RAISE(bool inserted,
                        context.dictionary_memo->AddOrReplaceDictionary(id, dict_data));
  if (kind != nullptr) {
    *kind = inserted ? DictionaryKind::New : DictionaryKind::Replacement;
  }
  return Status::OK();
}

Status ReadDictionary(const Message& message, const IpcReadContext& context,
                      DictionaryKind* kind) {
  DCHECK_EQ(message.type(), MessageType::DICTIONARY_BATCH);
  CHECK_HAS_BODY(message);
  ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message.body()));
  return ReadDictionary(*message.metadata(), context, kind, reader.get());
}


class StreamDecoder2::StreamDecoder2Impl : public MessageDecoderListener {
 private:
  enum State {
    SCHEMA,
    INITIAL_DICTIONARIES,
    RECORD_BATCHES,
    EOS,
  };

 public:
  explicit StreamDecoder2Impl(std::shared_ptr<Listener> listener, IpcReadOptions options)
      : listener_(std::move(listener)),
        options_(std::move(options)),
        state_(State::SCHEMA),
        message_decoder_(std::shared_ptr<StreamDecoder2Impl>(this, [](void*) {}),
                         options_.memory_pool),
        n_required_dictionaries_(0),
        dictionary_memo_(std::make_unique<DictionaryMemo>()) {}

  void Reset() {
    state_ = State::SCHEMA;
    field_inclusion_mask_.clear();
    n_required_dictionaries_ = 0;
    dictionary_memo_ = std::make_unique<DictionaryMemo>();
    schema_ = out_schema_ = nullptr;
    message_decoder_.Reset();
  }

  Status OnMessageDecoded(std::unique_ptr<Message> message) override {
    ++stats_.num_messages;
    switch (state_) {
      case State::SCHEMA:
        ARROW_RETURN_NOT_OK(OnSchemaMessageDecoded(std::move(message)));
        break;
      case State::INITIAL_DICTIONARIES:
        ARROW_RETURN_NOT_OK(OnInitialDictionaryMessageDecoded(std::move(message)));
        break;
      case State::RECORD_BATCHES:
        ARROW_RETURN_NOT_OK(OnRecordBatchMessageDecoded(std::move(message)));
        break;
      case State::EOS:
        break;
    }
    return Status::OK();
  }

  Status OnEOS() override {
    state_ = State::EOS;
    return listener_->OnEOS();
  }
  Status Consume(const uint8_t* data, int64_t size) {
    return message_decoder_.Consume(data, size);
  }

  Status Consume(std::shared_ptr<Buffer> buffer) {
    return message_decoder_.Consume(std::move(buffer));
  }

  std::shared_ptr<Schema> schema() const { return out_schema_; }

  int64_t next_required_size() const { return message_decoder_.next_required_size(); }

  ReadStats stats() const { return stats_; }

 private:
  Status OnSchemaMessageDecoded(std::unique_ptr<Message> message) {
    RETURN_NOT_OK(UnpackSchemaMessage(*message, options_, dictionary_memo_.get(), &schema_,
                                      &out_schema_, &field_inclusion_mask_,
                                      &swap_endian_));

    n_required_dictionaries_ = dictionary_memo_->fields().num_fields();
    if (n_required_dictionaries_ == 0) {
      state_ = State::RECORD_BATCHES;
      RETURN_NOT_OK(listener_->OnSchemaDecoded(schema_));
    } else {
      state_ = State::INITIAL_DICTIONARIES;
    }
    return Status::OK();
  }

  Status OnInitialDictionaryMessageDecoded(std::unique_ptr<Message> message) {
    if (message->type() != MessageType::DICTIONARY_BATCH) {
      return Status::Invalid("IPC stream did not have the expected number (",
                             dictionary_memo_->fields().num_fields(),
                             ") of dictionaries at the start of the stream");
    }
    RETURN_NOT_OK(ReadDictionary(*message));
    n_required_dictionaries_--;
    if (n_required_dictionaries_ == 0) {
      state_ = State::RECORD_BATCHES;
      ARROW_RETURN_NOT_OK(listener_->OnSchemaDecoded(schema_));
    }
    return Status::OK();
  }

  Status OnRecordBatchMessageDecoded(std::unique_ptr<Message> message) {
    IpcReadContext context(dictionary_memo_.get(), options_, swap_endian_);
    if (message->type() == MessageType::DICTIONARY_BATCH) {
      return ReadDictionary(*message);
    } else {
      CHECK_HAS_BODY(*message);
      ARROW_ASSIGN_OR_RAISE(auto reader, Buffer::GetReader(message->body()));
      IpcReadContext context(dictionary_memo_.get(), options_, swap_endian_);
      ARROW_ASSIGN_OR_RAISE(
          auto batch,
          ReadRecordBatchInternal(*message->metadata(), schema_, field_inclusion_mask_,
                                  context, reader.get()));
      ++stats_.num_record_batches;
      return listener_->OnRecordBatchDecoded(std::move(batch));
    }
  }

  Status ReadDictionary(const Message& message) {
    DictionaryKind kind;
    IpcReadContext context(dictionary_memo_.get(), options_, swap_endian_);
    RETURN_NOT_OK(::arrow::ipc::NDqs::ReadDictionary(message, context, &kind));
    ++stats_.num_dictionary_batches;
    switch (kind) {
      case DictionaryKind::New:
        break;
      case DictionaryKind::Delta:
        ++stats_.num_dictionary_deltas;
        break;
      case DictionaryKind::Replacement:
        ++stats_.num_replaced_dictionaries;
        break;
    }
    return Status::OK();
  }

  std::shared_ptr<Listener> listener_;
  const IpcReadOptions options_;
  State state_;
  MessageDecoder2 message_decoder_;
  std::vector<bool> field_inclusion_mask_;
  int n_required_dictionaries_;
  std::unique_ptr<DictionaryMemo> dictionary_memo_;
  std::shared_ptr<Schema> schema_, out_schema_;
  ReadStats stats_;
  bool swap_endian_;
};

Result<std::shared_ptr<Schema>> ReadSchema(io::InputStream* stream,
                                           DictionaryMemo* dictionary_memo) {
  std::unique_ptr<MessageReader> reader = MessageReader::Open(stream);
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Message> message, reader->ReadNextMessage());
  if (!message) {
    return Status::Invalid("Tried reading schema message, was null or length 0");
  }
  CHECK_MESSAGE_TYPE(MessageType::SCHEMA, message->type());
  return ReadSchema(*message, dictionary_memo);
}

Result<std::shared_ptr<Schema>> ReadSchema(const Message& message,
                                           DictionaryMemo* dictionary_memo) {
  std::shared_ptr<Schema> result;
  RETURN_NOT_OK(internal::GetSchema(message.header(), dictionary_memo, &result));
  return result;
}

Result<std::shared_ptr<Tensor>> ReadTensor(io::InputStream* file) {
  std::unique_ptr<Message> message;
  RETURN_NOT_OK(ReadContiguousPayload(file, &message));
  return ReadTensor(*message);
}

Result<std::shared_ptr<Tensor>> ReadTensor(const Message& message) {
  std::shared_ptr<DataType> type;
  std::vector<int64_t> shape;
  std::vector<int64_t> strides;
  std::vector<std::string> dim_names;
  CHECK_HAS_BODY(message);
  RETURN_NOT_OK(internal::GetTensorMetadata(*message.metadata(), &type, &shape, &strides,
                                            &dim_names));
  return Tensor::Make(type, message.body(), shape, strides, dim_names);
}


StreamDecoder2::StreamDecoder2(std::shared_ptr<Listener> listener, IpcReadOptions options) {
  impl_.reset(new StreamDecoder2::StreamDecoder2Impl(std::move(listener), options));
}

StreamDecoder2::~StreamDecoder2() {}

Status StreamDecoder2::Consume(const uint8_t* data, int64_t size) {
  return impl_->Consume(data, size);
}

void StreamDecoder2::Reset() {
  impl_->Reset();
}

Status StreamDecoder2::Consume(std::shared_ptr<Buffer> buffer) {
  return impl_->Consume(std::move(buffer));
}

std::shared_ptr<Schema> StreamDecoder2::schema() const { return impl_->schema(); }

int64_t StreamDecoder2::next_required_size() const { return impl_->next_required_size(); }

ReadStats StreamDecoder2::stats() const { return impl_->stats(); }

class InputStreamMessageReader : public MessageReader, public MessageDecoderListener {
 public:
  explicit InputStreamMessageReader(io::InputStream* stream)
      : stream_(stream),
        owned_stream_(),
        message_(),
        decoder_(std::shared_ptr<InputStreamMessageReader>(this, [](void*) {})) {}

  explicit InputStreamMessageReader(const std::shared_ptr<io::InputStream>& owned_stream)
      : InputStreamMessageReader(owned_stream.get()) {
    owned_stream_ = owned_stream;
  }

  ~InputStreamMessageReader() {}

  Status OnMessageDecoded(std::unique_ptr<Message> message) override {
    message_ = std::move(message);
    return Status::OK();
  }

  Result<std::unique_ptr<Message>> ReadNextMessage() override {
    ARROW_RETURN_NOT_OK(DecodeMessage(&decoder_, stream_));
    return std::move(message_);
  }

 private:
  io::InputStream* stream_;
  std::shared_ptr<io::InputStream> owned_stream_;
  std::unique_ptr<Message> message_;
  MessageDecoder decoder_;
};


std::unique_ptr<MessageReader> MessageReader::Open(io::InputStream* stream) {
  return std::unique_ptr<MessageReader>(new InputStreamMessageReader(stream));
}

std::unique_ptr<MessageReader> MessageReader::Open(
    const std::shared_ptr<io::InputStream>& owned_stream) {
  return std::unique_ptr<MessageReader>(new InputStreamMessageReader(owned_stream));
}

}

}  // namespace ipc::NDqs
}  // namespace arrow
