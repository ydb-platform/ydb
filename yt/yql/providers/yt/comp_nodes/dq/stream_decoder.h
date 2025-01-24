// almost copy of reader.cc + message.cc without comments
// TODO(): Remove when .Reset() will be added in contrib version
#pragma once

#include <arrow/ipc/reader.h>
#include <arrow/ipc/message.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <arrow/io/caching.h>
#include <arrow/io/type_fwd.h>
#include <arrow/ipc/message.h>
#include <arrow/ipc/options.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/type_fwd.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/macros.h>
#include <arrow/util/visibility.h>

namespace arrow {
namespace ipc::NDqs {

class ARROW_EXPORT MessageDecoder2 {
 public:
  enum State {
    INITIAL,
    METADATA_LENGTH,
    METADATA,
    BODY,
    EOS,
  };

  explicit MessageDecoder2(std::shared_ptr<MessageDecoderListener> listener,
                          MemoryPool* pool = default_memory_pool());

  MessageDecoder2(std::shared_ptr<MessageDecoderListener> listener, State initial_state,
                 int64_t initial_next_required_size,
                 MemoryPool* pool = default_memory_pool());

  virtual ~MessageDecoder2();
  Status Consume(const uint8_t* data, int64_t size);
  Status Consume(std::shared_ptr<Buffer> buffer);
  int64_t next_required_size() const;
  State state() const;
  void Reset();

 private:
  class MessageDecoderImpl;
  std::unique_ptr<MessageDecoderImpl> impl_;

  ARROW_DISALLOW_COPY_AND_ASSIGN(MessageDecoder2);
};

class ARROW_EXPORT MessageReader {
 public:
  virtual ~MessageReader() = default;
  static std::unique_ptr<MessageReader> Open(io::InputStream* stream);
  static std::unique_ptr<MessageReader> Open(
      const std::shared_ptr<io::InputStream>& owned_stream);
  virtual Result<std::unique_ptr<Message>> ReadNextMessage() = 0;
};

class ARROW_EXPORT StreamDecoder2 {
 public:
  StreamDecoder2(std::shared_ptr<Listener> listener,
                IpcReadOptions options = IpcReadOptions::Defaults());

  virtual ~StreamDecoder2();
  Status Consume(const uint8_t* data, int64_t size);
  Status Consume(std::shared_ptr<Buffer> buffer);
  std::shared_ptr<Schema> schema() const;
  int64_t next_required_size() const;
  ReadStats stats() const;
  void Reset();

 private:
  class StreamDecoder2Impl;
  std::unique_ptr<StreamDecoder2Impl> impl_;
};
}  // namespace ipc::NDqs
}  // namespace arrow
