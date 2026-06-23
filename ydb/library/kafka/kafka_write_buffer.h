#pragma once

#include <util/generic/buffer.h>
#include <util/generic/deque.h>
#include <util/generic/string.h>

namespace NKafka {

class TKafkaWriteBuffer {
public:
    explicit TKafkaWriteBuffer(size_t chunkSize);

    void write(const char* src, size_t length);

    TBuffer& GetFrontBuffer();
    const TBuffer& GetFrontBuffer() const;
    const TDeque<TBuffer>& GetBuffersDeque() const;

    TString AsString() const;

private:
    TBuffer& GetOrCreateFrontBuffer();

    size_t ChunkSize_;
    TDeque<TBuffer> Buffers_;
};

} // namespace NKafka
