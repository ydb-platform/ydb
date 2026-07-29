#include <util/system/yassert.h>

#include "kafka_write_buffer.h"

namespace NKafka {

TKafkaWriteBuffer::TKafkaWriteBuffer(size_t chunkSize)
    : ChunkSize_(chunkSize)
{
}

void TKafkaWriteBuffer::write(const char* src, size_t length) {
    size_t left = length;
    size_t offset = 0;

    do {
        TBuffer& buffer = GetOrCreateFrontBuffer();
        if (buffer.Avail() < left) {
            const size_t avail = buffer.Avail();
            buffer.Append(src + offset, avail);
            offset += avail;
            left -= avail;
            Buffers_.push_front(TBuffer(ChunkSize_));
        } else {
            buffer.Append(src + offset, left);
            break;
        }
    } while (left > 0);
}

TBuffer& TKafkaWriteBuffer::GetFrontBuffer() {
    return GetOrCreateFrontBuffer();
}

const TBuffer& TKafkaWriteBuffer::GetFrontBuffer() const {
    Y_ABORT_UNLESS(!Buffers_.empty());
    return Buffers_.front();
}

const TDeque<TBuffer>& TKafkaWriteBuffer::GetBuffersDeque() const {
    return Buffers_;
}

TString TKafkaWriteBuffer::AsString() const {
    TString result;
    for (auto it = Buffers_.rbegin(); it != Buffers_.rend(); ++it) {
        result.append(it->Data(), it->Size());
    }
    return result;
}

TBuffer& TKafkaWriteBuffer::GetOrCreateFrontBuffer() {
    if (Buffers_.empty()) {
        Buffers_.push_front(TBuffer(ChunkSize_));
    }
    return Buffers_.front();
}

} // namespace NKafka
