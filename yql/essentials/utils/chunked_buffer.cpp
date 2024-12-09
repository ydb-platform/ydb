#include "chunked_buffer.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NYql {

TChunkedBuffer::TChunkedBuffer(TChunkedBuffer&& other) {
    Items_ = std::move(other.Items_);
    Size_ = other.Size_;
    other.Size_ = 0;
}

TChunkedBuffer& TChunkedBuffer::operator=(TChunkedBuffer&& other) {
    Items_ = std::move(other.Items_);
    Size_ = other.Size_;
    other.Size_ = 0;
    return *this;
}

TChunkedBuffer::TChunkedBuffer(TStringBuf buf, const std::shared_ptr<const void>& owner) {
    Append(buf, owner);
}

TChunkedBuffer::TChunkedBuffer(TString&& str) {
    Append(std::move(str));
}

const TChunkedBuffer::TChunk& TChunkedBuffer::Front() const {
    YQL_ENSURE(!Items_.empty());
    return Items_.front();
}

size_t TChunkedBuffer::CopyTo(IOutputStream& dst, size_t toCopy) const {
    size_t copied = 0;
    for (auto& chunk : Items_) {
        if (!toCopy) {
            break;
        }
        size_t copyChunk = std::min(chunk.Buf.size(), toCopy);
        dst.Write(chunk.Buf.data(), copyChunk);
        toCopy -= copyChunk;
        copied += copyChunk;
    }
    return copied;
}

TChunkedBuffer& TChunkedBuffer::Append(TStringBuf buf, const std::shared_ptr<const void>& owner) {
    if (!buf.empty()) {
        Items_.emplace_back(TChunk{buf, owner});
        Size_ += buf.size();
    }
    return *this;
}

TChunkedBuffer& TChunkedBuffer::Append(TString&& str) {
    if (!str.empty()) {
        auto owner = std::make_shared<TString>(std::move(str));
        Items_.emplace_back(TChunk{*owner, owner});
        Size_ += owner->size();
    }
    return *this;
}

TChunkedBuffer& TChunkedBuffer::Append(TChunkedBuffer&& other) {
    while (!other.Items_.empty()) {
        Items_.emplace_back(std::move(other.Items_.front()));
        Size_ += Items_.back().Buf.size();
        other.Items_.pop_front();
    }
    other.Size_ = 0;
    return *this;
}

TChunkedBuffer& TChunkedBuffer::Clear() {
    Items_.clear();
    Size_ = 0;
    return *this;
}

TChunkedBuffer& TChunkedBuffer::Erase(size_t size) {
    while (size && !Items_.empty()) {
        TStringBuf& buf = Items_.front().Buf;
        size_t toErase = std::min(buf.size(), size);
        buf.Skip(toErase);
        size -= toErase;
        Size_ -= toErase;
        if (buf.empty()) {
            Items_.pop_front();
        }
    }
    return *this;
}

TChunkedBufferOutput::TChunkedBufferOutput(TChunkedBuffer& dst)
    : Dst_(dst)
{
}

void TChunkedBufferOutput::DoWrite(const void* buf, size_t len) {
    TString str(static_cast<const char*>(buf), len);
    Dst_.Append(std::move(str));
}

TChunkedBuffer CopyData(const TChunkedBuffer& src) {
    TChunkedBuffer result;
    TChunkedBufferOutput out(result);
    src.CopyTo(out);
    return result;
}

TChunkedBuffer CopyData(TChunkedBuffer&& src) {
    TChunkedBuffer result = CopyData(src);
    src.Clear();
    return result;
}

}
