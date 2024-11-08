#include "chunked_buffer.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NYql {

TChunkedBuffer::TChunkedBuffer(TChunkedBuffer&& other) {
    Items_ = std::move(other.Items_);
}

TChunkedBuffer& TChunkedBuffer::operator=(TChunkedBuffer&& other) {
    Items_ = std::move(other.Items_);
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

size_t TChunkedBuffer::ContigousSize() const {
    return Items_.empty() ? 0 : Front().Buf.size();
}

size_t TChunkedBuffer::Size() const {
    size_t result = 0;
    for (auto& item : Items_) {
        result += item.Buf.size();
    }
    return result;
}

bool TChunkedBuffer::Empty() const {
    return Items_.empty();
}

TChunkedBuffer& TChunkedBuffer::Append(TStringBuf buf, const std::shared_ptr<const void>& owner) {
    if (!buf.empty()) {
        Items_.emplace_back(TChunk{buf, owner});
    }
    return *this;
}

TChunkedBuffer& TChunkedBuffer::Append(TString&& str) {
    if (!str.empty()) {
        auto owner = std::make_shared<TString>(std::move(str));
        Items_.emplace_back(TChunk{*owner, owner});
    }
    return *this;
}

TChunkedBuffer& TChunkedBuffer::Append(TChunkedBuffer&& other) {
    while (!other.Items_.empty()) {
        Items_.emplace_back(std::move(other.Items_.front()));
        other.Items_.pop_front();
    }
    return *this;
}

TChunkedBuffer& TChunkedBuffer::Clear() {
    Items_.clear();
    return *this;
}

TChunkedBuffer& TChunkedBuffer::Erase(size_t size) {
    while (size && !Items_.empty()) {
        TStringBuf& buf = Items_.front().Buf;
        size_t toErase = std::min(buf.size(), size);
        buf.Skip(toErase);
        size -= toErase;
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
