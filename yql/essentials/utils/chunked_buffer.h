#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

#include <memory>
#include <deque>

namespace NYql {

class TChunkedBuffer {
public:
    TChunkedBuffer() = default;
    TChunkedBuffer(const TChunkedBuffer&) = default;
    TChunkedBuffer(TChunkedBuffer&& other);
    TChunkedBuffer& operator=(TChunkedBuffer&& other);
    explicit TChunkedBuffer(TStringBuf buf, const std::shared_ptr<const void>& owner);
    explicit TChunkedBuffer(TString&& str);

    size_t ContigousSize() const;
    size_t Size() const;
    bool Empty() const;

    struct TChunk {
        TStringBuf Buf;
        std::shared_ptr<const void> Owner;
    };

    const TChunk& Front() const;
    size_t CopyTo(IOutputStream& dst, size_t toCopy = std::numeric_limits<size_t>::max()) const;

    TChunkedBuffer& Append(TStringBuf buf, const std::shared_ptr<const void>& owner);
    TChunkedBuffer& Append(TString&& str);
    TChunkedBuffer& Append(TChunkedBuffer&& other);

    TChunkedBuffer& Clear();
    TChunkedBuffer& Erase(size_t size);

private:
    std::deque<TChunk> Items_;
};

class TChunkedBufferOutput : public IOutputStream {
public:
    explicit TChunkedBufferOutput(TChunkedBuffer& dst);
private:
    virtual void DoWrite(const void *buf, size_t len) override;

    TChunkedBuffer& Dst_;
};

TChunkedBuffer CopyData(const TChunkedBuffer& src);
TChunkedBuffer CopyData(TChunkedBuffer&& src);

}
