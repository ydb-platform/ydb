#include "rope_over_buffer.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NYql {

namespace {

class TContigousChunkOverBuf : public IContiguousChunk {
public:
    TContigousChunkOverBuf(const std::shared_ptr<const void>& owner, TContiguousSpan span)
        : Owner_(owner)
        , Span_(span)
    {
    }
private:
    TContiguousSpan GetData() const override {
        return Span_;
    }

    TMutableContiguousSpan GetDataMut() override {
        YQL_ENSURE(false, "Payload mutation is not supported");
    }

    size_t GetOccupiedMemorySize() const override {
        return Span_.GetSize();
    }

    const std::shared_ptr<const void> Owner_;
    const TContiguousSpan Span_;
};

} // namespace

TRope MakeReadOnlyRope(TChunkedBuffer&& buffer) {
    TRope result;
    while (!buffer.Empty()) {
        auto& front = buffer.Front();
        TRope chunk(new TContigousChunkOverBuf(front.Owner, {front.Buf.data(), front.Buf.size()}));
        result.Insert(result.End(), std::move(chunk));
        buffer.Erase(front.Buf.size());
    }

    return result;
}

TRope MakeReadOnlyRope(const TChunkedBuffer& buffer) {
    return MakeReadOnlyRope(TChunkedBuffer(buffer));
}

TChunkedBuffer MakeChunkedBuffer(TRope&& rope) {
    TChunkedBuffer result = MakeChunkedBuffer(rope);
    rope.clear();
    return result;
}

TChunkedBuffer MakeChunkedBuffer(const TRope& rope) {
    TChunkedBuffer result;
    for (auto it = rope.Begin(); it != rope.End(); ++it) {
        auto chunk = std::make_shared<TRope>(it, it + it.ContiguousSize());
        YQL_ENSURE(!chunk->empty());
        result.Append(TStringBuf(chunk->Begin().ContiguousData(), chunk->Begin().ContiguousSize()), chunk);
    }
    return result;
}

}