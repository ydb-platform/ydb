#include "rope_over_buffer.h"

#include "yql_panic.h"

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


TRope MakeReadOnlyRope(const std::shared_ptr<const void>& owner, const char* data, size_t size) {
    if (!size) {
        return TRope();
    }
    return TRope(new TContigousChunkOverBuf(owner, {data, size}));
}

} // namespace NYql