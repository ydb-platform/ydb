#include "rc_buf.h"

template<>
void Out<TRcBuf>(IOutputStream& s, const TRcBuf& x) {
    s.Write(TStringBuf(x));
}

static class DefaultRcBufAllocator final : public IRcBufAllocator {
public:
    TRcBuf AllocRcBuf(size_t size, size_t headRoom, size_t tailRoom) noexcept {
        return TRcBuf::Uninitialized(size, headRoom, tailRoom);
    }
    TRcBuf AllocPageAlignedRcBuf(size_t size, size_t tailRoom) noexcept {
        return TRcBuf::UninitializedPageAligned(size, tailRoom);
    }
} RcBufAllocator;

IRcBufAllocator* GetDefaultRcBufAllocator() noexcept {
    return &RcBufAllocator;
}
