#include "contiguous_data.h"

size_t checkedSum(size_t a, size_t b) {
    if (a > std::numeric_limits<size_t>::max() - b) {
        throw std::length_error("Allocate size overflow");
    }
    return a + b;
}

char* TContiguousData::TBackend::TSharedDataControllingOwner::Allocate(size_t size, size_t headroom, size_t tailroom) {
    char* data = nullptr;

    size_t fullSize = checkedSum(size, checkedSum(headroom, tailroom));

    if (fullSize > 0) {
        auto allocSize = checkedSum(fullSize, OverheadSize);
        char* raw = static_cast<char*>(y_allocate(allocSize));

        auto isAligned = [](const void * ptr, std::uintptr_t alignment) noexcept {
            auto iptr = reinterpret_cast<std::uintptr_t>(ptr);
            return !(iptr % alignment);
        };
        Y_VERIFY(isAligned(raw, alignof(TBackend::TCookies)));
        TBackend::TCookies* cookies = new(raw) TBackend::TCookies();

        auto* header = reinterpret_cast<NActors::TSharedData::THeader*>(raw + PrivateHeaderSize);
        header->RefCount = 1;
        header->Owner = &TContiguousData::TBackend::SharedDataOwner;

        data = raw + OverheadSize;
        cookies->Begin = data + headroom;
        cookies->End = data + headroom + size;
    }
    return data;
}

bool TContiguousData::TBackend::TSharedDataControllingOwner::CheckControlled(const char *data) noexcept {
    if (data != nullptr) {
        auto* header = reinterpret_cast<const NActors::TSharedData::THeader*>(data - HeaderSize);
#ifdef KIKIMR_TRACE_CONTIGUOUS_DATA_GROW
        // WARNING: with this definition method should be called ONLY on instances
        // which passed through TBackTracingOwner::FakeOwner
        const TBackTracingOwner* owner = reinterpret_cast<const TBackTracingOwner*>(header->Owner);
        return owner->GetRealOwner() == &TContiguousData::TBackend::SharedDataOwner;
#else
        return header->Owner == &TContiguousData::TBackend::SharedDataOwner;
#endif
    }
    return false;
}
