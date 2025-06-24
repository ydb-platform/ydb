#pragma once

#include "defs.h"

#if defined(PROFILE_MEMORY_ALLOCATIONS)
#   include <library/cpp/monlib/dynamic_counters/counters.h>
#endif

#include <yql/essentials/utils/chunked_buffer.h>

#include <util/generic/noncopyable.h>
#include <util/stream/output.h>
#include <util/system/yassert.h>

namespace NKikimr::NMiniKQL {

#if defined(PROFILE_MEMORY_ALLOCATIONS)
extern NMonitoring::TDynamicCounters::TCounterPtr TotalBytesWastedCounter;
void InitializeGlobalPagedBufferCounters(::NMonitoring::TDynamicCounterPtr root);
#endif

class TPagedBuffer;

class TBufferPage : private TNonCopyable {
    friend class TPagedBuffer;
    static const size_t DefaultPageCapacity;

public:
    static const size_t DefaultPageAllocSize = 128 * 1024;

    TBufferPage() = default;
    ~TBufferPage() = default;

    inline const TBufferPage* Next() const {
        return Next_;
    }

    inline TBufferPage* Next() {
        return Next_;
    }

    inline size_t Size() const {
        return Size_;
    }

    inline char* Data() {
        return reinterpret_cast<char*>(this + 1);
    }

    inline const char* Data() const {
        return reinterpret_cast<const char*>(this + 1);
    }

    inline void Clear() {
        Size_ = 0;
    }

private:
    TBufferPage* Next_ = nullptr;
    size_t Size_ = 0;

    static TBufferPage* Allocate(size_t pageAllocSize = DefaultPageAllocSize);
    static void Free(TBufferPage* page, size_t pageAllocSize = DefaultPageAllocSize);

    static inline const TBufferPage* GetPage(const char* data) {
        Y_DEBUG_ABORT_UNLESS(data);
        return reinterpret_cast<const TBufferPage*>(data - sizeof(TBufferPage));
    }

    static inline TBufferPage* GetPage(char* data) {
        Y_DEBUG_ABORT_UNLESS(data);
        return reinterpret_cast<TBufferPage*>(data - sizeof(TBufferPage));
    }
};

static constexpr bool IsValidPageAllocSize(size_t size) {
    return size <= std::numeric_limits<ui32>::max() && sizeof(TBufferPage) < size;
}

class TPagedBuffer : private TNonCopyable {
  public:
    using TPtr = std::shared_ptr<TPagedBuffer>;
    using TConstPtr = std::shared_ptr<const TPagedBuffer>;

    TPagedBuffer() = default;

    explicit TPagedBuffer(size_t pageAllocSize)
        : PageAllocSize_(pageAllocSize)
        , PageCapacity_(pageAllocSize - sizeof(TBufferPage))
    {
        Y_ENSURE(IsValidPageAllocSize(pageAllocSize));
    }

    ~TPagedBuffer() {
        if (Head_) {
            auto* tailPage = TBufferPage::GetPage(Tail_);
            tailPage->Size_ = TailSize_;

            TBufferPage* curr = TBufferPage::GetPage(Head_);
            while (curr) {
                auto drop = curr;
                curr = curr->Next_;
                TBufferPage::Free(drop, PageAllocSize_);
            }
        }
    }

    template<typename TFunc>
    inline void ForEachPage(TFunc f) const {
        if (!Head_) {
            return;
        }
        const TBufferPage* head = TBufferPage::GetPage(Head_);
        auto page = head;
        auto end = TBufferPage::GetPage(Tail_);

        while (page) {
            const char* src;
            size_t len;
            if (page == end) {
                src = Tail_;
                len = TailSize_;
            } else {
                src = page->Data();
                len = page->Size();
            }

            if (page == head) {
                src += HeadReserve_;
                len -= HeadReserve_;
            }

            if (len) {
                f(src, len);
            }
            page = (page == end) ? nullptr : page->Next();
        }
    }

    inline size_t Size() const {
        size_t sizeWithReserve = ClosedPagesSize_ + ((-size_t(Tail_ != nullptr)) & TailSize_);
        Y_DEBUG_ABORT_UNLESS(sizeWithReserve >= HeadReserve_);
        return sizeWithReserve - HeadReserve_;
    }

    template<typename TContainer>
    inline void CopyTo(TContainer& out) const {
        ForEachPage([&out](const char* data, size_t len) {
            out.insert(out.end(), data, data + len);
        });
    }

    inline void CopyTo(char* dst) const {
        ForEachPage([&dst](const char* data, size_t len) {
            std::memcpy(dst, data, len);
            dst += len;
        });
    }

    inline void CopyTo(IOutputStream& out) const {
        ForEachPage([&out](const char* data, size_t len) {
            out.Write(data, len);
        });
    }

    size_t ReservedHeaderSize() const {
        return HeadReserve_;
    }

    inline void ReserveHeader(size_t len) {
        Y_DEBUG_ABORT_UNLESS(len > 0);
        Y_DEBUG_ABORT_UNLESS(Head_ == Tail_);
        Y_DEBUG_ABORT_UNLESS(HeadReserve_ == 0);
        Advance(len);
        HeadReserve_ = len;
    }

    char* Header(size_t len) {
        if (len > HeadReserve_) {
            return nullptr;
        }
        Y_DEBUG_ABORT_UNLESS(Head_);
        HeadReserve_ -= len;
        return Head_ + HeadReserve_;
    }

    // buffer-style operations with last page
    inline char* Pos() const {
        Y_DEBUG_ABORT_UNLESS(Tail_);
        return Tail_ + TailSize_;
    }

    inline void Clear() {
        // TODO: not wasted or never called?
        Tail_ = Head_;
        ClosedPagesSize_ = HeadReserve_ = 0;
        TailSize_ = (-size_t(Tail_ == nullptr)) & PageCapacity_;
    }

    inline void EraseBack(size_t len) {
        Y_DEBUG_ABORT_UNLESS(Tail_ && TailSize_ >= len);
        TailSize_ -= len;
#if defined(PROFILE_MEMORY_ALLOCATIONS)
        TotalBytesWastedCounter->Add(len);
#endif
    }

    inline void Advance(size_t len) {
        if (Y_LIKELY(TailSize_ + len <= PageCapacity_)) {
            TailSize_ += len;
#if defined(PROFILE_MEMORY_ALLOCATIONS)
            TotalBytesWastedCounter->Sub(len);
#endif
            return;
        }

        MKQL_ENSURE(len <= PageCapacity_, "Advance() size too big");
        AppendPage();
        TailSize_ = len;
#if defined(PROFILE_MEMORY_ALLOCATIONS)
        TotalBytesWastedCounter->Sub(len);
#endif
    }

    inline void Append(char c) {
        Advance(1);
        *(Pos() - 1) = c;
    }

    inline void Append(const char* data, size_t size) {
        while (size) {
            if (TailSize_ == PageCapacity_) {
                AppendPage();
            }
            Y_DEBUG_ABORT_UNLESS(TailSize_ < PageCapacity_);

            size_t avail = PageCapacity_ - TailSize_;
            size_t chunk = std::min(avail, size);
            std::memcpy(Pos(), data, chunk);
            TailSize_ += chunk;
            data += chunk;
            size -= chunk;
#if defined(PROFILE_MEMORY_ALLOCATIONS)
            TotalBytesWastedCounter->Sub(chunk);
#endif
        }
    }

    static NYql::TChunkedBuffer AsChunkedBuffer(const TConstPtr& buf);
private:
    void AppendPage();

    const size_t PageAllocSize_ = TBufferPage::DefaultPageAllocSize;
    const size_t PageCapacity_ = TBufferPage::DefaultPageCapacity;

    char* Head_ = nullptr;
    char* Tail_ = nullptr;

    // TailSize_ is initialized as if last page is full, this way we can simplifiy check in Advance()
    size_t TailSize_ = PageCapacity_;
    size_t HeadReserve_ = 0;
    size_t ClosedPagesSize_ = 0;
};

} // namespace NKikimr::NMiniKQL
