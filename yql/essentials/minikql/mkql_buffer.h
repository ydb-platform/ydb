#pragma once

#include "defs.h"

#include <yql/essentials/utils/chunked_buffer.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/noncopyable.h>
#include <util/stream/output.h>
#include <util/system/yassert.h>

namespace NKikimr::NMiniKQL {

    extern NMonitoring::TDynamicCounters::TCounterPtr PageBufferBytesWastedCounter;

class TPagedBuffer;

class TBufferPage : private TNonCopyable {
    friend class TPagedBuffer;
    static const size_t PageCapacity;

public:
    static const size_t PageAllocSize = 32 * 1024;

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

     inline size_t Wasted() const  {
        Y_ABORT_UNLESS(PageCapacity - Size_ >= 0);
        return PageCapacity - Size_;
     }

private:
    TBufferPage* Next_ = nullptr;
    size_t Size_ = 0;

    static TBufferPage* Allocate();
    static void Free(TBufferPage* page);

    static inline const TBufferPage* GetPage(const char* data) {
        Y_DEBUG_ABORT_UNLESS(data);
        return reinterpret_cast<const TBufferPage*>(data - sizeof(TBufferPage));
    }

    static inline TBufferPage* GetPage(char* data) {
        Y_DEBUG_ABORT_UNLESS(data);
        return reinterpret_cast<TBufferPage*>(data - sizeof(TBufferPage));
    }
};

class TPagedBuffer : private TNonCopyable {
  public:
    using TPtr = std::shared_ptr<TPagedBuffer>;
    using TConstPtr = std::shared_ptr<const TPagedBuffer>;

    TPagedBuffer() = default;
    ~TPagedBuffer() {
        if (Head_) {
            TBufferPage* curr = TBufferPage::GetPage(Head_);
            TBufferPage::GetPage(Tail_)->Size_ = TailSize_;
            while (curr) {
                auto drop = curr;
                curr = curr->Next_;
                TBufferPage::Free(drop);
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
        //                      + (Tail_ ? TailSize_ : 0);
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

    inline void EraseBack(size_t len) {
        Y_DEBUG_ABORT_UNLESS(Tail_ && TailSize_ >= len);
        TailSize_ -= len;
        (*PageBufferBytesWastedCounter) += len;
    }

    inline void Advance(size_t len) {
        if (Y_LIKELY(TailSize_ + len <= TBufferPage::PageCapacity)) {
            TailSize_ += len;
            (*PageBufferBytesWastedCounter) -= len;
            Y_ABORT_UNLESS(*PageBufferBytesWastedCounter >= 0, "Total wasted vs page wasted: %ld, %ld", PageBufferBytesWastedCounter->Val(), TBufferPage::GetPage(Tail_)->Wasted());
            return;
        }

        MKQL_ENSURE(len <= TBufferPage::PageCapacity, "Advance() size too big");
        AppendPage();
        TailSize_ = len;
        (*PageBufferBytesWastedCounter) -= len;
        Y_ABORT_UNLESS(*PageBufferBytesWastedCounter >= 0, "Total wasted vs page wasted: %ld, %ld", PageBufferBytesWastedCounter->Val(), TBufferPage::GetPage(Tail_)->Wasted());
    }

    inline void Append(char c) {
        Advance(1);
        *(Pos() - 1) = c;
    }

    inline void Append(const char* data, size_t size) {
        while (size) {
            if (TailSize_ == TBufferPage::PageCapacity) {
                AppendPage();
            }
            Y_DEBUG_ABORT_UNLESS(TailSize_ < TBufferPage::PageCapacity);

            size_t avail = TBufferPage::PageCapacity - TailSize_;
            size_t chunk = std::min(avail, size);
            std::memcpy(Pos(), data, chunk);
            TailSize_ += chunk;
            data += chunk;
            size -= chunk;
            (*PageBufferBytesWastedCounter) -= chunk;
            Y_ABORT_UNLESS(*PageBufferBytesWastedCounter >= 0, "Total wasted vs page wasted: %ld, %ld", PageBufferBytesWastedCounter->Val(), TBufferPage::GetPage(Tail_)->Wasted());
        }
    }

    static NYql::TChunkedBuffer AsChunkedBuffer(const TConstPtr& buf);
private:
    void AppendPage();

    char* Head_ = nullptr;
    char* Tail_ = nullptr;

    // TailSize_ is initialized as if last page is full, this way we can simplify check in Advance()
    size_t TailSize_ = TBufferPage::PageCapacity;
    size_t HeadReserve_ = 0;
    size_t ClosedPagesSize_ = 0;
};

void InitializeGlobalPagedBufferCounters(::NMonitoring::TDynamicCounterPtr root);

} // namespace NKikimr::NMiniKQL
