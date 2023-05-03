#pragma once

#include "defs.h"

#include <util/generic/noncopyable.h>
#include <util/stream/output.h>
#include <util/system/yassert.h>

namespace NKikimr {

namespace NMiniKQL {

class TPagedBuffer;

class TBufferPage : private TNonCopyable {
    friend class TPagedBuffer;
    static const size_t PageAllocSize = 128 * 1024;
    static const size_t PageCapacity;

public:
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

    static TBufferPage* Allocate();
    static void Free(TBufferPage* page);

    static inline const TBufferPage* GetPage(const char* data) {
        Y_VERIFY_DEBUG(data);
        return reinterpret_cast<const TBufferPage*>(data - sizeof(TBufferPage));
    }

    static inline TBufferPage* GetPage(char* data) {
        Y_VERIFY_DEBUG(data);
        return reinterpret_cast<TBufferPage*>(data - sizeof(TBufferPage));
    }
};

class TPagedBuffer : private TMoveOnly {
public:
    TPagedBuffer() = default;
    TPagedBuffer(TPagedBuffer&& other)
        : Head_(other.Head_)
        , Tail_(other.Tail_)
        , TailSize_(other.TailSize_)
        , HeadReserve_(other.HeadReserve_)
        , ClosedPagesSize_(other.ClosedPagesSize_)
    {
        other.Reset();
    }

    ~TPagedBuffer() {
        Deallocate();
    }

    TPagedBuffer& operator=(TPagedBuffer&& other) noexcept {
        Deallocate();
        std::memcpy(this, &other, sizeof(TPagedBuffer));
        other.Reset();
        return *this;
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
        Y_VERIFY_DEBUG(sizeWithReserve >= HeadReserve_);
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
        Y_VERIFY_DEBUG(len > 0);
        Y_VERIFY_DEBUG(Head_ == Tail_);
        Y_VERIFY_DEBUG(HeadReserve_ == 0);
        Advance(len);
        HeadReserve_ = len;
    }

    char* Header(size_t len) {
        if (len > HeadReserve_) {
            return nullptr;
        }
        Y_VERIFY_DEBUG(Head_);
        HeadReserve_ -= len;
        return Head_ + HeadReserve_;
    }

    // buffer-style operations with last page
    inline char* Pos() const {
        Y_VERIFY_DEBUG(Tail_);
        return Tail_ + TailSize_;
    }

    inline void Clear() {
        Tail_ = Head_;
        ClosedPagesSize_ = HeadReserve_ = 0;
        //        = Tail_ ? 0 : TBufferPage::PageAllocSize;
        TailSize_ = (-size_t(Tail_ == nullptr)) & TBufferPage::PageCapacity;
    }

    inline void EraseBack(size_t len) {
        Y_VERIFY_DEBUG(Tail_ && TailSize_ >= len);
        TailSize_ -= len;
    }

    inline void Advance(size_t len) {
        if (Y_LIKELY(TailSize_ + len <= TBufferPage::PageCapacity)) {
            TailSize_ += len;
            return;
        }

        MKQL_ENSURE(len <= TBufferPage::PageCapacity, "Advance() size too big");
        AppendPage();
        TailSize_ = len;
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
            Y_VERIFY_DEBUG(TailSize_ < TBufferPage::PageCapacity);

            size_t avail = TBufferPage::PageCapacity - TailSize_;
            size_t chunk = std::min(avail, size);
            std::memcpy(Pos(), data, chunk);
            TailSize_ += chunk;
            data += chunk;
            size -= chunk;
        }
    }

private:
    void AppendPage();

    inline void Reset() noexcept {
        Head_ = Tail_ = nullptr;
        TailSize_ = TBufferPage::PageCapacity;
        ClosedPagesSize_ = HeadReserve_ = 0;
    }

    void Deallocate() noexcept {
        if (Head_) {
            TBufferPage *curr = TBufferPage::GetPage(Head_);
            while (curr) {
                auto drop = curr;
                curr = curr->Next_;
                TBufferPage::Free(drop);
            }
        }
    }

    char* Head_ = nullptr;
    char* Tail_ = nullptr;

    // TailSize_ is initialized as if last page is full, this way we can simplifiy check in Advance()
    size_t TailSize_ = TBufferPage::PageCapacity;
    size_t HeadReserve_ = 0;
    size_t ClosedPagesSize_ = 0;
};

} // NMiniKQL

} // NKikimr
