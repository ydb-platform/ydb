#pragma once

#include <atomic>

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/system/types.h>
#include <util/system/compiler.h>
#include <util/generic/array_ref.h>
#include <util/system/sys_alloc.h>

namespace NDetail {

struct TRcBufInternalBackend {
public:
    struct TCookies {
        using TSelf = TCookies;
        std::atomic<const char*> Begin;
        std::atomic<const char*> End;

        static size_t BytesToAligned(size_t size) {
            bool misaligned = size % alignof(TSelf);
            return misaligned ? alignof(TSelf) - size % alignof(TSelf) : 0;
        }

        static size_t BytesToAlloc(size_t size) {
            return size + BytesToAligned(size) + sizeof(TSelf);
        }
    };
private:
    // to be binary compatible with TSharedData
    struct THeader : public TCookies {
        TAtomic RefCount;
        ui64 Zero = 0;
    };

    enum : size_t {
        HeaderSize = sizeof(THeader),
        OverheadSize = HeaderSize,
        MaxDataSize = (std::numeric_limits<size_t>::max() - OverheadSize)
    };

public:
    TRcBufInternalBackend() noexcept
        : Data_(nullptr)
        , Size_(0)
    { }

    ~TRcBufInternalBackend() noexcept {
        Release();
    }

    TRcBufInternalBackend(const TRcBufInternalBackend& other) noexcept
        : Data_(other.Data_)
        , Size_(other.Size_)
    {
        AddRef();
    }

    TRcBufInternalBackend(TRcBufInternalBackend&& other) noexcept
        : Data_(other.Data_)
        , Size_(other.Size_)
    {
        other.Data_ = nullptr;
        other.Size_ = 0;
    }

    TRcBufInternalBackend& operator=(const TRcBufInternalBackend& other) noexcept {
        if (this != &other) {
            Release();
            Data_ = other.Data_;
            Size_ = other.Size_;
            AddRef();
        }
        return *this;
    }

    TRcBufInternalBackend& operator=(TRcBufInternalBackend&& other) noexcept {
        if (this != &other) {
            Release();
            Data_ = other.Data_;
            Size_ = other.Size_;
            other.Data_ = nullptr;
            other.Size_ = 0;
        }
        return *this;
    }

    Y_FORCE_INLINE explicit operator bool() const { return Size_ > 0; }

    Y_FORCE_INLINE char* mutable_data() { return Data(); }
    Y_FORCE_INLINE char* mutable_begin() { return Data(); }
    Y_FORCE_INLINE char* mutable_end() { return Data() + Size_; }

    Y_FORCE_INLINE const char* data() const { return Data(); }
    Y_FORCE_INLINE const char* begin() const { return Data(); }
    Y_FORCE_INLINE const char* end() const { return Data() + Size_; }

    Y_FORCE_INLINE size_t size() const { return Size_; }

    /**
     * Copies data to new allocated buffer if data is shared
     * New container loses original owner
     * Returns pointer to mutable buffer
     */
    char* Detach() {
        if (IsShared()) {
            *this = TRcBufInternalBackend::Copy(data(), size());
        }
        return Data_;
    }

    bool IsPrivate() const {
        return Data_ ? IsPrivate(Header()) : true;
    }

    bool IsShared() const {
        return !IsPrivate();
    }

    TString ToString() const {
        return TString(data(), size());
    }

    TCookies* GetCookies() {
        return Header();
    }

    /**
     * Attach to pre-allocated data with a preceding THeader
     */
    static TRcBufInternalBackend AttachUnsafe(char* data, size_t size) noexcept {
        TRcBufInternalBackend result;
        result.Data_ = data;
        result.Size_ = size;
        return result;
    }

    /**
     * Make uninitialized buffer of the specified size
     */
    static TRcBufInternalBackend Uninitialized(size_t size, size_t headroom = 0, size_t tailroom = 0) {
        size_t fullSize = checkedSum(size, checkedSum(headroom, tailroom));
        return AttachUnsafe(Allocate(size, headroom, tailroom), fullSize);
    }

    /**
     * Make a copy of the specified data
     */
    static TRcBufInternalBackend Copy(const void* data, size_t size) {
        TRcBufInternalBackend result = Uninitialized(size);
        if (size) {
            ::memcpy(result.Data(), data, size);
        }
        return result;
    }

private:
    Y_FORCE_INLINE THeader* Header() const noexcept {
        Y_DEBUG_ABORT_UNLESS(Data_);
        return reinterpret_cast<THeader*>(Data_);
    }

    Y_FORCE_INLINE char* Data() const noexcept {
        Y_DEBUG_ABORT_UNLESS(Data_);
        return Data_ + OverheadSize;
    }

    static bool IsPrivate(THeader* header) noexcept {
        return 1 == AtomicGet(header->RefCount);
    }

    void AddRef() noexcept {
        if (Data_) {
            AtomicIncrement(Header()->RefCount);
        }
    }

    void Release() noexcept {
        if (Data_) {
            auto* header = Header();
            if (IsPrivate(header) || 0 == AtomicDecrement(header->RefCount)) {
                Deallocate(Data_);
            }
        }
    }

private:
    static size_t checkedSum(size_t a, size_t b) {
        if (a > std::numeric_limits<size_t>::max() - b) {
            throw std::length_error("Allocate size overflow");
        }
        return a + b;
    }

    static char* Allocate(size_t size, size_t headroom = 0, size_t tailroom = 0) {
        char* data = nullptr;
        size_t fullSize = checkedSum(size, checkedSum(headroom, tailroom));
        if (fullSize > 0) {
            if (fullSize >= MaxDataSize) {
                throw std::length_error("Allocate size overflow");
            }
            auto allocSize = OverheadSize + fullSize;
            char* raw = reinterpret_cast<char*>(y_allocate(allocSize));

            auto* header = reinterpret_cast<THeader*>(raw);
            header->Begin = raw + OverheadSize + headroom;
            header->End = raw + allocSize - tailroom;
            header->RefCount = 1;

            data = raw;
        }

        return data;
    }

    static void Deallocate(char* data) noexcept {
        if (data) {
            char* raw = data;

            y_deallocate(raw);
        }
    }

private:
    char* Data_;
    size_t Size_;
};

} // namespace NDetail
