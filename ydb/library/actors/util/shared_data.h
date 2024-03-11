#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/system/types.h>
#include <util/system/compiler.h>
#include <util/generic/array_ref.h>

#include <atomic>

namespace NActors {

    class TSharedData {
    public:
        class IOwner {
        public:
            virtual ~IOwner() = default;

            virtual void Deallocate(char*) noexcept = 0;
        };

        struct TPrivateHeader {
            size_t AllocSize;
            size_t Pad;
        };

        static_assert(sizeof(TPrivateHeader) == 16, "TPrivateHeader has an unexpected size");

        struct THeader {
            std::atomic<size_t> RefCount;
            IOwner* Owner;

            explicit THeader(IOwner* owner)
                : RefCount{ 1 }
                , Owner(owner)
            {}
        };

        static_assert(sizeof(THeader) == 16, "THeader has an unexpected size");

        enum : size_t {
            PrivateHeaderSize = sizeof(TPrivateHeader),
            HeaderSize = sizeof(THeader),
            OverheadSize = PrivateHeaderSize + HeaderSize,
            MaxDataSize = (std::numeric_limits<size_t>::max() - OverheadSize)
        };

    public:
        TSharedData() noexcept
            : Data_(nullptr)
            , Size_(0)
        { }

        ~TSharedData() noexcept {
            Release();
        }

        TSharedData(const TSharedData& other) noexcept
            : Data_(other.Data_)
            , Size_(other.Size_)
        {
            AddRef();
        }

        TSharedData(TSharedData&& other) noexcept
            : Data_(other.Data_)
            , Size_(other.Size_)
        {
            other.Data_ = nullptr;
            other.Size_ = 0;
        }

        TSharedData& operator=(const TSharedData& other) noexcept {
            if (this != &other) {
                Release();
                Data_ = other.Data_;
                Size_ = other.Size_;
                AddRef();
            }
            return *this;
        }

        TSharedData& operator=(TSharedData&& other) noexcept {
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

        Y_FORCE_INLINE char* mutable_data() { Y_DEBUG_ABORT_UNLESS(IsPrivate()); return Data_; }
        Y_FORCE_INLINE char* mutable_begin() { Y_DEBUG_ABORT_UNLESS(IsPrivate()); return Data_; }
        Y_FORCE_INLINE char* mutable_end() { Y_DEBUG_ABORT_UNLESS(IsPrivate()); return Data_ + Size_; }

        Y_FORCE_INLINE const char* data() const { return Data_; }
        Y_FORCE_INLINE const char* begin() const { return Data_; }
        Y_FORCE_INLINE const char* end() const { return Data_ + Size_; }

        Y_FORCE_INLINE size_t size() const { return Size_; }

        /**
         * Trims data to the specified size
         * Underlying data is not reallocated
         * Returns trimmed amount in bytes
         */
        size_t TrimBack(size_t size) noexcept {
            size_t trimmed = 0;
            if (Size_ > size) {
                trimmed = Size_ - size;
                if (!size) {
                    Release();
                    Data_ = nullptr;
                }
                Size_ = size;
            }
            return trimmed;
        }

        /**
         * Copies data to new allocated buffer if data is shared
         * New container loses original owner
         * Returns pointer to mutable buffer
         */
        char* Detach() {
            if (IsShared()) {
                *this = TSharedData::Copy(data(), size());
            }
            return Data_;
        }

        /**
         * Returns a view of underlying data starting with pos and up to len bytes
         */
        TStringBuf Slice(size_t pos = 0, size_t len = -1) const noexcept {
            pos = Min(pos, Size_);
            len = Min(len, Size_ - pos);
            return { Data_ + pos, len };
        }

        explicit operator TStringBuf() const noexcept {
            return Slice();
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

        /**
         * Attach to pre-allocated data with a preceding THeader
         */
        static TSharedData AttachUnsafe(char* data, size_t size) noexcept {
            TSharedData result;
            result.Data_ = data;
            result.Size_ = size;
            return result;
        }

        /**
         * Make uninitialized buffer of the specified size
         */
        static TSharedData Uninitialized(size_t size) {
            return AttachUnsafe(Allocate(size), size);
        }

        /**
         * Make a copy of the specified data
         */
        static TSharedData Copy(const void* data, size_t size) {
            TSharedData result = Uninitialized(size);
            if (size) {
                ::memcpy(result.Data_, data, size);
            }
            return result;
        }

        /**
         * Make a copy of the specified data
         */
        static TSharedData Copy(TArrayRef<const char> data) {
            return Copy(data.data(), data.size());
        }

    private:
        Y_FORCE_INLINE THeader* Header() const noexcept {
            Y_DEBUG_ABORT_UNLESS(Data_);
            return reinterpret_cast<THeader*>(Data_ - sizeof(THeader));
        }

        static bool IsPrivate(THeader* header) noexcept {
            return 1 == header->RefCount.load(std::memory_order_relaxed);
        }

        void AddRef() noexcept {
            if (Data_) {
                Header()->RefCount.fetch_add(1, std::memory_order_relaxed);
            }
        }

        void Release() noexcept {
            if (Data_) {
                auto* header = Header();
                if (1 == header->RefCount.fetch_sub(1, std::memory_order_acq_rel)) {
                    if (auto* owner = header->Owner) {
                        owner->Deallocate(Data_);
                    } else {
                        Deallocate(Data_);
                    }
                }
            }
        }

    private:
        static char* Allocate(size_t size);
        static void Deallocate(char* data) noexcept;

    private:
        char* Data_;
        size_t Size_;
    };

}
