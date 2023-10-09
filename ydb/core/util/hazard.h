#pragma once

#include "lf_stack.h"
#include "fast_tls.h"

#include <util/generic/ptr.h>
#include <util/system/align.h>
#include <util/system/compiler.h>
#include <util/system/yassert.h>

#include <atomic>

namespace NKikimr {

    class THazardDomain;
    class THazardCache;

    class THazardPointer
        : public TLockFreeIntrusiveStackItem<THazardPointer>
    {
        friend class THazardDomain;
        friend class THazardCache;

    private:
        THazardPointer() noexcept = default;
        ~THazardPointer() noexcept = default;

    public:
        void* Protect(const std::atomic<void*>& ptr) noexcept {
            void* p = ptr.load(std::memory_order_acquire);
            for (;;) {
                // N.B. store/load pair must be sequentially consistent
                ProtectedPointer.store(p);
                if (!p) {
                    break;
                }
                auto* check = ptr.load();
                if (check == p) {
                    break;
                }
                p = check;
            }
            return p;
        }

        void Clear() noexcept {
            ProtectedPointer.store(nullptr);
        }

    private:
        std::atomic<void*> ProtectedPointer{ nullptr };
        THazardPointer* CachedNext = nullptr;
    };

    /**
     * A local cache for hazard pointers, there is no need to use it directly
     */
    class THazardCache {
    public:
        THazardPointer* AcquireCached() noexcept {
            if (auto* ptr = Head) {
                Head = ptr->CachedNext;
                ptr->CachedNext = nullptr;
                --Size_;
                return ptr;
            }
            return nullptr;
        }

        void ReleaseCached(THazardPointer* ptr) noexcept {
            ptr->CachedNext = Head;
            Head = ptr;
            ++Size_;
        }

        size_t Size() const {
            return Size_;
        }

    private:
        THazardPointer* Head = nullptr;
        size_t Size_ = 0;
    };

    class THazardDomain {
    private:
        static constexpr size_t MaxCached = 4;
        static constexpr size_t MinChunkSize = 16;
        static constexpr size_t MaxChunkSize = 128;

        struct TChunk {
            const size_t Offset;
            const size_t Count;
            TChunk* Next;

            TChunk(size_t offset, size_t count)
                : Offset(offset)
                , Count(count)
                , Next(nullptr)
            { }

            THazardPointer* Pointers() {
                char* ptr = reinterpret_cast<char*>(this) + PointersOffset();
                return reinterpret_cast<THazardPointer*>(ptr);
            }

            static size_t PointersOffset() {
                return AlignUp(sizeof(TChunk), alignof(THazardPointer));
            }
        };

        static TChunk* AllocateChunk(size_t offset, size_t count);

        static void FreeChunk(TChunk* chunk) noexcept;

    public:
        THazardDomain() noexcept
            : FreeList()
            , LocalCache()
        { }

        ~THazardDomain() noexcept;

        size_t MaxPointers() const {
            return GlobalCount.load(std::memory_order_acquire);
        }

        THazardPointer* Acquire();

        void Release(THazardPointer* ptr) noexcept;

        void DrainLocalCache();

        template<class Callback>
        void CollectHazards(const Callback& callback) const;

    private:
        std::atomic<TChunk*> GlobalList{ nullptr };
        std::atomic<size_t> GlobalCount{ 0 };
        TLockFreeIntrusiveStack<THazardPointer> FreeList;
        TFastThreadLocal<THazardCache> LocalCache;
    };

    template<class Callback>
    inline void THazardDomain::CollectHazards(const Callback& callback) const {
        const size_t count = GlobalCount.load(std::memory_order_acquire);
        auto* chunk = GlobalList.load(std::memory_order_acquire);
        while (chunk) {
            if (chunk->Offset < count) {
                auto* ptr = chunk->Pointers();
                const size_t local = Min(chunk->Count, count - chunk->Offset);
                for (size_t index = 0; index < local; ++index, ++ptr) {
                    if (void* p = ptr->ProtectedPointer.load()) {
                        callback(p);
                    }
                }
            }
            chunk = chunk->Next;
        }
    }

    class TAutoHazardPointer {
    public:
        TAutoHazardPointer() noexcept
            : Domain(nullptr)
            , Pointer(nullptr)
        { }

        explicit TAutoHazardPointer(THazardDomain& domain) noexcept
            : Domain(&domain)
            , Pointer(nullptr)
        { }

        ~TAutoHazardPointer() noexcept {
            Clear();
        }

        TAutoHazardPointer(TAutoHazardPointer&& rhs) noexcept
            : Domain(rhs.Domain)
            , Pointer(rhs.Pointer)
        {
            rhs.Pointer = nullptr;
        }

        TAutoHazardPointer& operator=(TAutoHazardPointer&& rhs) noexcept {
            if (Y_LIKELY(this != &rhs)) {
                Clear();
                Domain = rhs.Domain;
                Pointer = rhs.Pointer;
                rhs.Pointer = nullptr;
            }
            return *this;
        }

        void* Protect(const std::atomic<void*>& ptr) noexcept {
            if (!Pointer) {
                Y_ABORT_UNLESS(Domain, "Uninitialized hazard pointer");
                Pointer = Domain->Acquire();
            }
            return Pointer->Protect(ptr);
        }

        void Clear() noexcept {
            if (Pointer) {
                Pointer->Clear();
                Domain->Release(Pointer);
                Pointer = nullptr;
            }
        }

    private:
        THazardDomain* Domain;
        THazardPointer* Pointer;
    };

}
