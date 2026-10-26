#pragma once

#include <util/generic/utility.h>
#include <util/generic/yexception.h>
#include <util/system/align.h>
#include <util/system/yassert.h>
#include <memory>
#include <optional>

namespace NKikimr::NUtil {

    /**
     * Memory pool with support for transactions
     */
    class TMemoryPool
        : private std::allocator<char>
    {
        using allocator_base = std::allocator<char>;
        using allocator_traits = std::allocator_traits<allocator_base>;

        struct TChunk {
            TChunk* Next;
            char* Ptr;
            size_t Left;

            TChunk(size_t size) noexcept
                : Next(nullptr)
            {
                Y_DEBUG_ABORT_UNLESS((((uintptr_t)this) % PLATFORM_DATA_ALIGN) == 0);
                Y_DEBUG_ABORT_UNLESS(size >= ChunkHeaderSize());
                Ptr = DataStart();
                Left = size - ChunkHeaderSize();
            }

            /**
             * Aligns size to the specified power of two
             */
            static constexpr size_t AlignSizeUp(size_t size, size_t alignment) {
                return size + ((-size) & (alignment - 1));
            }

            /**
             * Aligned chunk header size that takes expected alignment into account
             */
            static constexpr size_t ChunkHeaderSize() noexcept {
                return AlignSizeUp(sizeof(TChunk), PLATFORM_DATA_ALIGN);
            }

            char* ChunkStart() noexcept {
                return reinterpret_cast<char*>(this);
            }

            const char* ChunkStart() const noexcept {
                return reinterpret_cast<const char*>(this);
            }

            size_t ChunkSize() const noexcept {
                return Left + (Ptr - ChunkStart());
            }

            char* DataStart() noexcept {
                return ChunkStart() + ChunkHeaderSize();
            }

            const char* DataStart() const noexcept {
                return ChunkStart() + ChunkHeaderSize();
            }

            size_t Used() const noexcept {
                return Ptr - DataStart();
            }

            size_t Wasted() const noexcept {
                return ChunkHeaderSize() + Left;
            }

            void Reset() noexcept {
                Rollback(DataStart());
            }

            void Rollback(char* to) noexcept {
                char* data = Ptr;
                Ptr = to;
                Left += data - to;
            }

            inline char* Allocate(size_t size) noexcept {
                if (Left >= size) {
                    char* ptr = Ptr;

                    Ptr += size;
                    Left -= size;

                    return ptr;
                }

                return nullptr;
            }

            inline char* Allocate(size_t size, size_t align) noexcept {
                size_t pad = AlignUp(Ptr, align) - Ptr;

                if (char* ret = Allocate(pad + size)) {
                    return ret + pad;
                }

                return nullptr;
            }
        };

    public:
        TMemoryPool(size_t initial)
            : First(AllocateChunk(Max(initial, TChunk::ChunkHeaderSize() + PLATFORM_DATA_ALIGN)))
            , Current(First)
        {
            Total_ = Current->ChunkSize();
        }

        ~TMemoryPool() {
            TChunk* chunk = First;
            while (chunk) {
                TChunk* next = chunk->Next;
                FreeChunk(chunk);
                chunk = next;
            }
        }

        void* Allocate(size_t size) {
            return DoAllocate(AlignUp<size_t>(size, PLATFORM_DATA_ALIGN));
        }

        void* Allocate(size_t size, size_t align) {
            return DoAllocate(AlignUp<size_t>(size, PLATFORM_DATA_ALIGN), align);
        }

        void* Append(const void* data, size_t len) {
            void* ptr = this->Allocate(len);
            if (len > 0) {
                ::memcpy(ptr, data, len);
            }
            return ptr;
        }

        void BeginTransaction() {
            Y_ENSURE(!RollbackState_);
            auto& state = RollbackState_.emplace();
            state.Chunk = Current;
            state.Ptr = Current->Ptr;
            state.Spare = Spare;
            state.SparePtr = Spare ? Spare->Ptr : nullptr;
        }

        void CommitTransaction() {
            Y_ENSURE(RollbackState_);
            RollbackState_.reset();
        }

        void RollbackTransaction() {
            Y_ENSURE(RollbackState_);
            auto& state = *RollbackState_;
            // The cached tail can precede the transaction's starting chunk
            if (state.Spare) {
                Y_DEBUG_ABORT_UNLESS(state.Spare->Ptr >= state.SparePtr);
                const size_t used = state.Spare->Ptr - state.SparePtr;
                state.Spare->Rollback(state.SparePtr);
                Used_ -= used;
                Wasted_ += used;
            }
            DoRollback(state.Chunk, state.Ptr);
            Spare = state.Spare;
            RollbackState_.reset();
        }

        size_t Total() const noexcept {
            return Total_;
        }

        size_t Used() const noexcept {
            return Used_ + Current->Used();
        }

        size_t Wasted() const noexcept {
            return Wasted_ + Current->Wasted();
        }

        size_t Available() const noexcept {
            return Available_ + Current->Left + (Spare ? Spare->Left : 0);
        }

    private:
        TChunk* AllocateChunk(size_t hint) {
            size_t chunkSize = FastClp2(hint);
            if (chunkSize - hint >= (chunkSize >> 2)) {
                chunkSize -= chunkSize >> 2;
            }
            char* ptr = allocator_traits::allocate(*this, chunkSize);
            return new (ptr) TChunk(chunkSize);
        }

        void FreeChunk(TChunk* chunk) {
            char* ptr = chunk->ChunkStart();
            size_t chunkSize = chunk->ChunkSize();
            chunk->~TChunk();
            allocator_traits::deallocate(*this, ptr, chunkSize);
        }

        TChunk* AddChunk(size_t size) {
            Y_ENSURE(!Current->Next);
            size_t hint = Max(AlignUp<size_t>(sizeof(TChunk), PLATFORM_DATA_ALIGN) + size, Current->ChunkSize() + 1);
            TChunk* next = AllocateChunk(hint);
            Total_ += next->ChunkSize();
            Used_ += Current->Used();
            Wasted_ += Current->Wasted();
            RememberTail(Current);
            Current->Next = next;
            Current = next;
            return next;
        }

        bool NextChunk() {
            if (Current->Next) {
                Used_ += Current->Used();
                Wasted_ += Current->Wasted();
                RememberTail(Current);
                Current = Current->Next;
                Y_DEBUG_ABORT_UNLESS(Current->Ptr == Current->DataStart());
                Wasted_ -= Current->ChunkSize();
                Available_ -= Current->Left;
                return true;
            }

            return false;
        }

        void RememberTail(TChunk* chunk) noexcept {
            if (chunk->Left && (!Spare || chunk->Left > Spare->Left)) {
                Spare = chunk;
            }
        }

        template <class... TArgs>
        void* AllocateSpare(size_t size, TArgs... args) noexcept {
            if (Spare) {
                const size_t left = Spare->Left;
                if (auto* ptr = Spare->Allocate(size, args...)) {
                    const size_t used = left - Spare->Left;
                    Used_ += used;
                    Wasted_ -= used;
                    return ptr;
                }
            }
            return nullptr;
        }

        void DoRollback(TChunk* target, char* ptr) {
            if (target != Current) {
                TChunk* chunk = target;
                size_t nextUsed = chunk->Used();
                size_t nextWasted = chunk->Wasted();
                do {
                    // Remove previously added stats
                    Used_ -= nextUsed;
                    Wasted_ -= nextWasted;
                    // Switch to the next chunk in the chain
                    chunk = chunk->Next;
                    Y_ENSURE(chunk, "Rollback cannot find current chunk in the chain");
                    // Reset chunk and add it to stats as wasted/free space
                    nextUsed = chunk->Used();
                    nextWasted = chunk->Wasted();
                    chunk->Reset();
                    Wasted_ += chunk->ChunkSize();
                    Available_ += chunk->Left;
                } while (chunk != Current);
                Current = target;
            }
            target->Rollback(ptr);
        }

        void* DoAllocate(size_t size) {
            if (auto* ptr = AllocateSpare(size)) {
                return ptr;
            }
            do {
                if (auto* ptr = Current->Allocate(size)) {
                    return ptr;
                }
            } while (NextChunk());

            return AddChunk(size)->Allocate(size);
        }

        void* DoAllocate(size_t size, size_t align) {
            if (auto* ptr = AllocateSpare(size, align)) {
                return ptr;
            }
            do {
                if (auto* ptr = Current->Allocate(size, align)) {
                    return ptr;
                }
            } while (NextChunk());

            return AddChunk(size + align - 1)->Allocate(size, align);
        }

    private:
        struct TRollbackState {
            TChunk* Chunk;
            char* Ptr;
            TChunk* Spare;
            char* SparePtr;
        };

    private:
        TChunk* First;
        TChunk* Current;
        TChunk* Spare = nullptr;
        size_t Total_ = 0;
        size_t Used_ = 0;
        size_t Wasted_ = 0;
        size_t Available_ = 0;
        std::optional<TRollbackState> RollbackState_;
    };

} // namespace NKikimr::NUtil
