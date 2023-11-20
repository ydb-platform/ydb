#pragma once

#include <util/generic/utility.h>
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

        void BeginTransaction() noexcept {
            Y_ABORT_UNLESS(!RollbackState_);
            auto& state = RollbackState_.emplace();
            state.Chunk = Current;
            state.Ptr = Current->Ptr;
        }

        void CommitTransaction() noexcept {
            Y_ABORT_UNLESS(RollbackState_);
            RollbackState_.reset();
        }

        void RollbackTransaction() noexcept {
            Y_ABORT_UNLESS(RollbackState_);
            auto& state = *RollbackState_;
            DoRollback(state.Chunk, state.Ptr);
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
            return Available_ + Current->Left;
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
            Y_ABORT_UNLESS(!Current->Next);
            size_t hint = Max(AlignUp<size_t>(sizeof(TChunk), PLATFORM_DATA_ALIGN) + size, Current->ChunkSize() + 1);
            TChunk* next = AllocateChunk(hint);
            Total_ += next->ChunkSize();
            Used_ += Current->Used();
            Wasted_ += Current->Wasted();
            Current->Next = next;
            Current = next;
            return next;
        }

        bool NextChunk() {
            if (Current->Next) {
                Used_ += Current->Used();
                Wasted_ += Current->Wasted();
                Current = Current->Next;
                Y_DEBUG_ABORT_UNLESS(Current->Ptr == Current->DataStart());
                Wasted_ -= Current->ChunkSize();
                Available_ -= Current->Left;
                return true;
            }

            return false;
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
                    Y_ABORT_UNLESS(chunk, "Rollback cannot find current chunk in the chain");
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
            do {
                if (auto* ptr = Current->Allocate(size)) {
                    return ptr;
                }
            } while (NextChunk());

            return AddChunk(size)->Allocate(size);
        }

        void* DoAllocate(size_t size, size_t align) {
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
        };

    private:
        TChunk* First;
        TChunk* Current;
        size_t Total_ = 0;
        size_t Used_ = 0;
        size_t Wasted_ = 0;
        size_t Available_ = 0;
        std::optional<TRollbackState> RollbackState_;
    };

} // namespace NKikimr::NUtil
