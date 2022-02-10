#pragma once
#include "defs.h"

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TAtomicBlockCounter
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
struct TAtomicBlockCounter {
    TAtomic Data = 0;

    static constexpr ui64 BlockAFlag = (1ull << 63ull);
    static constexpr ui64 BlockBFlag = (1ull << 62ull);
    static constexpr ui64 BlockMask = BlockAFlag | BlockBFlag;
    static constexpr ui64 SeqnoShift = 46ull;
    static constexpr ui64 SeqnoMask = (0xFFFFull << SeqnoShift);
    static constexpr ui64 CounterMask = ~(BlockMask|SeqnoMask);

    struct TResult {
        bool PrevA = false; // Block A state before operation
        bool PrevB = false; // Block B state before operation
        bool A = false; // Block A state after operation
        bool B = false; // Block B state after operation
        ui16 Seqno = 0;
        ui16 PrevSeqno = 0;

        bool Toggled() const noexcept {
            return WasBlocked() ^ IsBlocked();
        }

        bool WasBlocked() const noexcept {
            return PrevA || PrevB;
        }

        bool IsBlocked() const noexcept {
            return A || B;
        }
    };

    TAtomicBlockCounter() noexcept {}

    bool IsBlocked() const noexcept;

    // Blocking
    void Block(ui64 flag, TResult& res) noexcept;
    void BlockA() noexcept {
        TResult res;
        Block(BlockAFlag, res);
    }
    void BlockB() noexcept {
        TResult res;
        Block(BlockBFlag, res);
    }
    void BlockA(TResult& res) noexcept {
        Block(BlockAFlag, res);
    }
    void BlockB(TResult& res) noexcept {
        Block(BlockBFlag, res);
    }

    // Unblocking
    void Unblock(ui64 flag, TResult& res) noexcept;
    void UnblockA() noexcept {
        TResult res;
        Unblock(BlockAFlag, res);
    }
    void UnblockB() noexcept {
        TResult res;
        Unblock(BlockBFlag, res);
    }
    void UnblockA(TResult& res) noexcept {
        Unblock(BlockAFlag, res);
    }
    void UnblockB(TResult& res) noexcept {
        Unblock(BlockBFlag, res);
    }

    // Returns counter on success, 0 iff is blocked
    ui64 Add(ui64 value) noexcept;
    ui64 Increment() noexcept {
        return Add(1);
    }

    ui64 Sub(ui64 value) noexcept;
    ui64 Decrement() noexcept {
        return Sub(1);
    }

    // Operations with BlockA attached to Counter overflow over specified threshold
    ui64 ThresholdAdd(ui64 value, ui64 threshold, TResult& res) noexcept;
    ui64 ThresholdSub(ui64 value, ui64 threshold, TResult& res) noexcept;
    ui64 ThresholdUpdate(ui64 threshold, TResult& res) noexcept;

    ui64 Get() const noexcept;

private:
    inline static ui64 CheckedAddCounter(ui64 prevData, ui64 value) noexcept;
    inline static ui64 CheckedSubCounter(ui64 prevData, ui64 value) noexcept;
    inline static void FillResult(ui64 prevData, ui64 data, TResult& res) noexcept;
    inline static bool GetBlocked(ui64 data) noexcept;
    inline static ui16 GetSeqno(ui64 data) noexcept;
    inline static ui64 GetCounter(ui64 data) noexcept;
    inline static ui64 ThresholdBlock(ui64 prevData, ui64 threshold) noexcept;
    inline static ui64 NextSeqno(ui64 data) noexcept;
};

} // NPDisk
} // NKikimr
