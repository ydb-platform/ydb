#include "blobstorage_pdisk_util_atomicblockcounter.h"

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TAtomicBlockCounter
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
bool TAtomicBlockCounter::IsBlocked() const noexcept {
    return GetBlocked(AtomicGet(Data));
}

void TAtomicBlockCounter::Block(ui64 flag, TResult& res) noexcept {
    while (true) {
        ui64 prevData = AtomicGet(Data);
        if (prevData & flag) {
            FillResult(prevData, prevData, res);
            return; // Already blocked
        }
        ui64 data = NextSeqno(prevData | flag);
        if (AtomicCas(&Data, data, prevData)) {
            FillResult(prevData, data, res);
            return;
        }
    }
}

void TAtomicBlockCounter::Unblock(ui64 flag, TResult& res) noexcept {
    while (true) {
        ui64 prevData = AtomicGet(Data);
        if (!(prevData & flag)) {
            FillResult(prevData, prevData, res);
            return; // Already unblocked
        }
        ui64 data = NextSeqno(prevData & ~flag);
        if (AtomicCas(&Data, data, prevData)) {
            FillResult(prevData, data, res);
            return;
        }
    }
}

ui64 TAtomicBlockCounter::Add(ui64 value) noexcept {
    Y_VERIFY_S(value > 0, "zero value# " << value);
    while (true) {
        ui64 prevData = AtomicGet(Data);
        if (GetBlocked(prevData)) {
            return 0; // Add is forbidden iff blocked
        }
        ui64 data = NextSeqno(CheckedAddCounter(prevData, value));
        if (AtomicCas(&Data, data, prevData)) {
            return GetCounter(data);
        }
    }
}

ui64 TAtomicBlockCounter::Sub(ui64 value) noexcept {
    Y_VERIFY_S(value > 0, "zero value# " << value);
    while (true) {
        ui64 prevData = AtomicGet(Data);
        ui64 data = NextSeqno(CheckedSubCounter(prevData, value));
        if (AtomicCas(&Data, data, prevData)) {
            return GetCounter(data);
        }
    }
}

ui64 TAtomicBlockCounter::ThresholdAdd(ui64 value, ui64 threshold, TAtomicBlockCounter::TResult& res) noexcept {
    Y_VERIFY_S(value > 0, "zero value# " << value);
    while (true) {
        ui64 prevData = AtomicGet(Data);
        if (GetBlocked(prevData)) { // Add is forbidden iff blocked
            FillResult(prevData, prevData, res);
            return 0;
        }
        ui64 data = NextSeqno(ThresholdBlock(CheckedAddCounter(prevData, value), threshold));
        if (AtomicCas(&Data, data, prevData)) {
            FillResult(prevData, data, res);
            return GetCounter(data);
        }
    }
}

ui64 TAtomicBlockCounter::ThresholdSub(ui64 value, ui64 threshold, TAtomicBlockCounter::TResult& res) noexcept {
    Y_VERIFY_S(value > 0, "zero value# " << value);
    while (true) {
        ui64 prevData = AtomicGet(Data);
        ui64 data = NextSeqno(ThresholdBlock(CheckedSubCounter(prevData, value), threshold));
        if (AtomicCas(&Data, data, prevData)) {
            FillResult(prevData, data, res);
            return GetCounter(data);
        }
    }
}

ui64 TAtomicBlockCounter::ThresholdUpdate(ui64 threshold, TAtomicBlockCounter::TResult& res) noexcept {
    while (true) {
        ui64 prevData = AtomicGet(Data);
        ui64 data = NextSeqno(ThresholdBlock(prevData, threshold));
        if (AtomicCas(&Data, data, prevData)) {
            FillResult(prevData, data, res);
            return GetCounter(data);
        }
    }
}

ui64 TAtomicBlockCounter::Get() const noexcept {
    return GetCounter(AtomicGet(Data));
}

ui64 TAtomicBlockCounter::CheckedAddCounter(ui64 prevData, ui64 value) noexcept {
    Y_VERIFY_S(!(value & ~CounterMask), "invalid value# " << value);
    Y_VERIFY_S(!((GetCounter(prevData) + value) & ~CounterMask),
             "overflow value# " << value << " prevData# " << GetCounter(prevData));
    return prevData + value; // No overflow, so higher bits are untouched
}

ui64 TAtomicBlockCounter::CheckedSubCounter(ui64 prevData, ui64 value) noexcept {
    Y_VERIFY_S(!(value & ~CounterMask), "invalid value# " << value);
    Y_VERIFY_S(!((GetCounter(prevData) - value) & ~CounterMask),
             "underflow value# " << value << " prevData# " << GetCounter(prevData));
    return prevData - value; // No underflow, so higher bits are untouched
}

void TAtomicBlockCounter::FillResult(ui64 prevData, ui64 data, TAtomicBlockCounter::TResult& res) noexcept {
    res.PrevA = prevData & BlockAFlag;
    res.PrevB = prevData & BlockBFlag;
    res.A = data & BlockAFlag;
    res.B = data & BlockBFlag;
    res.Seqno = GetSeqno(data);
    res.PrevSeqno = GetSeqno(prevData);
}

bool TAtomicBlockCounter::GetBlocked(ui64 data) noexcept {
    return data & BlockMask;
}

ui16 TAtomicBlockCounter::GetSeqno(ui64 data) noexcept {
    return (data & SeqnoMask) >> SeqnoShift;
}

ui64 TAtomicBlockCounter::GetCounter(ui64 data) noexcept {
    return data & CounterMask;
}

ui64 TAtomicBlockCounter::ThresholdBlock(ui64 data, ui64 threshold) noexcept {
    if (GetCounter(data) > threshold) {
        return data | BlockAFlag;
    } else {
        return data & ~BlockAFlag;
    }
}

ui64 TAtomicBlockCounter::NextSeqno(ui64 data) noexcept {
    if (GetSeqno(data) == Max<ui16>()) { // Overflow case
        return data & ~SeqnoMask;
    } else { // No overflow case
        return data + (1ull << SeqnoShift);
    }
}

} // NPDisk
} // NKikimr
