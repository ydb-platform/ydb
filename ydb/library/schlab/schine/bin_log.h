#pragma once
#include "defs.h"
#include <util/generic/fwd.h>
#include <util/generic/ptr.h>

namespace NKikimr {
namespace NSchLab {

class TBinLog {
    union TItem {
        ui64 Ui64;
        i64 I64;
        double Double;
    };

    TArrayHolder<TItem> Buffer;
    ui64 Used = 0;
    const ui64 Size = 1ull << 20ull;
    ui64 ReadPosition = 0;

public:
    TBinLog() {
        Buffer = TArrayHolder<TItem>(new TItem[Size]);
    }

    inline void Write(ui64 value) {
        Buffer[Used].Ui64 = value;
        ++Used;
    }

    inline void Write(i64 value) {
        Buffer[Used].I64 = value;
        ++Used;
    }

    inline void Write(double value) {
        Buffer[Used].Double = value;
        ++Used;
    }

    inline bool HasSpace(ui64 sizeItems) const {
        return sizeItems + Used <= Size;
    }

    inline ui64 ReadUi64() {
        ui64 result = Buffer[ReadPosition].Ui64;
        ++ReadPosition;
        return result;
    }

    inline i64 ReadI64() {
        i64 result = Buffer[ReadPosition].I64;
        ++ReadPosition;
        return result;
    }

    inline double ReadDouble() {
        double result = Buffer[ReadPosition].Double;
        ++ReadPosition;
        return result;
    }

    inline void Clear() {
        Used = 0;
        ReadPosition = 0;
    }

    inline void ResetReadPosition() {
        ReadPosition = 0;
    }

    inline ui64 ReadableItemCount() const {
        return Used - ReadPosition;
    }
};


} // NSchLab
} // NKikimr
