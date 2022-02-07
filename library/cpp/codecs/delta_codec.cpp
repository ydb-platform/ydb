#include "delta_codec.h"

namespace NCodecs {
    template <>
    TStringBuf TDeltaCodec<ui64, true>::MyName() {
        return "delta64-unsigned";
    }
    template <>
    TStringBuf TDeltaCodec<ui32, true>::MyName() {
        return "delta32-unsigned";
    }
    template <>
    TStringBuf TDeltaCodec<ui64, false>::MyName() {
        return "delta64-signed";
    }
    template <>
    TStringBuf TDeltaCodec<ui32, false>::MyName() {
        return "delta32-signed";
    }

}
