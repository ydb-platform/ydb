#include "pfor_codec.h"

namespace NCodecs {
    template <>
    TStringBuf TPForCodec<ui64, true>::MyName() {
        return "pfor-delta64-sorted";
    }
    template <>
    TStringBuf TPForCodec<ui32, true>::MyName() {
        return "pfor-delta32-sorted";
    }

    template <>
    TStringBuf TPForCodec<ui64, false>::MyName() {
        return "pfor-ui64";
    }
    template <>
    TStringBuf TPForCodec<ui32, false>::MyName() {
        return "pfor-ui32";
    }

}
