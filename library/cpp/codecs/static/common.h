#pragma once

#include <util/string/hex.h>
#include <util/digest/city.h>
#include <util/system/byteorder.h>

namespace NCodecs {
    template <class T>
    ui64 DataSignature(const T& t) {
        static_assert(!std::is_scalar<T>::value, "no scalars");
        return CityHash64(t.data(), t.size());
    }

    template <class T>
    TString HexWriteScalar(T t) { 
        static_assert(std::is_scalar<T>::value, "scalars only");
        t = LittleToBig(t);
        TString res = HexEncode(&t, sizeof(t)); 
        res.to_lower();
        return res;
    }

    template <class T>
    T HexReadScalar(TStringBuf s) {
        static_assert(std::is_scalar<T>::value, "scalars only");
        T t = 0;
        HexDecode(s.data(), Min(s.size(), sizeof(T)), &t);
        t = BigToLittle(t);
        return t;
    }

}
