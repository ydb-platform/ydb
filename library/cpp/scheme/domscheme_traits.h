#pragma once

#include "scheme.h"
#include <util/string/cast.h>

struct TSchemeTraits {
    using TValue = NSc::TValue;
    using TValueRef = TValue*;
    using TConstValueRef = const TValue*;
    using TStringType = TStringBuf; 

    // anyvalue defaults
    template <class T>
    static inline TValue Value(T&& t) {
        return TValue(std::forward<T>(t));
    }

    template <class T>
    static inline TValue Value(std::initializer_list<T> t) {
        return TValue().SetArray().AppendAll(t);
    }

    static inline TValueRef Ref(TValue& v) {
        return &v;
    }

    static inline TConstValueRef Ref(const TValue& v) {
        return &v;
    }

    // common ops
    static inline bool IsNull(TConstValueRef v) {
        return v->IsNull();
    }

    static inline TString ToJson(TConstValueRef v) {
        return v->ToJson();
    }

    // struct ops
    static inline TValueRef GetField(TValueRef v, const TStringBuf& name) {
        return &(*v)[name];
    }

    static inline TConstValueRef GetField(TConstValueRef v, const TStringBuf& name) {
        return &(*v)[name];
    }

    // array ops
    static bool IsArray(TConstValueRef v) {
        return v->IsArray();
    }

    static inline void ArrayClear(TValueRef v) {
        v->SetArray();
        v->ClearArray();
    }

    using TArrayIterator = size_t;

    static inline TValueRef ArrayElement(TValueRef v, TArrayIterator n) {
        return &(*v)[n];
    }

    static inline TConstValueRef ArrayElement(TConstValueRef v, TArrayIterator n) {
        return &(*v)[n];
    }

    static inline size_t ArraySize(TConstValueRef v) {
        return v->GetArray().size();
    }

    static inline TArrayIterator ArrayBegin(TConstValueRef) {
        return 0;
    }

    static inline TArrayIterator ArrayEnd(TConstValueRef v) {
        return ArraySize(v);
    }

    // dict ops
    static bool IsDict(TConstValueRef v) {
        return v->IsDict();
    }

    static inline void DictClear(TValueRef v) {
        v->SetDict();
        v->ClearDict();
    }

    static inline TValueRef DictElement(TValueRef v, TStringBuf key) {
        return &(*v)[key];
    }

    static inline TConstValueRef DictElement(TConstValueRef v, TStringBuf key) {
        return &(*v)[key];
    }

    static inline size_t DictSize(TConstValueRef v) {
        return v->GetDict().size();
    }

    using TDictIterator = NSc::TDict::const_iterator;

    static inline TDictIterator DictBegin(TConstValueRef v) {
        return v->GetDict().begin();
    }

    static inline TDictIterator DictEnd(TConstValueRef v) {
        return v->GetDict().end();
    }

    static inline TStringBuf DictIteratorKey(TConstValueRef /*dict*/, const TDictIterator& it) {
        return it->first;
    }

    static inline TConstValueRef DictIteratorValue(TConstValueRef /*dict*/, const TDictIterator& it) {
        return &it->second;
    }

    // boolean ops
    static inline void Get(TConstValueRef v, bool def, bool& b) {
        b = def == true ? !v->IsExplicitFalse() : v->IsTrue();
    }

    static inline void Get(TConstValueRef v, bool& b) {
        b = v->IsTrue();
    }

    static inline void Set(TValueRef v, bool b) {
        v->SetIntNumber(b ? 1 : 0);
    }

    static inline bool IsValidPrimitive(const bool&, TConstValueRef v) {
        return v->IsTrue() || v->IsExplicitFalse();
    }

#define INTEGER_OPS_EX(type, min, max, isUnsigned)                                 \
    static inline void Get(TConstValueRef v, type def, type& i) {                  \
        if (isUnsigned) {                                                          \
            i = v->IsNumber() && v->GetIntNumber() >= 0 ? v->GetIntNumber() : def; \
        } else {                                                                   \
            i = v->IsNumber() ? v->GetIntNumber() : def;                           \
        }                                                                          \
    }                                                                              \
    static inline void Get(TConstValueRef v, type& i) {                            \
        if (isUnsigned) {                                                          \
            i = Max<i64>(0, v->GetIntNumber());                                    \
        } else {                                                                   \
            i = v->GetIntNumber();                                                 \
        }                                                                          \
    }                                                                              \
    static inline bool IsValidPrimitive(const type&, TConstValueRef v) {           \
        return v->IsIntNumber() &&                                                 \
               v->GetIntNumber() >= min &&                                         \
               v->GetIntNumber() <= max;                                           \
    }                                                                              \
    static inline void Set(TValueRef v, type i) {                                  \
        v->SetIntNumber(i);                                                        \
    }

#define INTEGER_OPS(type, isUnsigned) INTEGER_OPS_EX(type, Min<type>(), Max<type>(), isUnsigned)

    INTEGER_OPS(i8, false)
    INTEGER_OPS(i16, false)
    INTEGER_OPS(i32, false)
    INTEGER_OPS(i64, false)
    INTEGER_OPS(ui8, true)
    INTEGER_OPS(ui16, true)
    INTEGER_OPS(ui32, true)
    INTEGER_OPS_EX(ui64, 0, (i64)(Max<i64>() >> 1), true)

#undef INTEGER_OPS
#undef INTEGER_OPS_EX

    // double ops
    static inline bool Get(TConstValueRef v, double def, double& d) {
        if (v->IsNumber()) {
            d = v->GetNumber(def);
            return true;
        }
        d = def;
        return false;
    }

    static inline void Get(TConstValueRef v, double& d) {
        d = v->GetNumber();
    }

    static inline void Set(TValueRef v, double d) {
        v->SetNumber(d);
    }

    static inline bool IsValidPrimitive(const double&, TConstValueRef v) {
        return v->IsNumber();
    }

    // string ops
    static inline void Get(TConstValueRef v, TStringBuf def, TStringBuf& s) {
        s = v->GetString(def);
    }

    static inline void Get(TConstValueRef v, TStringBuf& s) {
        s = v->GetString();
    }

    static inline void Set(TValueRef v, TStringBuf s) {
        v->SetString(s);
    }

    static inline bool IsValidPrimitive(const TStringBuf&, TConstValueRef v) {
        return v->IsString();
    }

    // validation ops
    static inline TVector<TString> GetKeys(TConstValueRef v) { 
        TVector<TString> res; 
        for (const auto& key : v->DictKeys(true)) {
            res.push_back(ToString(key));
        }
        return res;
    }

    template <typename T>
    static inline bool IsValidPrimitive(const T&, TConstValueRef) {
        return false;
    }
};
