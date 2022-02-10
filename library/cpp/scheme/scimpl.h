#pragma once

#include "scheme.h"

#include <util/stream/output.h>

namespace NSc {
    struct TValue::TScCore : TAtomicRefCount<TScCore, TDestructor>, TNonCopyable {
        TPoolPtr Pool;
        double FloatNumber = 0;
        i64 IntNumber = 0;
        TStringBuf String;
        TDict Dict;
        TArray Array;
        TValue::EType ValueType = TValue::EType::Null;

        TScCore(TPoolPtr& p)
            : Pool(p)
            , Dict(Pool->Get())
            , Array(Pool->Get())
        {
        }

        bool IsNull() const {
            return TValue::EType::Null == ValueType;
        }

        bool IsBool() const {
            return TValue::EType::Bool == ValueType;
        }

        bool IsIntNumber() const {
            return TValue::EType::IntNumber == ValueType || IsBool();
        }

        bool IsNumber() const {
            return TValue::EType::FloatNumber == ValueType || IsIntNumber();
        }

        bool IsString() const {
            return TValue::EType::String == ValueType;
        }

        bool IsArray() const {
            return TValue::EType::Array == ValueType;
        }

        bool IsDict() const {
            return TValue::EType::Dict == ValueType;
        }

        bool HasChildren() const {
            return GetDict().size() + GetArray().size();
        }

        void SetNull() {
            ValueType = TValue::EType::Null;
        }

        void SetArray() {
            if (Y_LIKELY(IsArray())) {
                return;
            }

            ValueType = TValue::EType::Array;
            Array.clear();
        }

        void SetDict() {
            if (Y_LIKELY(IsDict())) {
                return;
            }

            ValueType = TValue::EType::Dict;
            Dict.clear();
        }

        void SetNumber(double n) {
            ValueType = TValue::EType::FloatNumber;
            FloatNumber = n;
        }

        void SetIntNumber(i64 n) {
            ValueType = TValue::EType::IntNumber;
            IntNumber = n;
        }

        void SetBool(bool b) {
            ValueType = TValue::EType::Bool;
            IntNumber = b;
        }

        void SetString(TStringBuf s) {
            SetOwnedString(Pool->AppendBuf(s));
        }

        void SetOwnedString(TStringBuf s) {
            ValueType = TValue::EType::String;
            String = s;
        }

        double& GetNumberMutable(double defaultnum) {
            switch (ValueType) {
                case TValue::EType::Bool:
                    SetNumber(bool(IntNumber));
                    break;
                case TValue::EType::IntNumber:
                    SetNumber(IntNumber);
                    break;
                case TValue::EType::FloatNumber:
                    break;
                default:
                    SetNumber(defaultnum);
                    break;
            }

            return FloatNumber;
        }

        i64& GetIntNumberMutable(i64 defaultnum) {
            switch (ValueType) {
                case TValue::EType::Bool:
                    SetIntNumber(bool(IntNumber));
                    break;
                case TValue::EType::IntNumber:
                    break;
                case TValue::EType::FloatNumber:
                    SetIntNumber(FloatNumber);
                    break;
                default:
                    SetIntNumber(defaultnum);
                    break;
            }

            return IntNumber;
        }

        void ClearArray() {
            if (!IsArray()) {
                return;
            }

            Array.clear();
        }

        void ClearDict() {
            if (!IsDict()) {
                return;
            }

            Dict.clear();
        }

        double GetNumber(double d = 0) const {
            switch (ValueType) {
                case TValue::EType::Bool:
                    return (bool)IntNumber;
                case TValue::EType::IntNumber:
                    return IntNumber;
                case TValue::EType::FloatNumber:
                    return FloatNumber;
                default:
                    return d;
            }
        }

        i64 GetIntNumber(i64 n = 0) const {
            switch (ValueType) {
                case TValue::EType::Bool:
                    return (bool)IntNumber;
                case TValue::EType::IntNumber:
                    return IntNumber;
                case TValue::EType::FloatNumber:
                    return FloatNumber;
                default:
                    return n;
            }
        }

        bool GetBool(bool b = false) const {
            return GetIntNumber(b);
        }

        TStringBuf GetString(TStringBuf s = TStringBuf()) const {
            return IsString() ? String : s;
        }

        const TArray& GetArray() const {
            return IsArray() ? Array : TValue::DefaultArray();
        }

        TArray& GetArrayMutable() {
            SetArray();
            return Array;
        }

        TDict& GetDictMutable() {
            SetDict();
            return Dict;
        }

        const TDict& GetDict() const {
            return IsDict() ? Dict : TValue::DefaultDict();
        }

        static void DoPush(TPoolPtr& p, TArray& a) {
            a.push_back(TValue(p));
            a.back().CopyOnWrite = false;
        }

        TValue& Push() {
            SetArray();
            DoPush(Pool, Array);
            return Array.back();
        }

        TValue Pop() {
            if (!IsArray() || Array.empty()) {
                return TValue::DefaultValue();
            }

            TValue v = Array.back();
            Array.pop_back();
            return v;
        }

        const TValue& Get(size_t key) const {
            return IsArray() && Array.size() > key ? Array[key] : TValue::DefaultValue();
        }

        TValue* GetNoAdd(size_t key) {
            return IsArray() && Array.size() > key ? &Array[key] : nullptr;
        }

        TValue& GetOrAdd(size_t key) {
            SetArray();
            for (size_t i = Array.size(); i <= key; ++i) {
                DoPush(Pool, Array);
            }

            return Array[key];
        }

        TValue& Back() {
            SetArray();

            if (Array.empty()) {
                DoPush(Pool, Array);
            }

            return Array.back();
        }

        TValue& Insert(size_t key) {
            SetArray();

            if (Array.size() <= key) {
                return GetOrAdd(key);
            } else {
                Array.insert(Array.begin() + key, TValue(Pool));
                Array[key].CopyOnWrite = false;
                return Array[key];
            }
        }

        TValue Delete(size_t key) {
            if (!IsArray() || Array.size() <= key) {
                return TValue::DefaultValue();
            }

            TValue v = Array[key];
            Array.erase(Array.begin() + key);
            return v;
        }

        const TValue& Get(TStringBuf key) const {
            if (!IsDict()) {
                return TValue::DefaultValue();
            }

            TDict::const_iterator it = Dict.find(key);
            return it != Dict.end() ? it->second : TValue::DefaultValue();
        }

        TValue* GetNoAdd(TStringBuf key) {
            if (!IsDict()) {
                return nullptr;
            }

            return Dict.FindPtr(key);
        }

        TValue& Add(TStringBuf key) {
            SetDict();
            TDict::iterator it = Dict.insert(std::make_pair(Pool->AppendBuf(key), TValue(Pool))).first;
            it->second.CopyOnWrite = false;
            return it->second;
        }

        TValue& GetOrAdd(TStringBuf key) {
            SetDict();
            TDict::insert_ctx ctx;
            TDict::iterator it = Dict.find(key, ctx);

            if (it == Dict.end()) {
                it = Dict.insert_direct(std::make_pair(Pool->AppendBuf(key), TValue(Pool)), ctx);
                it->second.CopyOnWrite = false;
            }

            return it->second;
        }

        TValue Delete(TStringBuf key) {
            if (!IsDict()) {
                return TValue::DefaultValue();
            }

            TDict::iterator it = Dict.find(key);

            if (it == Dict.end()) {
                return TValue::DefaultValue();
            }

            TValue v = it->second;
            Dict.erase(key);
            return v;
        }
    };

    TValue::TScCore* TValue::NewCore(TPoolPtr& p) {
        return new (p->Pool.Allocate<TScCore>()) TScCore(p);
    }

    TValue::TValue() {
        auto p = TPoolPtr(new NDefinitions::TPool);
        TheCore = NewCore(p);
    }

    TValue::TValue(double t)
        : TValue()
    {
        SetNumber(t);
    }

    TValue::TValue(unsigned long long t)
        : TValue()
    {
        SetIntNumber(t);
    }

    TValue::TValue(unsigned long t)
        : TValue()
    {
        SetIntNumber(t);
    }

    TValue::TValue(unsigned t)
        : TValue()
    {
        SetIntNumber(t);
    }

    TValue::TValue(long long t)
        : TValue()
    {
        SetIntNumber(t);
    }

    TValue::TValue(long t)
        : TValue()
    {
        SetIntNumber(t);
    }

    TValue::TValue(int t)
        : TValue()
    {
        SetIntNumber(t);
    }

    //TValue::TValue(bool t)
    //    : TValue()
    //{
    //    SetBool(t);
    //}

    TValue::TValue(TStringBuf t)
        : TValue()
    {
        SetString(t);
    }

    TValue::TValue(const char* t)
        : TValue()
    {
        SetString(t);
    }

    TValue::TValue(TValue& v)
        : TheCore(v.TheCore)
        , CopyOnWrite(v.CopyOnWrite)
    {
    }

    TValue::TValue(const TValue& v)
        : TheCore(v.TheCore)
        , CopyOnWrite(true)
    {
    }

    TValue::TValue(TValue&& v) noexcept
        : TheCore(std::move(v.TheCore))
        , CopyOnWrite(v.CopyOnWrite)
    {}

    TValue::operator double() const {
        return GetNumber();
    }

    TValue::operator float() const {
        return GetNumber();
    }

    TValue::operator long long() const {
        return GetIntNumber();
    }

    TValue::operator long() const {
        return GetIntNumber();
    }

    TValue::operator int() const {
        return GetIntNumber();
    }

    TValue::operator short() const {
        return GetIntNumber();
    }

    TValue::operator char() const {
        return GetIntNumber();
    }

    TValue::operator unsigned long long() const {
        return GetIntNumber();
    }

    TValue::operator unsigned long() const {
        return GetIntNumber();
    }

    TValue::operator unsigned() const {
        return GetIntNumber();
    }

    TValue::operator unsigned short() const {
        return GetIntNumber();
    }

    TValue::operator unsigned char() const {
        return GetIntNumber();
    }

    TValue::operator signed char() const {
        return GetIntNumber();
    }

    TValue::operator TStringBuf() const {
        return GetString();
    }

    TValue::operator const ::NSc::TArray&() const {
        return GetArray();
    }

    TValue::operator const ::NSc::TDict&() const {
        return GetDict();
    }

    TValue& TValue::operator=(double t) {
        return SetNumber(t);
    }

    TValue& TValue::operator=(unsigned long long t) {
        return SetIntNumber(t);
    }

    TValue& TValue::operator=(unsigned long t) {
        return SetIntNumber(t);
    }

    TValue& TValue::operator=(unsigned t) {
        return SetIntNumber(t);
    }

    TValue& TValue::operator=(long long t) {
        return SetIntNumber(t);
    }

    TValue& TValue::operator=(long t) {
        return SetIntNumber(t);
    }

    TValue& TValue::operator=(int t) {
        return SetIntNumber(t);
    }

    //TValue& TValue::operator=(bool t) {
    //    return SetBool(t);
    //}

    TValue& TValue::operator=(TStringBuf t) {
        return SetString(t);
    }

    TValue& TValue::operator=(const char* t) {
        return SetString(t);
    }

    TValue& TValue::operator=(TValue& v) & {
        if (!Same(*this, v)) {
            //Extend TheCore lifetime not to trigger possible v deletion via parent-child chain
            auto tmpCore = TheCore;
            TheCore = v.TheCore;
            CopyOnWrite = v.CopyOnWrite;
        }
        return *this;
    }

    TValue& TValue::operator=(const TValue& v) & {
        if (!Same(*this, v)) {
            //Extend TheCore lifetime not to trigger possible v deletion via parent-child chain
            auto tmpCore = TheCore;
            TheCore = v.TheCore;
            CopyOnWrite = true;
        }
        return *this;
    }

    TValue& TValue::operator=(TValue&& v) & noexcept {
        if (!Same(*this, v)) {
            //Extend TheCore lifetime not to trigger possible v deletion via parent-child chain
            auto tmpCore = TheCore;
            TheCore = std::move(v.TheCore);
            CopyOnWrite = v.CopyOnWrite;
        }
        return *this;
    }

    bool TValue::Has(size_t idx) const {
        return IsArray() && GetArray().size() > idx;
    }

    bool TValue::Has(TStringBuf s) const {
        return GetDict().contains(s);
    }

    TValue& TValue::GetOrAddUnsafe(size_t idx) {
        return CoreMutable().GetOrAdd(idx);
    }

    TValue& TValue::GetOrAdd(TStringBuf idx) {
        return CoreMutable().GetOrAdd(idx);
    }

    const TValue& TValue::Get(size_t idx) const {
        return Core().Get(idx);
    }

    TValue* TValue::GetNoAdd(size_t idx) {
        return CoreMutable().GetNoAdd(idx);
    }

    const TValue& TValue::Get(TStringBuf idx) const {
        return Core().Get(idx);
    }

    TValue* TValue::GetNoAdd(TStringBuf key) {
        return CoreMutable().GetNoAdd(key);
    }

    TValue& TValue::Back() {
        return CoreMutable().Back();
    }

    const TValue& TValue::Back() const {
        const TArray& arr = GetArray();
        return arr.empty() ? DefaultValue() : arr.back();
    }

    TValue TValue::Delete(size_t idx) {
        return CoreMutable().Delete(idx);
    }

    TValue TValue::Delete(TStringBuf idx) {
        return CoreMutable().Delete(idx);
    }

    TValue& TValue::AddAll(std::initializer_list<std::pair<TStringBuf, TValue>> t) {
        for (const auto& el : t) {
            Add(el.first) = el.second;
        }
        return *this;
    }

    TValue& TValue::InsertUnsafe(size_t idx) {
        return CoreMutable().Insert(idx);
    }

    template <class TIt>
    TValue& TValue::AppendAll(TIt begin, TIt end) {
        GetArrayMutable().AppendAll(begin, end);
        return *this;
    }

    template <class TColl>
    TValue& TValue::AppendAll(TColl&& coll) {
        return AppendAll(std::begin(coll), std::end(coll));
    }

    TValue& TValue::AppendAll(std::initializer_list<TValue> coll) {
        return AppendAll(coll.begin(), coll.end());
    }

    TValue& TValue::Push() {
        return CoreMutable().Push();
    }

    TValue TValue::Pop() {
        return CoreMutable().Pop();
    }

    TValue::EType TValue::GetType() const {
        return Core().ValueType;
    }

    bool TValue::IsNull() const {
        return Core().IsNull();
    }

    bool TValue::IsNumber() const {
        return Core().IsNumber();
    }

    bool TValue::IsIntNumber() const {
        return Core().IsIntNumber();
    }

    bool TValue::IsBool() const {
        return Core().IsBool();
    }

    bool TValue::IsString() const {
        return Core().IsString();
    }

    bool TValue::IsArray() const {
        return Core().IsArray();
    }

    bool TValue::IsDict() const {
        return Core().IsDict();
    }

    TValue& TValue::SetNumber(double i) {
        CoreMutableForSet().SetNumber(i);
        return *this;
    }

    TValue& TValue::SetIntNumber(i64 n) {
        CoreMutableForSet().SetIntNumber(n);
        return *this;
    }

    TValue& TValue::SetBool(bool val) {
        CoreMutableForSet().SetBool(val);
        return *this;
    }

    TValue& TValue::SetString(TStringBuf s) {
        CoreMutableForSet().SetString(s);
        return *this;
    }

    double TValue::GetNumber(double d) const {
        return Core().GetNumber(d);
    }

    i64 TValue::GetIntNumber(i64 n) const {
        return Core().GetIntNumber(n);
    }

    bool TValue::GetBool(bool b) const {
        return Core().GetBool(b);
    }

    double& TValue::GetNumberMutable(double defaultval) {
        return CoreMutable().GetNumberMutable(defaultval);
    }

    i64& TValue::GetIntNumberMutable(i64 defaultval) {
        return CoreMutable().GetIntNumberMutable(defaultval);
    }

    TStringBuf TValue::GetString(TStringBuf d) const {
        return Core().GetString(d);
    }

    TValue& TValue::SetArray() {
        CoreMutable().SetArray();
        return *this;
    }

    TValue& TValue::ClearArray() {
        CoreMutable().ClearArray();
        return *this;
    }

    TValue& TValue::SetDict() {
        CoreMutable().SetDict();
        return *this;
    }

    TValue& TValue::ClearDict() {
        CoreMutable().ClearDict();
        return *this;
    }

    const TArray& TValue::GetArray() const {
        return Core().GetArray();
    }

    TArray& TValue::GetArrayMutable() {
        return CoreMutable().GetArrayMutable();
    }

    const TDict& TValue::GetDict() const {
        return Core().GetDict();
    }

    TDict& TValue::GetDictMutable() {
        return CoreMutable().GetDictMutable();
    }

    size_t TValue::StringSize() const {
        return GetString().size();
    }

    size_t TValue::ArraySize() const {
        return GetArray().size();
    }

    size_t TValue::DictSize() const {
        return GetDict().size();
    }

    bool TValue::StringEmpty() const {
        return GetString().empty();
    }

    bool TValue::ArrayEmpty() const {
        return GetArray().empty();
    }

    bool TValue::DictEmpty() const {
        return GetDict().empty();
    }

    TValue::TValue(TPoolPtr& p)
        : TheCore(NewCore(p))
    {
    }

    TValue::TScCore& TValue::CoreMutable() {
        if (Y_UNLIKELY(!TheCore)) {
            *this = TValue();
        } else if (Y_UNLIKELY(CopyOnWrite) && Y_UNLIKELY(TheCore->RefCount() > 1)) {
            *this = Clone();
        }

        CopyOnWrite = false;

        return *TheCore;
    }

    TValue::TScCore& TValue::CoreMutableForSet() {
        if (Y_UNLIKELY(!TheCore) || Y_UNLIKELY(CopyOnWrite) && Y_UNLIKELY(TheCore->RefCount() > 1)) {
            *this = TValue();
        }

        CopyOnWrite = false;

        return *TheCore;
    }

    const TValue::TScCore& TValue::Core() const {
        return TheCore ? *TheCore : DefaultCore();
    }

    TValue& TValue::SetNull() {
        CoreMutableForSet().SetNull();
        return *this;
    }

    namespace NPrivate {
        int CompareStr(const NSc::TValue& a, TStringBuf b);

        int CompareInt(const NSc::TValue& a, i64 r);

        int CompareFloat(const NSc::TValue& a, double r);
    }

    bool operator==(const TValue& a, const TValue& b);

    bool operator!=(const TValue& a, const TValue& b);

    bool operator<=(const TValue&, const TValue&) = delete;
    bool operator>=(const TValue&, const TValue&) = delete;
    bool operator<(const TValue&, const TValue&) = delete;
    bool operator>(const TValue&, const TValue&) = delete;

#define LIBRARY_SCHEME_DECLARE_TVALUE_OPS(T, Impl) \
    bool operator==(const NSc::TValue& a, T b);    \
    bool operator==(T b, const NSc::TValue& a);    \
    bool operator!=(const NSc::TValue& a, T b);    \
    bool operator!=(T b, const NSc::TValue& a);    \
    bool operator<=(const NSc::TValue& a, T b);    \
    bool operator<=(T b, const NSc::TValue& a);    \
    bool operator>=(const NSc::TValue& a, T b);    \
    bool operator>=(T b, const NSc::TValue& a);    \
    bool operator<(const NSc::TValue& a, T b);     \
    bool operator<(T b, const NSc::TValue& a);     \
    bool operator>(const NSc::TValue& a, T b);     \
    bool operator>(T b, const NSc::TValue& a);

#define LIBRARY_SCHEME_DECLARE_TVALUE_INT_OPS(T)            \
    LIBRARY_SCHEME_DECLARE_TVALUE_OPS(signed T, CompareInt) \
    LIBRARY_SCHEME_DECLARE_TVALUE_OPS(unsigned T, CompareInt)

    //LIBRARY_SCHEME_DECLARE_TVALUE_OPS(bool, CompareInt)
    LIBRARY_SCHEME_DECLARE_TVALUE_OPS(char, CompareInt)
    LIBRARY_SCHEME_DECLARE_TVALUE_INT_OPS(char)
    LIBRARY_SCHEME_DECLARE_TVALUE_INT_OPS(short)
    LIBRARY_SCHEME_DECLARE_TVALUE_INT_OPS(int)
    LIBRARY_SCHEME_DECLARE_TVALUE_INT_OPS(long)
    LIBRARY_SCHEME_DECLARE_TVALUE_INT_OPS(long long)

    LIBRARY_SCHEME_DECLARE_TVALUE_OPS(float, CompareFloat)
    LIBRARY_SCHEME_DECLARE_TVALUE_OPS(double, CompareFloat)

    LIBRARY_SCHEME_DECLARE_TVALUE_OPS(TStringBuf, CompareStr)
    LIBRARY_SCHEME_DECLARE_TVALUE_OPS(const TString&, CompareStr)
    LIBRARY_SCHEME_DECLARE_TVALUE_OPS(const char* const, CompareStr)

#undef LIBRARY_SCHEME_DECLARE_TVALUE_OPS
#undef LIBRARY_SCHEME_DECLARE_TVALUE_INT_OPS

}
