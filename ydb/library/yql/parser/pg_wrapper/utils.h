#pragma once
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/computation/mkql_block_item.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

namespace NYql {

inline NKikimr::NUdf::TUnboxedValuePod ScalarDatumToPod(Datum datum) {
    return NKikimr::NUdf::TUnboxedValuePod((ui64)datum);
}

inline Datum ScalarDatumFromPod(const NKikimr::NUdf::TUnboxedValuePod& value) {
    return (Datum)value.Get<ui64>();
}

inline Datum ScalarDatumFromItem(const NKikimr::NUdf::TBlockItem& value) {
    return (Datum)value.As<ui64>();
}

inline NKikimr::NUdf::TUnboxedValuePod PointerDatumToPod(Datum datum) {
    auto header = ((NKikimr::NMiniKQL::TMkqlPAllocHeader*)datum) - 1;
    // remove this block from list
    header->U.Entry.Unlink();
    NKikimr::NUdf::IBoxedValue* raw = &header->U.Boxed;
    new(raw) NKikimr::NMiniKQL::TBoxedValueWithFree();
    NKikimr::NUdf::IBoxedValuePtr ref(raw);
    return NKikimr::NUdf::TUnboxedValuePod(std::move(ref));
}

inline NKikimr::NUdf::TUnboxedValuePod OwnedPointerDatumToPod(Datum datum) {
    auto header = ((NKikimr::NMiniKQL::TMkqlPAllocHeader*)datum) - 1;
    NKikimr::NUdf::IBoxedValue* raw = &header->U.Boxed;
    NKikimr::NUdf::IBoxedValuePtr ref(raw);
    return NKikimr::NUdf::TUnboxedValuePod(std::move(ref));
}

class TVPtrHolder {
public:
    TVPtrHolder() {
        new(Dummy) NKikimr::NMiniKQL::TBoxedValueWithFree();
    }

    static bool IsBoxedVPtr(Datum ptr) {
        auto header = ((NKikimr::NMiniKQL::TMkqlPAllocHeader*)ptr) - 1;
        return *(const uintptr_t*)&header->U.Boxed == *(const uintptr_t*)Instance.Dummy;
    }

private:
    char Dummy[sizeof(NKikimr::NUdf::IBoxedValue)];

    static TVPtrHolder Instance;
};

inline NKikimr::NUdf::TUnboxedValuePod AnyDatumToPod(Datum datum, bool passByValue) {
    if (passByValue) {
        return ScalarDatumToPod(datum);
    }

    if (TVPtrHolder::IsBoxedVPtr(datum)) {
        // returned one of arguments
        return OwnedPointerDatumToPod(datum);
    }

    return PointerDatumToPod(datum);
}

inline Datum PointerDatumFromPod(const NKikimr::NUdf::TUnboxedValuePod& value) {
    return (Datum)(((const NKikimr::NMiniKQL::TMkqlPAllocHeader*)value.AsRawBoxed()) + 1);
}

inline Datum PointerDatumFromItem(const NKikimr::NUdf::TBlockItem& value) {
    return (Datum)(value.AsStringRef().Data() + sizeof(void*));
}

inline ui32 GetFullVarSize(const text* s) {
    return VARSIZE(s);
}

inline ui32 GetCleanVarSize(const text* s) {
    return VARSIZE(s) - VARHDRSZ;
}

inline const char* GetVarData(const text* s) {
    return VARDATA(s);
}

inline TStringBuf GetVarBuf(const text* s) {
    return TStringBuf(GetVarData(s), GetCleanVarSize(s));
}

inline char* GetMutableVarData(text* s) {
    return VARDATA(s);
}

inline void UpdateCleanVarSize(text* s, ui32 cleanSize) {
    SET_VARSIZE(s, cleanSize + VARHDRSZ);
}

inline char* MakeCStringNotFilled(size_t size) {
    char* ret = (char*)palloc(size + 1);
    ret[size] = '\0';
    return ret;
}

inline char* MakeCString(TStringBuf s) {
    char* ret = MakeCStringNotFilled(s.Size());
    memcpy(ret, s.Data(), s.Size());
    return ret;
}

inline char* MakeFixedStringNotFilled(size_t size) {
    char* ret = (char*)palloc(size);
    memset(ret, 0, size);
    return ret;
}

inline char* MakeFixedString(TStringBuf s, size_t size) {
    auto ret = MakeFixedStringNotFilled(size);
    Y_ENSURE(s.Size() <= size);
    memcpy(ret, s.Data(), s.Size());
    return ret;
}

inline text* MakeVarNotFilled(size_t size) {
    text* ret = (text*)palloc(size + VARHDRSZ);
    UpdateCleanVarSize(ret, size);
    return ret;
}

inline text* MakeVar(TStringBuf s) {
    text* ret = MakeVarNotFilled(s.Size());
    memcpy(GetMutableVarData(ret), s.Data(), s.Size());
    return ret;
}

inline ui32 MakeTypeIOParam(const NPg::TTypeDesc& desc) {
    return desc.ElementTypeId ? desc.ElementTypeId : desc.TypeId;
}

void PrepareVariadicArraySlow(FunctionCallInfoBaseData& callInfo, const NPg::TProcDesc& desc);
void FreeVariadicArray(FunctionCallInfoBaseData& callInfo, ui32 originalArgs);

inline bool PrepareVariadicArray(FunctionCallInfoBaseData& callInfo, const NPg::TProcDesc& desc) {
    if (!desc.VariadicArgType || desc.VariadicArgType == desc.VariadicType) {
        return false;
    }

    PrepareVariadicArraySlow(callInfo, desc);
    return true;
}

}
