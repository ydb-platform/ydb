#pragma once

namespace NYql {

// allow to construct TListEntry in the space for IBoxedValue
static_assert(sizeof(NKikimr::NUdf::IBoxedValue) >= sizeof(NKikimr::NMiniKQL::TAllocState::TListEntry));

constexpr size_t PallocHdrSize = sizeof(void*) + sizeof(NKikimr::NUdf::IBoxedValue);

inline NKikimr::NUdf::TUnboxedValuePod ScalarDatumToPod(Datum datum) {
    return NKikimr::NUdf::TUnboxedValuePod((ui64)datum);
}

inline Datum ScalarDatumFromPod(const NKikimr::NUdf::TUnboxedValuePod& value) {
    return (Datum)value.Get<ui64>();
}

inline Datum ScalarDatumFromItem(const NKikimr::NUdf::TBlockItem& value) {
    return (Datum)value.As<ui64>();
}

class TBoxedValueWithFree : public NKikimr::NUdf::TBoxedValueBase {
public:
    void operator delete(void *mem) noexcept {
        return NKikimr::NMiniKQL::MKQLFreeDeprecated(mem);
    }
};

inline NKikimr::NUdf::TUnboxedValuePod PointerDatumToPod(Datum datum) {
    auto original = (char*)datum - PallocHdrSize;
    // remove this block from list
    ((NKikimr::NMiniKQL::TAllocState::TListEntry*)original)->Unlink();

    auto raw = (NKikimr::NUdf::IBoxedValue*)original;
    new(raw) TBoxedValueWithFree();
    NKikimr::NUdf::IBoxedValuePtr ref(raw);
    return NKikimr::NUdf::TUnboxedValuePod(std::move(ref));
}

inline NKikimr::NUdf::TUnboxedValuePod OwnedPointerDatumToPod(Datum datum) {
    auto original = (char*)datum - PallocHdrSize;
    auto raw = (NKikimr::NUdf::IBoxedValue*)original;
    NKikimr::NUdf::IBoxedValuePtr ref(raw);
    return NKikimr::NUdf::TUnboxedValuePod(std::move(ref));
}

class TVPtrHolder {
public:
    TVPtrHolder() {
        new(Dummy) TBoxedValueWithFree();
    }

    static bool IsBoxedVPtr(Datum ptr) {
        return *(const uintptr_t*)((char*)ptr - PallocHdrSize) == *(const uintptr_t*)Instance.Dummy;
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
    return (Datum)(((const char*)value.AsBoxed().Get()) + PallocHdrSize);
}

inline Datum PointerDatumFromItem(const NKikimr::NUdf::TBlockItem& value) {
    return (Datum)value.AsStringRef().Data();
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

inline char* MakeCString(TStringBuf s) {
    char* ret = (char*)palloc(s.Size() + 1);
    memcpy(ret, s.Data(), s.Size());
    ret[s.Size()] = '\0';
    return ret;
}

inline text* MakeVar(TStringBuf s) {
    text* ret = (text*)palloc(s.Size() + VARHDRSZ);
    UpdateCleanVarSize(ret, s.Size());
    memcpy(GetMutableVarData(ret), s.Data(), s.Size());
    return ret;
}

inline ui32 MakeTypeIOParam(const NPg::TTypeDesc& desc) {
    return desc.ElementTypeId ? desc.ElementTypeId : desc.TypeId;
}

}
