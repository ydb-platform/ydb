#pragma once

#include <yql/essentials/minikql/mkql_node.h>

#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/vector.h>

namespace NKikimr::NMiniKQL {

class TPresortCodec {
public:
    TPresortCodec() = default;

    struct TTypeInfo {
        NUdf::EDataSlot Slot;
        bool IsOptional;
        bool IsDesc;
    };

    void AddType(NUdf::EDataSlot slot, bool isOptional = false, bool isDesc = false);

protected:
    size_t Current_ = 0;
    TVector<TTypeInfo> Types_;
};

class TPresortEncoder: public TPresortCodec {
public:
    TPresortEncoder() = default;

    void Start();
    void Start(TStringBuf prefix);
    void Encode(const NUdf::TUnboxedValuePod& value);
    TStringBuf Finish(); // user must copy

private:
    TVector<ui8> Output_;
};

class TPresortDecoder: public TPresortCodec {
public:
    TPresortDecoder() = default;

    void Start(TStringBuf input);
    NUdf::TUnboxedValue Decode();
    void Finish();

private:
    TVector<ui8> Buffer_;
    TStringBuf Input_;
};

class THolderFactory;

class TGenericPresortEncoder {
public:
    explicit TGenericPresortEncoder(TType* type);
    TStringBuf Encode(const NUdf::TUnboxedValue& value, bool desc); // user must copy
    NUdf::TUnboxedValue Decode(TStringBuf buf, bool desc, const THolderFactory& factory);

private:
    TType* Type_;
    TVector<ui8> Output_;
    TVector<ui8> Buffer_;
};

} // namespace NKikimr::NMiniKQL
