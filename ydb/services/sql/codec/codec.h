#pragma once

#include <util/generic/string.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NKikimr::NSql {

enum class EWireFormat {
    Text,
    Binary,
};

// A cell is the bytes already stored for the column. NULL is an empty cell,
// not a type of its own.
struct TCodecValue {
    bool Ok = false;
    bool Null = false;
    TString Bytes;
    TString SqlState;
    TString Message;
};

TCodecValue EncodeCell(NScheme::TTypeId type, bool null, TStringBuf cell, EWireFormat format);
TCodecValue DecodeCell(NScheme::TTypeId type, bool null, TStringBuf wire, EWireFormat format);

// One family per file. A family that does not own the type returns 0A000.
TCodecValue EncodeIntegerCell(NScheme::TTypeId type, bool null, TStringBuf cell, EWireFormat format);
TCodecValue DecodeIntegerCell(NScheme::TTypeId type, bool null, TStringBuf wire, EWireFormat format);
TCodecValue EncodeTextCell(NScheme::TTypeId type, bool null, TStringBuf cell, EWireFormat format);
TCodecValue DecodeTextCell(NScheme::TTypeId type, bool null, TStringBuf wire, EWireFormat format);

} // namespace NKikimr::NSql
