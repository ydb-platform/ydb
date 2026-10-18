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

TCodecValue EncodeCell(NScheme::TTypeId type, bool null, TStringBuf cell, EWireFormat format, ui8 precision = 0, ui8 scale = 0);
TCodecValue DecodeCell(NScheme::TTypeId type, bool null, TStringBuf wire, EWireFormat format, ui8 precision = 0, ui8 scale = 0);

// One family per file. A family that does not own the type returns 0A000.
TCodecValue EncodeIntegerCell(NScheme::TTypeId type, bool null, TStringBuf cell, EWireFormat format);
TCodecValue DecodeIntegerCell(NScheme::TTypeId type, bool null, TStringBuf wire, EWireFormat format);
TCodecValue EncodeTextCell(NScheme::TTypeId type, bool null, TStringBuf cell, EWireFormat format);
TCodecValue DecodeTextCell(NScheme::TTypeId type, bool null, TStringBuf wire, EWireFormat format);
TCodecValue EncodeTimeCell(NScheme::TTypeId type, bool null, TStringBuf cell, EWireFormat format);
TCodecValue DecodeTimeCell(NScheme::TTypeId type, bool null, TStringBuf wire, EWireFormat format);
TCodecValue EncodeDecimalCell(NScheme::TTypeId type, bool null, TStringBuf cell, EWireFormat format, ui8 precision, ui8 scale);
TCodecValue DecodeDecimalCell(NScheme::TTypeId type, bool null, TStringBuf wire, EWireFormat format, ui8 precision, ui8 scale);

} // namespace NKikimr::NSql
