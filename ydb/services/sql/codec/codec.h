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

} // namespace NKikimr::NSql
