#include "codec.h"

namespace NKikimr::NSql {

namespace {

TCodecValue FirstOwned(TCodecValue value, TCodecValue (*next)(NScheme::TTypeId, bool, TStringBuf, EWireFormat),
    NScheme::TTypeId type, bool null, TStringBuf bytes, EWireFormat format)
{
    if (value.Ok || value.SqlState != "0A000") {
        return value;
    }
    return next(type, null, bytes, format);
}

} // namespace

TCodecValue EncodeCell(NScheme::TTypeId type, bool null, TStringBuf cell, EWireFormat format) {
    return FirstOwned(FirstOwned(EncodeIntegerCell(type, null, cell, format), EncodeTextCell, type, null, cell, format),
        EncodeTimeCell, type, null, cell, format);
}

TCodecValue DecodeCell(NScheme::TTypeId type, bool null, TStringBuf wire, EWireFormat format) {
    return FirstOwned(FirstOwned(DecodeIntegerCell(type, null, wire, format), DecodeTextCell, type, null, wire, format),
        DecodeTimeCell, type, null, wire, format);
}

} // namespace NKikimr::NSql
