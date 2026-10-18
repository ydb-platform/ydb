#include "codec.h"

namespace NKikimr::NSql {

namespace {

template <class TNext>
TCodecValue FirstOwned(TCodecValue value, TNext next, NScheme::TTypeId type, bool null, TStringBuf bytes, EWireFormat format)
{
    if (value.Ok || value.SqlState != "0A000") {
        return value;
    }
    return next(type, null, bytes, format);
}

} // namespace

TCodecValue EncodeCell(NScheme::TTypeId type, bool null, TStringBuf cell, EWireFormat format, ui8 precision, ui8 scale) {
    return FirstOwned(FirstOwned(FirstOwned(EncodeIntegerCell(type, null, cell, format), EncodeTextCell, type, null, cell, format),
        EncodeTimeCell, type, null, cell, format),
        [precision, scale](NScheme::TTypeId typeId, bool isNull, TStringBuf bytes, EWireFormat wire) {
            return EncodeDecimalCell(typeId, isNull, bytes, wire, precision, scale);
        },
        type, null, cell, format);
}

TCodecValue DecodeCell(NScheme::TTypeId type, bool null, TStringBuf wire, EWireFormat format, ui8 precision, ui8 scale) {
    return FirstOwned(FirstOwned(FirstOwned(DecodeIntegerCell(type, null, wire, format), DecodeTextCell, type, null, wire, format),
        DecodeTimeCell, type, null, wire, format),
        [precision, scale](NScheme::TTypeId typeId, bool isNull, TStringBuf bytes, EWireFormat wireFormat) {
            return DecodeDecimalCell(typeId, isNull, bytes, wireFormat, precision, scale);
        },
        type, null, wire, format);
}

} // namespace NKikimr::NSql
