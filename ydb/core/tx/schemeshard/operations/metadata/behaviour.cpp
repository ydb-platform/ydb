#include "behaviour.h"

template <>
void Out<NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase>(
    IOutputStream& o, NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase w) {
    o.Write(w);
}

namespace NKikimr::NSchemeShard::NOperations {}
