#ifndef GUID_INL_H_
#error "Direct inclusion of this file is not allowed, include guid.h"
// For the sake of sane code completion.
#include "guid.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void ToProto(NProto::TGuid* protoGuid, TGuid guid)
{
    protoGuid->set_first(guid.Parts64[0]);
    protoGuid->set_second(guid.Parts64[1]);
}

Y_FORCE_INLINE void FromProto(TGuid* guid, const NYT::NProto::TGuid& protoGuid)
{
    guid->Parts64[0] = protoGuid.first();
    guid->Parts64[1] = protoGuid.second();
}

Y_FORCE_INLINE void ToProto(TProtoStringType* protoGuid, TGuid guid)
{
    *protoGuid = guid ? ToString(guid) : TString();
}

Y_FORCE_INLINE void FromProto(TGuid* guid, const TProtoStringType& protoGuid)
{
    *guid = protoGuid ? TGuid::FromString(protoGuid) : TGuid();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
