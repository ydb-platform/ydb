#pragma once

#include "public.h"

#include <yt/yt_proto/yt/core/misc/proto/guid.pb.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <library/cpp/yt/string/guid.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TGuid* protoGuid, TGuid guid);
void FromProto(TGuid* guid, const NProto::TGuid& protoGuid);

void ToProto(TString* protoGuid, TGuid guid);
void FromProto(TGuid* guid, const TString& protoGuid);

void ToProto(std::string* protoGuid, TGuid guid);
void FromProto(TGuid* guid, const std::string& protoGuid);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define GUID_INL_H_
#include "guid-inl.h"
#undef GUID_INL_H_
