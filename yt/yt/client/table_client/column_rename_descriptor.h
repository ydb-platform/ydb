#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TColumnRenameDescriptor
{
    TString OriginalName;
    TString NewName;
};

void Deserialize(TColumnRenameDescriptors& value, NYTree::INodePtr node);
void Serialize(const TColumnRenameDescriptors& value, NYson::IYsonConsumer* consumer);

void ToProto(NProto::TColumnRenameDescriptor* protoDescriptor, const TColumnRenameDescriptor& descriptor);
void FromProto(TColumnRenameDescriptor* descriptor, const NProto::TColumnRenameDescriptor& protoDescriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
