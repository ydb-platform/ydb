#pragma once

#include "public.h"
#include "resource_vector.h"
#include "resource_volume.h"

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/node.h>

#include <yt/yt/core/misc/string_builder.h>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

TJobResources ToJobResources(const TJobResourcesConfig& config, TJobResources defaultValue);

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TJobResources& resources, NYson::IYsonConsumer* consumer);
void Deserialize(TJobResources& resources, NYTree::INodePtr node);

void FormatValue(TStringBuilderBase* builder, const TJobResources& resources, TStringBuf /* format */);

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TResourceVolume& volume, NYson::IYsonConsumer* consumer);
void Deserialize(TResourceVolume& volume, NYTree::INodePtr node);
void Deserialize(TResourceVolume& volume, NYson::TYsonPullParserCursor* cursor);

void Serialize(const TResourceVector& resourceVector, NYson::IYsonConsumer* consumer);

void FormatValue(TStringBuilderBase* builder, const TResourceVolume& volume, TStringBuf /* format */);
TString ToString(const TResourceVolume& volume);

void FormatValue(TStringBuilderBase* builder, const TResourceVector& resourceVector, TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf


