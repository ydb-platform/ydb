#pragma once

#include <yt/yt/core/yson/building_consumer.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void CreateBuildingYsonConsumer(std::unique_ptr<NYson::IBuildingYsonConsumer<T>>* buildingConsumer, NYson::EYsonType ysonType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define DEFAULT_BUILDING_CONSUMER_INL_H_
#include "default_building_consumer-inl.h"
#undef DEFAULT_BUILDING_CONSUMER_INL_H_
