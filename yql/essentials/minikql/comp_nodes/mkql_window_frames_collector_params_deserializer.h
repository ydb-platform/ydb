#pragma once

#include <yql/essentials/minikql/mkql_core_window_frames_collector_params_deserializer.h>
#include <yql/essentials/minikql/mkql_window_defs.h>
#include <yql/essentials/minikql/mkql_node.h>

namespace NKikimr::NMiniKQL {

ESortOrder DeserializeSortOrder(const TRuntimeNode& node);

TString DeserializeSortColumnName(const TRuntimeNode& node);

template <typename TStreamElement>
TComparatorBounds<TStreamElement> DeserializeBounds(const TRuntimeNode& node, NYql::ESortOrder sortOrder);

bool AnyRangeProvided(const TRuntimeNode& node);

} // namespace NKikimr::NMiniKQL
