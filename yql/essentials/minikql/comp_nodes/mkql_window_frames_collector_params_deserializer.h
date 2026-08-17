#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_core_window_frames_collector_params_deserializer.h>
#include <yql/essentials/minikql/mkql_window_defs.h>
#include <yql/essentials/minikql/mkql_node.h>

namespace NKikimr::NMiniKQL {

using TNodeExtractor = std::function<IComputationNode*(const TRuntimeNode&)>;
using TUnboxedValueVariantBound = TVariantBound<TComputationContext, NYql::NUdf::TUnboxedValue>;
using TUnboxedValueVariantBounds = TVariantBounds<TComputationContext, NYql::NUdf::TUnboxedValue>;

std::pair<TUnboxedValueVariantBounds, std::vector<IComputationNode*>>
DeserializeBoundsAsVariant(const TRuntimeNode& node, const TStructType* streamType, TNodeExtractor nodeExtractor, ui32& ctxIndex);

ESortOrder DeserializeSortOrder(const TRuntimeNode& node);

bool AnyRangeProvided(const TRuntimeNode& node);

} // namespace NKikimr::NMiniKQL
