#pragma once

#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

TVector<TMapElement> KeepLiveMapElements(const TIntrusivePtr<TOpMap>& map, const TInfoUnitSet& liveOut, const TPlanProps& props);
TVector<TInfoUnit> KeepLiveColumns(const TVector<TInfoUnit>& columns, const TInfoUnitSet& liveOut);
bool NarrowReadColumns(const TIntrusivePtr<TOpRead>& read, const TVector<TInfoUnit>& liveOutput);
bool PruneAggregateTraits(const TIntrusivePtr<TOpAggregate>& aggregate, const TVector<TInfoUnit>& liveOutput);

} // namespace NKqp
} // namespace NKikimr
