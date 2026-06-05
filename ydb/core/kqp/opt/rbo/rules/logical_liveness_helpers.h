#pragma once

#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

bool AddLiveColumn(TInfoUnitSet& target, const TInfoUnit& iu);
bool AddColumnsToSet(TInfoUnitSet& target, const TVector<TInfoUnit>& ius);
bool AddColumnsToSet(TInfoUnitSet& target, const TInfoUnitSet& ius);

TVector<TMapElement> KeepLiveMapElements(const TIntrusivePtr<TOpMap>& map, const TInfoUnitSet& liveOut, const TPlanProps& props);
TVector<TInfoUnit> KeepLiveColumns(const TVector<TInfoUnit>& columns, const TInfoUnitSet& liveOut);
bool NarrowReadColumns(const TIntrusivePtr<TOpRead>& read, const TVector<TInfoUnit>& liveOutput);
bool PruneAggregateTraits(const TIntrusivePtr<TOpAggregate>& aggregate, const TVector<TInfoUnit>& liveOutput);
bool CanOverrideOutput(EOperator kind);

} // namespace NKqp
} // namespace NKikimr
