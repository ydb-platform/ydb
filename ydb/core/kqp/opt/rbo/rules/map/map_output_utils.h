#pragma once

#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

TVector<TInfoUnit> BuildMapOutput(const TVector<TInfoUnit>& inputOutput, const TVector<TMapElement>& elements);
TVector<TInfoUnit> BuildMapOutput(const TIntrusivePtr<TOpMap>& map, const TVector<TMapElement>& elements);

} // namespace NKqp
} // namespace NKikimr
