#pragma once

#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

#include <optional>

namespace NKikimr {
namespace NKqp {
namespace NMapRules {

struct TRenameCandidate {
    size_t Index = 0;
    TInfoUnit From;
    TInfoUnit To;
    bool FromRenameElement = false;
};

bool CanRenameOutput(const TIntrusivePtr<IOperator>& op, const TInfoUnit& from, const TInfoUnit& to, const TPlanProps& props);

std::optional<TRenameCandidate> FindRenameCandidate(const TIntrusivePtr<TOpMap>& topMap, const TPlanProps& props);

bool CanStartLocalRenamePush(const TIntrusivePtr<TOpMap>& topMap, const TRenameCandidate& candidate, const TPlanProps& props);

TMapElement MakeRenameElement(const TRenameCandidate& candidate, const TIntrusivePtr<TOpMap>& topMap);

bool FinishRenamePush(TIntrusivePtr<IOperator>& input, const TIntrusivePtr<TOpMap>& topMap, const TRenameCandidate& candidate,
                      TRBOContext& ctx, TPlanProps& props);

} // namespace NMapRules
} // namespace NKqp
} // namespace NKikimr
