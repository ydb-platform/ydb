#pragma once

#include "../kqp_cbo_trees.h"

#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_context.h>

#include <memory>
#include <optional>
#include <string>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NKikimr::NKqp {

struct TCboRunTiming {
    std::optional<ui64> RuntimeNs;
};

void AddCboWarning(TRBOContext& ctx, const TString& message);

std::shared_ptr<TCboRunTiming> AddCboRunTrace(
    TRBOContext& ctx,
    const TIntrusivePtr<TOpCBOTree>& cboTree,
    const std::shared_ptr<TJoinOptimizerNode>& initialJoinTree,
    const TVector<TCBOLeaf>& leaves,
    const TCBOSettings& settings,
    int optLevel,
    bool enableShuffleElimination,
    bool useBlockHashJoin,
    bool hasRowStorageInput,
    const TOptimizerHints& hints,
    const TSimpleSharedPtr<TOrderingsStateMachine>& shuffleFsm);

void AddCboDetailsTextTrace(TRBOContext& ctx, const std::string& title, const std::string& text);

} // namespace NKikimr::NKqp
