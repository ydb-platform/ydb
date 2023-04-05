#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>
#include <ydb/core/erasure/erasure.h>
#include <ydb/core/protos/cms.pb.h>

#include <library/cpp/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/system/compiler.h>
#include <util/system/yassert.h>

#include <bitset>
#include <sstream>
#include <algorithm>
#include <string>

namespace NKikimr::NCms {

class TNodesStateBase {
public:
    enum ENodeState : ui32 {
        NODE_STATE_UNSPECIFIED /* "Unspecified" */,
        NODE_STATE_UP /* "Up" */,
        NODE_STATE_LOCKED /* "Locked" */,
        NODE_STATE_RESTART /* "Restart" */,
        NODE_STATE_DOWN /* "Down" */
    };

private:
    static ENodeState NodeState(NKikimrCms::EState state);

protected:
    ui32 DisabledNodesLimit;
    ui32 DisabledNodesRatioLimit;

    THashMap<ui32, ENodeState> NodeToState;
    ui32 LockedNodesCount;
    ui32 DownNodesCount;

public:
    TNodesStateBase(ui32 disabledNodesLimit, ui32 disabledNodesRatioLimit)
        : DisabledNodesLimit(disabledNodesLimit)
        , DisabledNodesRatioLimit(disabledNodesRatioLimit)
        , LockedNodesCount(0)
        , DownNodesCount(0)
    {
    }

    virtual ~TNodesStateBase() = default;

    void ApplyLimits(ui32 nodesLimit, ui32 ratioLimit) {
        DisabledNodesLimit = nodesLimit;
        DisabledNodesRatioLimit = ratioLimit;
    }

    void AddNode(ui32 nodeId);
    void UpdateNode(ui32 nodeId, NKikimrCms::EState);

    void LockNode(ui32 nodeId);
    void UnlockNode(ui32 nodeId);

    bool TryToLockNode(ui32 nodeId, bool isForceRestart = false);

    virtual std::string ReadableReason() const = 0;

};

class TTenantState : public TNodesStateBase {
private:
    const std::string TenantName;

public:
    TTenantState(const std::string &tenantName, ui32 disabledNodesLimit, ui32 disabledNodesRatioLimit)
        : TNodesStateBase(disabledNodesLimit, disabledNodesRatioLimit)
        , TenantName(tenantName)
    {
    }

    std::string ReadableReason() const override final {
        std::stringstream reason;
        reason << "Too many locked nodes for tenant " << TenantName
               << "; locked: " << LockedNodesCount
               << "; down: " << DownNodesCount
               << "; total: " << NodeToState.size()
               << "; limit: " << DisabledNodesLimit
               << "; ratio limit: " << DisabledNodesRatioLimit << "%";

        return reason.str();
    }
};

class TClusterNodesState : public TNodesStateBase {
public:
    TClusterNodesState(ui32 disabledNodesLimit, ui32 disabledNodesRatioLimit)
        : TNodesStateBase(disabledNodesLimit, disabledNodesRatioLimit)
    {
    }

    std::string ReadableReason() const override final {
        std::stringstream reason;
        reason << "Too many locked nodes in cluster"
               << "; locked: " << LockedNodesCount
               << "; down: " << DownNodesCount
               << "; total: " << NodeToState.size()
               << "; limit: " << DisabledNodesLimit
               << "; ratio limit: " << DisabledNodesRatioLimit << "%";

        return reason.str();
    }
};

} // namespace NKikimr::NCms
