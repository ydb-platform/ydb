#pragma once

#include "defs.h"
#include "error_info.h"

#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/bootstrap.pb.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>

namespace NKikimr::NCms {

/**
 * A base class for storing the state of some group of nodes. For example, tenant nodes, state storage nodes, etc.
 *
 * Different groups of nodes may have their own failure model, so the checks for node permissions may be different.
 */
class INodesChecker {
public:
    enum ENodeState : ui32 {
        NODE_STATE_UNSPECIFIED /* "Unspecified" */,
        NODE_STATE_UP /* "Up" */,
        NODE_STATE_LOCKED /* "Locked" */,
        NODE_STATE_RESTART /* "Restart" */,
        NODE_STATE_DOWN /* "Down" */
    };

protected:
    static ENodeState NodeState(NKikimrCms::EState state);

public:
    virtual ~INodesChecker() = default;

    virtual void AddNode(ui32 nodeId) = 0;
    virtual void UpdateNode(ui32 nodeId, NKikimrCms::EState) = 0;

    virtual bool IsNodeLocked(ui32 nodeId) const = 0;
    virtual void LockNode(ui32 nodeId) = 0;
    virtual void UnlockNode(ui32 nodeId) = 0;

    virtual bool TryToLockNode(ui32 nodeId, NKikimrCms::EAvailabilityMode mode, TReason& reason) const = 0;
};

/**
 * Base class for simple nodes counter with some limits
 */
class TNodesCounterBase : public INodesChecker {
protected:
    THashMap<ui32, ENodeState> NodeToState;
    ui32 LockedNodesCount;
    ui32 DownNodesCount;

public:
    TNodesCounterBase()
        : LockedNodesCount(0)
        , DownNodesCount(0)
    {
    }

    void AddNode(ui32 nodeId) override;
    void UpdateNode(ui32 nodeId, NKikimrCms::EState) override;

    bool IsNodeLocked(ui32 nodeId) const override;
    void LockNode(ui32 nodeId) override;
    void UnlockNode(ui32 nodeId) override;

    const THashMap<ui32, ENodeState>& GetNodeToState() const;
};

/**
 * Base class for counting groups of nodes with a limit on the number of locked and disabled nodes.
 *
 * Each such group of nodes has parameters, which are set in CmsConfigItem
 *    DisabledNodesLimit - the maximum number of unavailable nodes
 *    DisabledNodesRatioLimit - the maximum percentage of unavailable nodes
 */
class TNodesLimitsCounterBase : public TNodesCounterBase {
protected:
    ui32 DisabledNodesLimit;
    ui32 DisabledNodesRatioLimit;

    virtual TString ReasonPrefix(ui32 nodeId) const {
         return TStringBuilder() << "Cannot lock node '" << nodeId << "'";
    }
    
    virtual TReason::EType DisabledNodesLimitReachedReasonType() const {
        return TReason::EType::DisabledNodesLimitReached;
    };

public:
    explicit TNodesLimitsCounterBase(ui32 disabledNodesLimit, ui32 disabledNodesRatioLimit)
        : DisabledNodesLimit(disabledNodesLimit)
        , DisabledNodesRatioLimit(disabledNodesRatioLimit)
    {
    }

    void ApplyLimits(ui32 nodesLimit, ui32 ratioLimit) {
        DisabledNodesLimit = nodesLimit;
        DisabledNodesRatioLimit = ratioLimit;
    }

    bool TryToLockNode(ui32 nodeId, NKikimrCms::EAvailabilityMode mode, TReason& reason) const override final;
};

class TTenantLimitsCounter : public TNodesLimitsCounterBase {
private:
    const TString TenantName;

protected:
    TString ReasonPrefix(ui32 nodeId) const override final {
        return TStringBuilder() << "Cannot lock node '" << nodeId << "' of tenant '" << TenantName << "'";
    }

    TReason::EType DisabledNodesLimitReachedReasonType() const override final {
        return TReason::EType::TenantDisabledNodesLimitReached;
    }

public:
    explicit TTenantLimitsCounter(const TString& tenantName, ui32 disabledNodesLimit, ui32 disabledNodesRatioLimit)
        : TNodesLimitsCounterBase(disabledNodesLimit, disabledNodesRatioLimit)
        , TenantName(tenantName)
    {
    }
};

class TClusterLimitsCounter : public TNodesLimitsCounterBase {
public:
    explicit TClusterLimitsCounter(ui32 disabledNodesLimit, ui32 disabledNodesRatioLimit)
        : TNodesLimitsCounterBase(disabledNodesLimit, disabledNodesRatioLimit)
    {
    }
};

/**
 * Class to hold information about nodes where can start some system tablet. Those nodes are
 * described in bootstrap config.
 *
 * At least one node from the bootstrap list must always be available
 */
class TSysTabletsNodesCounter : public TNodesCounterBase {
private:
    const NKikimrConfig::TBootstrap::ETabletType TabletType;

public:
    explicit TSysTabletsNodesCounter(NKikimrConfig::TBootstrap::ETabletType tabletType)
        : TabletType(tabletType)
    {
    }

    bool TryToLockNode(ui32 nodeId, NKikimrCms::EAvailabilityMode mode, TReason& reason) const override final;
};

} // namespace NKikimr::NCms
