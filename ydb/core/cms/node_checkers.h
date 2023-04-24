#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>
#include <ydb/core/erasure/erasure.h>
#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/protos/config.pb.h>

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

    virtual void LockNode(ui32 nodeId) = 0;
    virtual void UnlockNode(ui32 nodeId) = 0;

    virtual bool TryToLockNode(ui32 nodeId, NKikimrCms::EAvailabilityMode mode) const = 0;

    virtual std::string ReadableReason(ui32 nodeId, NKikimrCms::EAvailabilityMode mode) const = 0;
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
    {}

    virtual ~TNodesCounterBase() = default;

    void AddNode(ui32 nodeId) override;
    void UpdateNode(ui32 nodeId, NKikimrCms::EState) override;

    void LockNode(ui32 nodeId) override;
    void UnlockNode(ui32 nodeId) override;
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

public:
    TNodesLimitsCounterBase(ui32 disabledNodesLimit, ui32 disabledNodesRatioLimit)
        : DisabledNodesLimit(disabledNodesLimit)
        , DisabledNodesRatioLimit(disabledNodesRatioLimit)
    {
    }

    virtual ~TNodesLimitsCounterBase() = default;

    void ApplyLimits(ui32 nodesLimit, ui32 ratioLimit) {
        DisabledNodesLimit = nodesLimit;
        DisabledNodesRatioLimit = ratioLimit;
    }

    bool TryToLockNode(ui32 nodeId, NKikimrCms::EAvailabilityMode mode) const override final;
};

class TTenantLimitsCounter : public TNodesLimitsCounterBase {
private:
    const std::string TenantName;

public:
    TTenantLimitsCounter(const std::string &tenantName, ui32 disabledNodesLimit, ui32 disabledNodesRatioLimit)
        : TNodesLimitsCounterBase(disabledNodesLimit, disabledNodesRatioLimit)
        , TenantName(tenantName)
    {
    }

    std::string ReadableReason(ui32 nodeId, NKikimrCms::EAvailabilityMode mode) const override final {
        Y_UNUSED(mode);

        std::stringstream reason;
        reason << "Cannot lock node: " << nodeId
               << ". Too many locked nodes for tenant " << TenantName
               << "; locked: " << LockedNodesCount
               << "; down: " << DownNodesCount
               << "; total: " << NodeToState.size()
               << "; limit: " << DisabledNodesLimit
               << "; ratio limit: " << DisabledNodesRatioLimit << "%";

        return reason.str();
    }
};

class TClusterLimitsCounter : public TNodesLimitsCounterBase {
public:
    TClusterLimitsCounter(ui32 disabledNodesLimit, ui32 disabledNodesRatioLimit)
        : TNodesLimitsCounterBase(disabledNodesLimit, disabledNodesRatioLimit)
    {
    }

    std::string ReadableReason(ui32 nodeId, NKikimrCms::EAvailabilityMode mode) const override final {
        Y_UNUSED(mode);

        std::stringstream reason;
        reason << "Cannot lock node: " << nodeId
               <<". Too many locked nodes in cluster"
               << "; locked: " << LockedNodesCount
               << "; down: " << DownNodesCount
               << "; total: " << NodeToState.size()
               << "; limit: " << DisabledNodesLimit
               << "; ratio limit: " << DisabledNodesRatioLimit << "%";

        return reason.str();
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
    NKikimrConfig::TBootstrap::ETabletType TabletType;

public:
    explicit TSysTabletsNodesCounter(NKikimrConfig::TBootstrap::ETabletType tabletType)
        : TabletType(tabletType)
    {}

    bool TryToLockNode(ui32 nodeId, NKikimrCms::EAvailabilityMode mode) const override final;

    std::string ReadableReason(ui32 nodeId, NKikimrCms::EAvailabilityMode mode) const override final {
        std::stringstream reason;

        if (mode == NKikimrCms::MODE_FORCE_RESTART) {
            return reason.str();
        }

        reason << "Cannot lock node: " << nodeId
               << ". Tablet "
               << NKikimrConfig::TBootstrap_ETabletType_Name(TabletType)
               << " has too many unavailable nodes. Locked: " << LockedNodesCount
               << ". Down: " << DownNodesCount;
        if (mode == NKikimrCms::MODE_MAX_AVAILABILITY) {
            reason << ". Limit: " << NodeToState.size() / 2 << " (50%)";
        }

        if (mode == NKikimrCms::MODE_KEEP_AVAILABLE) {
            reason << ". Limit: " << NodeToState.size() - 1; 
        }

        return reason.str();
    }
};

} // namespace NKikimr::NCms
