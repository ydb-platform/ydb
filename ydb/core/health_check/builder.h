#pragma once

#include "log.h"
#include "health_check.h"
#include "health_check_data.h"
#include "health_check_helper.h"
#include "self_check_request.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/mind/tenant_slot_broker.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/util/proto_duration.h>
#include <ydb/core/util/tuples.h>

#include <ydb/core/protos/blobstorage_distributed_config.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/public/api/protos/ydb_monitoring.pb.h>

#include <regex>

namespace NKikimr::NHealthCheck {

struct TOverallStateContext {
    Ydb::Monitoring::SelfCheckResult* Result;
    Ydb::Monitoring::StatusFlag::Status Status = Ydb::Monitoring::StatusFlag::GREY;
    bool HasDegraded = false;
    std::unordered_set<std::pair<TString, TString>> IssueIds;

    TOverallStateContext(Ydb::Monitoring::SelfCheckResult* result) {
        Result = result;
    }

    void FillSelfCheckResult() {
        switch (Status) {
        case Ydb::Monitoring::StatusFlag::GREEN:
            Result->set_self_check_result(Ydb::Monitoring::SelfCheck::GOOD);
            break;
        case Ydb::Monitoring::StatusFlag::YELLOW:
            if (HasDegraded) {
                Result->set_self_check_result(Ydb::Monitoring::SelfCheck::DEGRADED);
            } else {
                Result->set_self_check_result(Ydb::Monitoring::SelfCheck::GOOD);
            }
            break;
        case Ydb::Monitoring::StatusFlag::BLUE:
            Result->set_self_check_result(Ydb::Monitoring::SelfCheck::DEGRADED);
            break;
        case Ydb::Monitoring::StatusFlag::ORANGE:
            Result->set_self_check_result(Ydb::Monitoring::SelfCheck::MAINTENANCE_REQUIRED);
            break;
        case Ydb::Monitoring::StatusFlag::RED:
            Result->set_self_check_result(Ydb::Monitoring::SelfCheck::EMERGENCY);
            break;
        default:
            break;
        }
    }

    void UpdateMaxStatus(Ydb::Monitoring::StatusFlag::Status status) {
        Status = MaxStatus(Status, status);
    }

    void AddIssues(TList<TSelfCheckResult::TIssueRecord>& issueRecords) {
        for (auto& issueRecord : issueRecords) {
            std::pair<TString, TString> key{issueRecord.IssueLog.location().database().name(), issueRecord.IssueLog.id()};
            if (IssueIds.emplace(key).second) {
                Result->mutable_issue_log()->Add()->CopyFrom(issueRecord.IssueLog);
            }
        }
    }
};

template <typename Derived>
class TBuilderBase {
public:
    template <typename... TArgs>
    decltype(auto) Build(TArgs&&... args) {
        return static_cast<Derived*>(this)->BuildImpl(std::forward<TArgs>(args)...);
    }
};

// each builder incapsulates neccesary data from /home/andrew-rykov/git/ydb/ydb/core/health_check/builder.cpp
// and provides declarartion BuildImpl method to build the result, implementation in /home/andrew-rykov/git/ydb/ydb/core/health_check/builder.cpp
template<typename TDatabase, typename TSystemTablets, typename TStorage>
class TBuilderResult : TBuilderBase<TBuilderResult<TDatabase, TSystemTablets, TStorage>> {
public:
    TString DomainPath;
    TString FilterDatabase;
    bool HaveAllBSControllerInfo;
    THashMap<TString, TDatabaseState>& DatabaseState;
    THashMap<ui64, TStoragePoolState>& StoragePoolState;
    THashSet<ui64>& StoragePoolSeen;
    THashMap<TString, TStoragePoolState>& StoragePoolStateByName;
    THashSet<TString>& StoragePoolSeenByName;

    THolder<TDatabase> BuilderDatabase;
    THolder<TSystemTablets> BuilderSystemTablets;
    THolder<TStorage> BuilderStorage;
    TBuilderResult(THolder<TDatabase> builderDatabase,
                   THolder<TSystemTablets> builderSystemTablets,
                   THolder<TStorage> builderStorage,
                   const TString& domainPath,
                   const TString& filterDatabase,
                   const bool haveAllBSControllerInfo,
                   THashMap<TString, TDatabaseState>& databaseState,
                   THashMap<ui64, TStoragePoolState>& storagePoolState,
                   THashSet<ui64>& storagePoolSeen,
                   THashMap<TString, TStoragePoolState>& storagePoolStateByName,
                   THashSet<TString>& storagePoolSeenByName)
        : BuilderDatabase(builderDatabase)
        , BuilderSystemTablets(builderSystemTablets)
        , BuilderStorage(builderStorage)
        , DomainPath(domainPath)
        , FilterDatabase(filterDatabase)
        , HaveAllBSControllerInfo(haveAllBSControllerInfo)
        , DatabaseState(databaseState)
        , StoragePoolState(storagePoolState)
        , StoragePoolSeen(storagePoolSeen)
        , StoragePoolStateByName(storagePoolStateByName)
        , StoragePoolSeenByName(storagePoolSeenByName)
    {}

private:
    bool IsSpecificDatabaseFilter() const {
        return FilterDatabase && FilterDatabase != DomainPath;
    }

    void BuildImpl(TOverallStateContext context);
};

template<typename TCompute, typename TStorage>
class TBuilderDatabase : TBuilderBase<TBuilderDatabase<TCompute, TStorage>> {
public:
    THolder<TCompute> BuilderCompute;
    THolder<TStorage> BuilderStorage;
    bool IsSpecificDatabaseFilter;

    TBuilderDatabase(THolder<TCompute> builderCompute, THolder<TStorage> builderStorage, const bool isSpecificDatabaseFilter)
        : BuilderCompute(builderCompute)
        , BuilderStorage(builderStorage)
        , IsSpecificDatabaseFilter(isSpecificDatabaseFilter)
    {}

    void BuildImpl(TOverallStateContext& context, const TString& path, TDatabaseState& state);
};

template<typename TComputeNode, typename TSystemTablets, typename TComputeDatabase, typename TTablets>
class TBuilderCompute {
public:
    THashMap<TSubDomainKey, TString> FilterDomainKey;
    THashMap<TNodeId, const NKikimrWhiteboard::TSystemStateInfo*> MergedNodeSystemState;

    THolder<TComputeNode> BuilderComputeNode;
    THolder<TSystemTablets> BuilderSystemTablets;
    THolder<TComputeDatabase> BuilderComputeDatabase;
    THolder<TTablets> BuilderTablets;

    TBuilderCompute(THolder<TComputeNode> builderComputeNode,
                    THolder<TSystemTablets> builderSystemTablets,
                    THolder<TComputeDatabase> builderComputeDatabase,
                    const TTablets& builderTablets,
                    const THashMap<TSubDomainKey, TString>& filterDomainKey,
                    const THashMap<TNodeId, const NKikimrWhiteboard::TSystemStateInfo*> mergedNodeSystemState)
        : BuilderComputeNode(builderComputeNode)
        , BuilderSystemTablets(builderSystemTablets)
        , BuilderComputeDatabase(builderComputeDatabase)
        , BuilderTablets(builderTablets)
        , FilterDomainKey(filterDomainKey)
        , MergedNodeSystemState(mergedNodeSystemState)
    {}

    void BuildImpl(TDatabaseState& databaseState, Ydb::Monitoring::ComputeStatus& computeStatus, TSelfCheckResult context);
};

template<typename TDescribe>
class TBuilderComputeDatabase : public TBuilderBase<TBuilderComputeDatabase<TDescribe>> {
public:
    TDescribe& DescribeByPath;

    TBuilderComputeDatabase(const TDescribe& describeByPath)
        : DescribeByPath(describeByPath)
    {}

    void BuildImpl(TDatabaseState& databaseState, Ydb::Monitoring::ComputeStatus& computeStatus, TSelfCheckResult context);
};

template<typename TSystemTablets, typename TComputeDatabase, typename TTablets>
class TBuilderComputeNode : public TBuilderBase<TBuilderComputeNode<TSystemTablets, TComputeDatabase, TTablets>> {
    THolder<TSystemTablets> BuilderSystemTablets;
    THolder<TComputeDatabase> BuilderComputeDatabase;
    THolder<TTablets> BuilderTablets;
public:
    TBuilderComputeNode(THolder<TSystemTablets> builderSystemTablets,
                        THolder<TComputeDatabase> builderComputeDatabase,
                        THolder<TTablets> builderTablets)
        : BuilderSystemTablets(builderSystemTablets)
        , BuilderComputeDatabase(builderComputeDatabase)
        , BuilderTablets(builderTablets)
    {}

    void BuildImpl(TDatabaseState& databaseState, TNodeId nodeId, Ydb::Monitoring::ComputeNodeStatus& computeNodeStatus, TSelfCheckResult context);
};

class TBuilderTablets : public TBuilderBase<TBuilderTablets> {
public:
    void BuildImpl(TDatabaseState& databaseState, TNodeId nodeId, Ydb::Monitoring::ComputeTabletStatus& computeTabletStatus, TSelfCheckResult context);
};

class TBuilderSystemTablets : public TBuilderBase<TBuilderSystemTablets> {
    TTabletRequestsState& TabletRequests;
public:
    TBuilderSystemTablets(TTabletRequestsState& tabletRequests)
        : TabletRequests(tabletRequests)
    {}

    Ydb::Monitoring::StatusFlag::Status BuildImpl(TDatabaseState& databaseState, TSelfCheckResult context);
};

template<typename TPool>
class TBuilderStorage : public TBuilderBase<TBuilderStorage<TPool>> {
public:
    THolder<TPool> BuilderPool;

    TBuilderStorage(THolder<TPool> builderPool)
        : BuilderPool(builderPool)
    {}

    void BuildImpl(TDatabaseState& databaseState, Ydb::Monitoring::StorageStatus& storageStatus, TSelfCheckResult context);
};

template<typename TGroup>
class TBuilderStoragePool : public TBuilderBase<TBuilderStoragePool<TGroup>> {
public:
    THolder<TGroup> BuilderGroup;

    TBuilderStoragePool(THolder<TGroup> builderGroup)
        : BuilderGroup(builderGroup)
    {}

    void BuildImpl(const TStoragePoolState& pool, Ydb::Monitoring::StoragePoolStatus& storagePoolStatus, TSelfCheckResult context);
};

template<typename TVDisk>
class TBuilderStorageGroup : public TBuilderBase<TBuilderStorageGroup<TVDisk>> {
public:
    THolder<TVDisk> BuilderVDisk;

    TBuilderStorageGroup(THolder<TVDisk> builderVDisk)
        : BuilderVDisk(builderVDisk)
    {}

    void BuildImpl(TGroupId groupId, Ydb::Monitoring::StorageGroupStatus& storageGroupStatus, TSelfCheckResult context);
};

template<typename TVDisk>
class TBuilderStorageGroupWithWhiteboard : public TBuilderBase<TBuilderStorageGroupWithWhiteboard<TVDisk>> {
public:
    THolder<TVDisk> BuilderVDisk;

    TBuilderStorageGroupWithWhiteboard(THolder<TVDisk> builderVDisk)
        : BuilderVDisk(builderVDisk)
    {}

    void BuildImpl(TGroupId groupId, const NKikimrWhiteboard::TBSGroupStateInfo& groupInfo, Ydb::Monitoring::StorageGroupStatus& storageGroupStatus, TSelfCheckResult context);
};

template<typename TPDisk>
class TBuilderStorageVDisk : public TBuilderBase<TBuilderStorageVDisk<TPDisk>> {
public:
    THolder<TPDisk> BuilderPDisk;

    TBuilderStorageVDisk(THolder<TPDisk> builderPDisk)
        : BuilderPDisk(builderPDisk)
    {}

    void BuildImpl(const NKikimrSysView::TVSlotEntry* vSlot, Ydb::Monitoring::StorageVDiskStatus& storageVDiskStatus, TSelfCheckResult context);
};

template<typename TPDisk>
class TBuilderStorageVDiskWithWhiteboard : public TBuilderBase<TBuilderStorageVDiskWithWhiteboard<TPDisk>> {
public:
    THolder<TPDisk> BuilderPDisk;

    TBuilderStorageVDiskWithWhiteboard(THolder<TPDisk> builderPDisk)
        : BuilderPDisk(builderPDisk)
    {}

    void BuildImpl(const TString& vDiskId, const NKikimrWhiteboard::TVDiskStateInfo& vDiskInfo, Ydb::Monitoring::StorageVDiskStatus& storageVDiskStatus, TSelfCheckResult context);
};

class TBuilderStoragePDisk : public TBuilderBase<TBuilderStoragePDisk> {
public:
    void BuildImpl(const TString& pDiskId, Ydb::Monitoring::StoragePDiskStatus& storagePDiskStatus, TSelfCheckResult context);
};

class TBuilderStoragePDiskWithWhiteboard : public TBuilderBase<TBuilderStoragePDiskWithWhiteboard> {
public:
    void BuildImpl(const TString& pDiskId, const NKikimrWhiteboard::TPDiskStateInfo& pDiskInfo, Ydb::Monitoring::StoragePDiskStatus& storagePDiskStatus, TSelfCheckResult context);
};

} // NKikimr::NHealthCheck
