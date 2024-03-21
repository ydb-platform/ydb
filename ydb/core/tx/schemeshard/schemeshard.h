#pragma once
#include "defs.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/storage_pools.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/protos/tx_scheme.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/library/ydb_issue/issue_helpers.h>

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>
#include <library/cpp/object_factory/object_factory.h>

#include "schemeshard_identificators.h"

namespace NKikimr {
namespace NSchemeShard {

static constexpr ui64 RootSchemeShardId = 0;
static constexpr ui64 RootPathId = 1;

class TSchemeShard;
struct TSchemeLimits;

class ISSDataProcessor {
protected:
    virtual void DoProcess(TSchemeShard& schemeShard, NKikimrScheme::TEvProcessingResponse& result) const = 0;
    virtual bool DoDeserializeFromString(const TString& data) = 0;
    virtual TString DoSerializeToString() const = 0;
public:
    using TPtr = std::shared_ptr<ISSDataProcessor>;
    using TFactory = NObjectFactory::TObjectFactory<ISSDataProcessor, TString>;
    virtual ~ISSDataProcessor() = default;

    virtual TString DebugString() const = 0;

    TString SerializeToString() const {
        return DoSerializeToString();
    }
    bool DeserializeFromString(const TString& data) {
        return DoDeserializeFromString(data);
    }

    virtual TString GetClassName() const = 0;

    void Process(TSchemeShard& schemeShard, NKikimrScheme::TEvProcessingResponse& result) {
        return DoProcess(schemeShard, result);
    }
};

struct TEvSchemeShard {
    enum EEv {
        EvModifySchemeTransaction = EventSpaceBegin(TKikimrEvents::ES_FLAT_TX_SCHEMESHARD),  // 271122432
        EvModifySchemeTransactionResult = EvModifySchemeTransaction + 1 * 512,
        EvDescribeScheme,
        EvDescribeSchemeResult,
        EvFindTabletSubDomainPathId,
        EvFindTabletSubDomainPathIdResult,
        EvSubDomainPathIdFound,

        EvInitRootShard = EvModifySchemeTransaction + 5 * 512,
        EvInitRootShardResult,
        EvUpdateConfig,
        EvUpdateConfigResult,
        EvNotifyTxCompletion,
        EvNotifyTxCompletionRegistered, // 271124997 (0x10290a05)
        EvNotifyTxCompletionResult,
        EvMeasureSelfResponseTime,
        EvWakeupToMeasureSelfResponseTime,
        EvInitTenantSchemeShard,
        EvInitTenantSchemeShardResult, // 271125002
        EvSyncTenantSchemeShard,
        EvUpdateTenantSchemeShard,
        EvMigrateSchemeShard,
        EvMigrateSchemeShardResult,
        EvPublishTenantAsReadOnly,
        EvPublishTenantAsReadOnlyResult,
        EvRewriteOwner,
        EvRewriteOwnerResult,
        EvPublishTenant,
        EvPublishTenantResult,  // 271125012
        EvLogin,
        EvLoginResult,


        EvBackupDatashard = EvModifySchemeTransaction + 6 * 512,
        EvBackupDatashardResult,
        EvCancelTx,
        EvCancelTxResult,
        EvProcessingRequest,
        EvProcessingResponse,

        EvOwnerActorAck,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_FLAT_TX_SCHEMESHARD), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_FLAT_TX_SCHEMESHARD)");

    struct TEvProcessingResponse: public TEventPB<TEvProcessingResponse,
        NKikimrScheme::TEvProcessingResponse, EvProcessingResponse> {
    private:
        using TBase = TEventPB<TEvProcessingResponse,
            NKikimrScheme::TEvProcessingResponse, EvProcessingResponse>;
    public:
        using TBase::TBase;
        TEvProcessingResponse(const TString& errorMessage) {
            Record.MutableError()->SetErrorMessage(errorMessage);
        }
    };

    struct TEvProcessingRequest: public TEventPB<TEvProcessingRequest,
        NKikimrScheme::TEvProcessingRequest, EvProcessingRequest> {
    private:
        using TBase = TEventPB<TEvProcessingRequest,
            NKikimrScheme::TEvProcessingRequest, EvProcessingRequest>;
    public:
        using TBase::TBase;
        TEvProcessingRequest(const ISSDataProcessor& processor) {
            Record.SetClassName(processor.GetClassName());
            Record.SetData(processor.SerializeToString());
        }

        ISSDataProcessor::TPtr RestoreProcessor() const {
            auto result = ISSDataProcessor::TFactory::MakeHolder(TBase::Record.GetClassName());
            if (!result) {
                return nullptr;
            } else if (!result->DeserializeFromString(TBase::Record.GetData())) {
                return nullptr;
            } else {
                return std::shared_ptr<ISSDataProcessor>(result.Release());
            }
        }
    };

    struct TEvModifySchemeTransaction : public TEventPB<TEvModifySchemeTransaction,
                                                        NKikimrScheme::TEvModifySchemeTransaction,
                                                        EvModifySchemeTransaction> {
        TEvModifySchemeTransaction()
        {}

        TEvModifySchemeTransaction(ui64 txid, ui64 tabletId)
        {
            Record.SetTxId(txid);
            Record.SetTabletId(tabletId);
        }

        TString ToString() const {
            TStringStream str;
            str << "{TEvModifySchemeTransaction";
            if (Record.HasTxId()) {
                str << " txid# " << Record.GetTxId();
            }
            if (Record.HasTabletId()) {
                str << " TabletId# " << Record.GetTabletId();
            }
            str << "}";
            return str.Str();
        }
    };

    struct TEvCancelTx
        : public TEventPB<TEvCancelTx,
                          NKikimrScheme::TEvCancelTx,
                          EvCancelTx>
    {
    };

    struct TEvCancelTxResult:
        public TEventPB<TEvCancelTxResult,
                        NKikimrScheme::TEvCancelTxResult,
                        EvCancelTxResult>
    {
        TEvCancelTxResult() = default;
        TEvCancelTxResult(ui64 targetTxId, ui64 txId) {
            Record.SetTargetTxId(targetTxId);
            Record.SetTxId(txId);
        }
    };

    using EStatus = NKikimrScheme::EStatus;

    struct TEvModifySchemeTransactionResult : public TEventPB<TEvModifySchemeTransactionResult,
                                                              NKikimrScheme::TEvModifySchemeTransactionResult,
                                                              EvModifySchemeTransactionResult> {

        TEvModifySchemeTransactionResult()
        {}

        TEvModifySchemeTransactionResult(TTxId txid, TTabletId schemeshardId) {
            Record.SetTxId(ui64(txid));
            Record.SetSchemeshardId(ui64(schemeshardId));
        }

        TEvModifySchemeTransactionResult(EStatus status, ui64 txid, ui64 schemeshardId, const TStringBuf& reason = TStringBuf())
            : TEvModifySchemeTransactionResult(TTxId(txid), TTabletId(schemeshardId))
        {
            Record.SetStatus(status);
            if (reason.size() > 0) {
                Record.SetReason(reason.data(), reason.size());
            }
        }

        bool IsAccepted() const {
            return Record.GetReason().empty() && (Record.GetStatus() == EStatus::StatusAccepted);
        }

        bool IsConditionalAccepted() const {
            //happens on retries, we answer like StatusAccepted with error message and do nothing in operation
            return !Record.GetReason().empty() && (Record.GetStatus() == EStatus::StatusAccepted);
        }

        bool IsDone() const {
            return Record.GetReason().empty() && (Record.GetStatus() == EStatus::StatusSuccess);
        }

        void SetStatus(EStatus status, const TString& reason = {}) {
            Record.SetStatus(status);
            if (reason) {
                Record.SetReason(reason);
            }
        }

        void SetError(EStatus status, const TString& errStr) {
            Record.SetStatus(status);
            Record.SetReason(errStr);
        }

        void AddWarning(const TString& text) {
            auto issue = MakeIssue(NKikimrIssues::TIssuesIds::WARNING, text);
            NYql::IssueToMessage(issue, Record.AddIssues());
        }

        void AddNotice(const TString& text) {
            auto issue = MakeIssue(NKikimrIssues::TIssuesIds::INFO, text);
            NYql::IssueToMessage(issue, Record.AddIssues());
        }

        void SetPathCreateTxId(ui64 txId) { Record.SetPathCreateTxId(txId); }
        void SetPathDropTxId(ui64 txId) { Record.SetPathDropTxId(txId); }
        void SetPathId(ui64 pathId) { Record.SetPathId(pathId); }

        TString ToString() const {
            TStringStream str;
            str << "{TEvModifySchemeTransactionResult";
            if (Record.HasStatus()) {
                str << " Status# " << Record.GetStatus();
            }
            if (Record.HasTxId()) {
                str << " txid# " << Record.GetTxId();
            }
            if (Record.HasReason()) {
                str << " Reason# " << Record.GetReason();
            }
            str << "}";
            return str.Str();
        }
    };

    struct TEvInitRootShard : public TEventPB<TEvInitRootShard, NKikimrTxScheme::TEvInitRootShard, EvInitRootShard> {
        TEvInitRootShard()
        {}

        TEvInitRootShard(const TActorId& source, ui32 rootTag, const TString& rootTagName)
        {
            ActorIdToProto(source, Record.MutableSource());
            Record.SetRootTag(rootTag);
            Record.SetRootTagName(rootTagName);
        }
    };

    struct TEvInitRootShardResult : public TEventPB<TEvInitRootShardResult,
        NKikimrTxScheme::TEvInitRootShardResult, EvInitRootShardResult> {
        enum EStatus {
            StatusUnknown,
            StatusSuccess,
            StatusAlreadyInitialized,
            StatusBadArgument
        };

        TEvInitRootShardResult()
        {}

        TEvInitRootShardResult(ui64 origin, EStatus status)
        {
            Record.SetOrigin(origin);
            Record.SetStatus(status);
        }
    };

    struct TEvDescribeScheme : public TEventPB<TEvDescribeScheme,
                                               NKikimrSchemeOp::TDescribePath,
                                               EvDescribeScheme> {
        TEvDescribeScheme()
        {}

        TEvDescribeScheme(const NKikimrSchemeOp::TDescribePath& describePath)
        {
            Record.CopyFrom(describePath);
        }

        TEvDescribeScheme(const TString& path)
        {
            Record.SetPath(path);
        }

        TEvDescribeScheme(ui64 tabletId, ui64 pathId)
        {
            Record.SetSchemeshardId(tabletId);
            Record.SetPathId(pathId);
        }

        TEvDescribeScheme(TTableId tableId)
        {
            Record.SetSchemeshardId(tableId.PathId.OwnerId);
            Record.SetPathId(tableId.PathId.LocalPathId);
        }

        TEvDescribeScheme(NKikimr::TPathId pathId)
        {
            Record.SetSchemeshardId(pathId.OwnerId);
            Record.SetPathId(pathId.LocalPathId);
        }
    };

    struct TEvDescribeSchemeResult : public TEventPreSerializedPB<TEvDescribeSchemeResult,
                                                                  NKikimrScheme::TEvDescribeSchemeResult,
                                                                  EvDescribeSchemeResult> {
        TEvDescribeSchemeResult() = default;

        TEvDescribeSchemeResult(const TString& path, TPathId pathId)
        {
            Record.SetPath(path);
            Record.SetPathId(pathId.LocalPathId);
            Record.SetPathOwnerId(pathId.OwnerId);
        }

        // TEventPreSerializedPB::ToString() calls TEventPreSerializedPB::GetRecord()
        // which reconstructs full message by deserializing PreSerializedData.
        // That could be expensive for NKikimrScheme::TEvDescribeSchemeResult (e.g.
        // table with huge number of partitions).
        // Override ToString() to avoid unintentional message reconstruction.
        TString ToString() const override {
            TStringStream str;
            str << ToStringHeader()
                << " PreSerializedData size# " << PreSerializedData.size()
                << " Record# " << Record.ShortDebugString()
            ;
            return str.Str();
        }
    };

    struct TEvDescribeSchemeResultBuilder : TEvDescribeSchemeResult {
        using TBase::Record;

        TEvDescribeSchemeResultBuilder() = default;

        TEvDescribeSchemeResultBuilder(const TString& path, TPathId pathId)
            : TEvDescribeSchemeResult(path, pathId)
        {
        }
    };

    struct TEvNotifyTxCompletion : public TEventPB<TEvNotifyTxCompletion,
                                                     NKikimrScheme::TEvNotifyTxCompletion,
                                                     EvNotifyTxCompletion> {
        explicit TEvNotifyTxCompletion(ui64 txId = 0)
        {
            Record.SetTxId(txId);
        }
    };

    struct TEvNotifyTxCompletionRegistered : public TEventPB<TEvNotifyTxCompletionRegistered,
                                                     NKikimrScheme::TEvNotifyTxCompletionRegistered,
                                                     EvNotifyTxCompletionRegistered> {
        explicit TEvNotifyTxCompletionRegistered(ui64 txId = 0)
        {
            Record.SetTxId(txId);
        }
    };

    struct TEvNotifyTxCompletionResult : public TEventPB<TEvNotifyTxCompletionResult,
                                                     NKikimrScheme::TEvNotifyTxCompletionResult,
                                                     EvNotifyTxCompletionResult> {
        explicit TEvNotifyTxCompletionResult(ui64 txId = 0)
        {
            Record.SetTxId(txId);
        }
    };

    struct TEvMeasureSelfResponseTime : public TEventLocal<TEvMeasureSelfResponseTime, EvMeasureSelfResponseTime> {
    };

    struct TEvWakeupToMeasureSelfResponseTime : public TEventLocal<TEvWakeupToMeasureSelfResponseTime, EvWakeupToMeasureSelfResponseTime> {
    };

    struct TEvInitTenantSchemeShard: public TEventPB<TEvInitTenantSchemeShard,
                                                            NKikimrScheme::TEvInitTenantSchemeShard,
                                                            EvInitTenantSchemeShard> {

        TEvInitTenantSchemeShard() = default;

        TEvInitTenantSchemeShard(ui64 selfTabletId,
                                 ui64 pathId, TString tenantRootPath,
                                 TString owner, TString effectiveRootACL, ui64 effectiveRootACLVersion,
                                 const NKikimrSubDomains::TProcessingParams& processingParams,
                                 const TStoragePools& storagePools,
                                 const TMap<TString, TString> userAttrData, ui64 UserAttrsVersion,
                                 const TSchemeLimits& limits, ui64 sharedHive, const TPathId& resourcesDomainId = TPathId());
    };

    struct TEvInitTenantSchemeShardResult: public TEventPB<TEvInitTenantSchemeShardResult,
                                                            NKikimrScheme::TEvInitTenantSchemeShardResult,
                                                            EvInitTenantSchemeShardResult> {

        TEvInitTenantSchemeShardResult() = default;

        TEvInitTenantSchemeShardResult(ui64 tenantSchemeShard, EStatus status) {
            Record.SetTenantSchemeShard(tenantSchemeShard);
            Record.SetStatus(status);
        }
    };

    struct TEvMigrateSchemeShard: public TEventPB<TEvMigrateSchemeShard,
                                                   NKikimrScheme::TEvMigrate,
                                                   EvMigrateSchemeShard> {
        TEvMigrateSchemeShard() = default;

        TPathId GetPathId() const {
            if (!Record.GetPath().HasPathId()) {
                return NKikimr::TPathId();
            }

            auto& pathId = Record.GetPath().GetPathId();
            return NKikimr::TPathId(pathId.GetOwnerId(), pathId.GetLocalId());
        }
    };

    struct TEvMigrateSchemeShardResult: public TEventPB<TEvMigrateSchemeShardResult,
                                                   NKikimrScheme::TEvMigrateResult,
                                                   EvMigrateSchemeShardResult> {
        TEvMigrateSchemeShardResult() = default;

        TEvMigrateSchemeShardResult(ui64 tenantSchemeShard) {
            Record.SetTenantSchemeShard(tenantSchemeShard);
        }

        TPathId GetPathId() const {
            if (!Record.HasPathId()) {
                return NKikimr::TPathId();
            }

            auto& pathId = Record.GetPathId();
            return NKikimr::TPathId(pathId.GetOwnerId(), pathId.GetLocalId());
        }
    };

    struct TEvPublishTenantAsReadOnly: public TEventPB<TEvPublishTenantAsReadOnly,
                                              NKikimrScheme::TEvPublishTenantAsReadOnly,
                                              EvPublishTenantAsReadOnly> {
        TEvPublishTenantAsReadOnly() = default;
        TEvPublishTenantAsReadOnly(ui64 domainSchemeShardID) {
            Record.SetDomainSchemeShard(domainSchemeShardID);
        }
    };

    struct TEvPublishTenantAsReadOnlyResult: public TEventPB<TEvPublishTenantAsReadOnlyResult,
                                                        NKikimrScheme::TEvPublishTenantAsReadOnlyResult,
                                                        EvPublishTenantAsReadOnlyResult> {
        TEvPublishTenantAsReadOnlyResult() = default;
        TEvPublishTenantAsReadOnlyResult(ui64 tenantSchemeShard, EStatus status) {
            Record.SetTenantSchemeShard(tenantSchemeShard);
            Record.SetStatus(status);
        }
    };

    struct TEvRewriteOwner: public TEventPB<TEvRewriteOwner,
                                             NKikimrScheme::TEvRewriteOwner,
                                             EvRewriteOwner> {
        TEvRewriteOwner() = default;
    };

    struct TEvRewriteOwnerResult: public TEventPB<TEvRewriteOwnerResult,
                                             NKikimrScheme::TEvRewriteOwnerResult,
                                             EvRewriteOwnerResult> {
        TEvRewriteOwnerResult() = default;
    };

    struct TEvPublishTenant: public TEventPB<TEvPublishTenant,
                                                        NKikimrScheme::TEvPublishTenant,
                                                        EvPublishTenant> {
        TEvPublishTenant() = default;
        TEvPublishTenant(ui64 domainSchemeShardID) {
            Record.SetDomainSchemeShard(domainSchemeShardID);
        }
    };

    struct TEvPublishTenantResult: public TEventPB<TEvPublishTenantResult,
                                                              NKikimrScheme::TEvPublishTenantResult,
                                                              EvPublishTenantResult> {
        TEvPublishTenantResult() = default;
        TEvPublishTenantResult(ui64 tenantSchemeShard) {
            Record.SetTenantSchemeShard(tenantSchemeShard);
        }
    };

    struct TEvSyncTenantSchemeShard: public TEventPB<TEvSyncTenantSchemeShard,
                                                      NKikimrScheme::TEvSyncTenantSchemeShard,
                                                      EvSyncTenantSchemeShard> {
        TEvSyncTenantSchemeShard() = default;

        struct TEvSyncTenantSchemeShardInitializer {
            TPathId DomainKey;
            ui64 TabletId;
            ui64 Generation;
            ui64 EffectiveACLVersion;
            ui64 SubdomainVersion;
            ui64 UserAttrsVersion;
            ui64 TenantHive;
            ui64 TenantSysViewProcessor;
            ui64 TenantStatisticsAggregator;
            ui64 TenantGraphShard;
            TString RootACL;
        };

        TEvSyncTenantSchemeShard(const TEvSyncTenantSchemeShardInitializer& _)
        {
            Record.SetDomainSchemeShard(_.DomainKey.OwnerId);
            Record.SetDomainPathId(_.DomainKey.LocalPathId);

            Record.SetTabletID(_.TabletId);
            Record.SetGeneration(_.Generation);

            Record.SetEffectiveACLVersion(_.EffectiveACLVersion);
            Record.SetSubdomainVersion(_.SubdomainVersion);
            Record.SetUserAttributesVersion(_.UserAttrsVersion);

            Record.SetTenantHive(_.TenantHive);
            Record.SetTenantSysViewProcessor(_.TenantSysViewProcessor);
            Record.SetTenantStatisticsAggregator(_.TenantStatisticsAggregator);
            Record.SetTenantGraphShard(_.TenantGraphShard);

            Record.SetTenantRootACL(_.RootACL);
        }

    };

    struct TEvUpdateTenantSchemeShard: public TEventPB<TEvUpdateTenantSchemeShard,
                                                            NKikimrScheme::TEvUpdateTenantSchemeShard,
                                                            EvUpdateTenantSchemeShard> {

        TEvUpdateTenantSchemeShard() = default;

        TEvUpdateTenantSchemeShard(ui64 tabletId, ui64 generation) {
            Record.SetTabletId(tabletId);
            Record.SetGeneration(generation);
        }

        void SetEffectiveACL(const TString& owner, const TString& effectiveACL, ui64 version) {
            Record.SetOwner(owner);
            Record.SetEffectiveACL(effectiveACL);
            Record.SetEffectiveACLVersion(version);
        }

        void SetStoragePools(const TStoragePools& storagePools, ui64 version) {
            for (auto& x: storagePools) {
                *Record.AddStoragePools() = x;
            }
            Record.SetSubdomainVersion(version);
        }

        void SetUserAttrs(const TMap<TString, TString>& userAttrData, ui64 version) {
            for (auto& x: userAttrData) {
                auto item = Record.AddUserAttributes();
                item->SetKey(x.first);
                item->SetValue(x.second);
            }
            Record.SetUserAttributesVersion(version);
        }

        void SetTenantHive(ui64 hive) {
            Record.SetTenantHive(hive);
        }

        void SetTenantSysViewProcessor(ui64 svp) {
            Record.SetTenantSysViewProcessor(svp);
        }

        void SetTenantStatisticsAggregator(ui64 sa) {
            Record.SetTenantStatisticsAggregator(sa);
        }

        void SetUpdateTenantRootACL(const TString& acl) {
            Record.SetUpdateTenantRootACL(acl);
        }

        void SetTenantGraphShard(ui64 gs) {
            Record.SetTenantGraphShard(gs);
        }
    };

    struct TEvFindTabletSubDomainPathId
        : public TEventPB<TEvFindTabletSubDomainPathId,
                          NKikimrScheme::TEvFindTabletSubDomainPathId,
                          EvFindTabletSubDomainPathId>
    {
        TEvFindTabletSubDomainPathId() = default;

        explicit TEvFindTabletSubDomainPathId(ui64 tabletId) {
            Record.SetTabletId(tabletId);
        }
    };

    struct TEvFindTabletSubDomainPathIdResult
        : public TEventPB<TEvFindTabletSubDomainPathIdResult,
                          NKikimrScheme::TEvFindTabletSubDomainPathIdResult,
                          EvFindTabletSubDomainPathIdResult>
    {
        using EStatus = NKikimrScheme::TEvFindTabletSubDomainPathIdResult::EStatus;

        TEvFindTabletSubDomainPathIdResult() = default;

        // Failure
        TEvFindTabletSubDomainPathIdResult(ui64 tabletId, EStatus status) {
            Record.SetStatus(status);
            Record.SetTabletId(tabletId);
        }

        // Success
        TEvFindTabletSubDomainPathIdResult(ui64 tabletId, ui64 schemeShardId, ui64 subDomainPathId) {
            Record.SetStatus(NKikimrScheme::TEvFindTabletSubDomainPathIdResult::SUCCESS);
            Record.SetTabletId(tabletId);
            Record.SetSchemeShardId(schemeShardId);
            Record.SetSubDomainPathId(subDomainPathId);
        }
    };

    struct TEvSubDomainPathIdFound : public TEventLocal<TEvSubDomainPathIdFound, EvSubDomainPathIdFound> {
            TEvSubDomainPathIdFound(ui64 schemeShardId, ui64 localPathId)
                : SchemeShardId(schemeShardId)
                , LocalPathId(localPathId)
            { }

            const ui64 SchemeShardId;
            const ui64 LocalPathId;
    };



    struct TEvLogin : TEventPB<TEvLogin, NKikimrScheme::TEvLogin, EvLogin> {
        TEvLogin() = default;
    };

    struct TEvLoginResult : TEventPB<TEvLoginResult, NKikimrScheme::TEvLoginResult, EvLoginResult> {
        TEvLoginResult() = default;
    };

    struct TEvOwnerActorAck : TEventPB<TEvOwnerActorAck, NKikimrScheme::TEvOwnerActorAck, EvOwnerActorAck> {
        TEvOwnerActorAck() = default;
    };
};

}

IActor* CreateFlatTxSchemeShard(const TActorId &tablet, TTabletStorageInfo *info);
bool PartitionConfigHasExternalBlobsEnabled(const NKikimrSchemeOp::TPartitionConfig &partitionConfig);
IActor* CreateFindSubDomainPathIdActor(const TActorId& parent, ui64 tabletId, ui64 schemeShardId, bool delayFirstRequest, TDuration maxFindSubDomainPathIdDelay = TDuration::Minutes(10));
}

template<>
inline void Out<NKikimrScheme::EStatus>(IOutputStream& o, NKikimrScheme::EStatus x) {
    o << NKikimrScheme::EStatus_Name(x);
    return;
}
