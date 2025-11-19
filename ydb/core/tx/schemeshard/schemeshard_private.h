#pragma once
#include "schemeshard_identificators.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/datetime/base.h>

namespace NKikimr {
namespace NSchemeShard {

namespace TEvPrivate {
    enum EEv {
        EvProgressOperation = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvOperationPlanStep,
        EvCommitTenantUpdate,
        EvUndoTenantUpdate,
        EvRunConditionalErase,
        EvIndexBuildBilling,
        EvImportSchemeReady,
        EvImportSchemeQueryResult,
        EvExportSchemeUploadResult,
        EvServerlessStorageBilling,
        EvCleanDroppedPaths,
        EvCleanDroppedSubDomains,
        EvSubscribeToShardDeletion,
        EvNotifyShardDeleted,
        EvRunBackgroundCompaction,
        EvRunBorrowedCompaction,
        EvCompletePublication,
        EvCompleteBarrier,
        EvPersistTableStats,
        EvConsoleConfigsTimeout,
        EvRunCdcStreamScan,
        EvPersistTopicStats,
        EvSendBaseStatsToSA,
        EvRunBackgroundCleaning,
        EvRetryNodeSubscribe,
        EvRunDataErasure,
        EvRunTenantDataErasure,
        EvAddNewShardToDataErasure,
        EvTestNotifySubdomainCleanup,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

    // This event is sent by a schemeshard to itself to signal that some tx state has changed
    // and it should run all the actions associated with this state
    struct TEvProgressOperation: public TEventLocal<TEvProgressOperation, EvProgressOperation> {
        const ui64 TxId;
        const ui32 TxPartId;

        TEvProgressOperation(ui64 txId, ui32 part)
            : TxId(txId)
            , TxPartId(part)
        {}
    };

    struct TEvOperationPlan: public TEventLocal<TEvOperationPlan, EvOperationPlanStep> {
        const ui64 StepId;
        const ui64 TxId;

        TEvOperationPlan(ui64 step, ui64 txId)
            : StepId(step)
            , TxId(txId)
        {}
    };

    struct TEvCommitTenantUpdate: public TEventLocal<TEvCommitTenantUpdate, EvCommitTenantUpdate> {
        TEvCommitTenantUpdate()
        {}
    };

    struct TEvUndoTenantUpdate: public TEventLocal<TEvUndoTenantUpdate, EvUndoTenantUpdate> {
        TEvUndoTenantUpdate()
        {}
    };

    struct TEvRunConditionalErase: public TEventLocal<TEvRunConditionalErase, EvRunConditionalErase> {
    };

    struct TEvIndexBuildingMakeABill: public TEventLocal<TEvIndexBuildingMakeABill, EvIndexBuildBilling> {
        const ui64 BuildId;
        const TInstant SendAt;

        TEvIndexBuildingMakeABill(ui64 id, TInstant sendAt)
            : BuildId(id)
            , SendAt(std::move(sendAt))
        {}
    };

    struct TEvImportSchemeReady: public TEventLocal<TEvImportSchemeReady, EvImportSchemeReady> {
        const ui64 ImportId;
        const ui32 ItemIdx;
        const bool Success;
        const TString Error;

        TEvImportSchemeReady(ui64 id, ui32 itemIdx, bool success, const TString& error)
            : ImportId(id)
            , ItemIdx(itemIdx)
            , Success(success)
            , Error(error)
        {}
    };

    struct TEvImportSchemeQueryResult: public TEventLocal<TEvImportSchemeQueryResult, EvImportSchemeQueryResult> {
        const ui64 ImportId;
        const ui32 ItemIdx;
        const Ydb::StatusIds::StatusCode Status;
        const std::variant<TString, NKikimrSchemeOp::TModifyScheme> Result;

        // failed query
        TEvImportSchemeQueryResult(ui64 id, ui32 itemIdx, Ydb::StatusIds::StatusCode status, TString&& error)
            : ImportId(id)
            , ItemIdx(itemIdx)
            , Status(status)
            , Result(error)
        {}

        // successful query
        TEvImportSchemeQueryResult(ui64 id, ui32 itemIdx, Ydb::StatusIds::StatusCode status, NKikimrSchemeOp::TModifyScheme&& preparedQuery)
            : ImportId(id)
            , ItemIdx(itemIdx)
            , Status(status)
            , Result(preparedQuery)
        {}
    };

    struct TEvExportSchemeUploadResult: public TEventLocal<TEvExportSchemeUploadResult, EvExportSchemeUploadResult> {
        const ui64 ExportId;
        const ui32 ItemIdx;
        const bool Success;
        const TString Error;

        TEvExportSchemeUploadResult(ui64 id, ui32 itemIdx, bool success, const TString& error)
            : ExportId(id)
            , ItemIdx(itemIdx)
            , Success(success)
            , Error(error)
        {}
    };

    struct TEvServerlessStorageBilling: public TEventLocal<TEvServerlessStorageBilling, EvServerlessStorageBilling> {
        TEvServerlessStorageBilling()
        {}
    };

    struct TEvCleanDroppedPaths : public TEventLocal<TEvCleanDroppedPaths, EvCleanDroppedPaths> {
        TEvCleanDroppedPaths() = default;
    };

    struct TEvCleanDroppedSubDomains : public TEventLocal<TEvCleanDroppedSubDomains, EvCleanDroppedSubDomains> {
        TEvCleanDroppedSubDomains() = default;
    };

    struct TEvSubscribeToShardDeletion : public TEventLocal<TEvSubscribeToShardDeletion, EvSubscribeToShardDeletion> {
        TShardIdx ShardIdx;

        explicit TEvSubscribeToShardDeletion(const TShardIdx& shardIdx)
            : ShardIdx(shardIdx)
        { }
    };

    struct TEvNotifyShardDeleted : public TEventLocal<TEvNotifyShardDeleted, EvNotifyShardDeleted> {
        TShardIdx ShardIdx;

        explicit TEvNotifyShardDeleted(const TShardIdx& shardIdx)
            : ShardIdx(shardIdx)
        { }
    };

    struct TEvTestNotifySubdomainCleanup : public TEventLocal<TEvTestNotifySubdomainCleanup, EvTestNotifySubdomainCleanup> {
        TPathId SubdomainPathId;

        explicit TEvTestNotifySubdomainCleanup(const TPathId& subdomainPathId)
            : SubdomainPathId(subdomainPathId)
        { }
    };

    struct TEvCompletePublication: public TEventLocal<TEvCompletePublication, EvCompletePublication> {
        const TOperationId OpId;
        const TPathId PathId;
        const ui64 Version;

        TEvCompletePublication(const TOperationId& opId, const TPathId& pathId, ui64 version)
            : OpId(opId)
            , PathId(pathId)
            , Version(version)
        {}

        TString ToString() const {
            return TStringBuilder() << ToStringHeader()
                                    << " {"
                                    << " OpId: " << OpId
                                    << " PathId: " << PathId
                                    << " Version: " << Version
                                    << " }";
        }
    };

    struct TEvCompleteBarrier: public TEventLocal<TEvCompleteBarrier, EvCompleteBarrier> {
        const TTxId TxId;
        const TString Name;

        TEvCompleteBarrier(const TTxId txId, const TString name)
            : TxId(txId)
            , Name(name)
        {}

        TString ToString() const {
            return TStringBuilder() << ToStringHeader()
                                    << " {"
                                    << " TxId: " << TxId
                                    << " Name: " << Name
                                    << " }";
        }
    };

    struct TEvPersistTableStats: public TEventLocal<TEvPersistTableStats, EvPersistTableStats> {
        TEvPersistTableStats() = default;
    };

    struct TEvPersistTopicStats: public TEventLocal<TEvPersistTopicStats, EvPersistTopicStats> {
        TEvPersistTopicStats() = default;
    };

    struct TEvConsoleConfigsTimeout: public TEventLocal<TEvConsoleConfigsTimeout, EvConsoleConfigsTimeout> {
    };

    struct TEvRunCdcStreamScan: public TEventLocal<TEvRunCdcStreamScan, EvRunCdcStreamScan> {
        const TPathId StreamPathId;

        TEvRunCdcStreamScan(const TPathId& streamPathId)
            : StreamPathId(streamPathId)
        {}
    };

    struct TEvSendBaseStatsToSA: public TEventLocal<TEvSendBaseStatsToSA, EvSendBaseStatsToSA> {
    };

    struct TEvRetryNodeSubscribe : public TEventLocal<TEvRetryNodeSubscribe, EvRetryNodeSubscribe> {
        ui32 NodeId;

        explicit TEvRetryNodeSubscribe(ui32 nodeId)
            : NodeId(nodeId)
        { }
    };

    struct TEvAddNewShardToDataErasure : public TEventLocal<TEvAddNewShardToDataErasure, EvAddNewShardToDataErasure> {
        const std::vector<TShardIdx> Shards;

        TEvAddNewShardToDataErasure(std::vector<TShardIdx>&& shards)
            : Shards(std::move(shards))
        {}
    };
}; // TEvPrivate

} // NSchemeShard
} // NKikimr
