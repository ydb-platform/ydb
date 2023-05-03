#include "dst_creator.h"
#include "logging.h"
#include "private_events.h"
#include "util.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr::NReplication::NController {

using namespace NSchemeShard;

class TDstCreator: public TActorBootstrapped<TDstCreator> {
    void DescribeSrcPath() {
        switch (Kind) {
        case TReplication::ETargetKind::Table:
            Send(YdbProxy, new TEvYdbProxy::TEvDescribeTableRequest(SrcPath, {}));
            break;
        }

        Become(&TThis::StateDescribeSrcPath);
    }

    STATEFN(StateDescribeSrcPath) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvDescribeTableResponse, Handle);
            sFunc(TEvents::TEvWakeup, DescribeSrcPath);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvYdbProxy::TEvDescribeTableResponse::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        Y_VERIFY(Kind == TReplication::ETargetKind::Table);
        const auto& result = ev->Get()->Result;

        if (!result.IsSuccess()) {
            if (IsRetryableError(result)) {
                return Retry();
            }

            return Error(NKikimrScheme::StatusNotAvailable, TStringBuilder() << "Cannot describe table"
                << ": status: " << result.GetStatus()
                << ", issue: " << result.GetIssues().ToOneLineString());
        }

        Ydb::Table::CreateTableRequest scheme;
        result.GetTableDescription().SerializeTo(scheme);

        TTableProfiles profiles; // TODO: load
        Ydb::StatusIds::StatusCode status;
        TString error;
        if (!FillTableDescription(TxBody, scheme, profiles, status, error)) {
            return Error(NKikimrScheme::StatusSchemeError, error);
        }

        TxBody.MutableCreateTable()->SetName(ToString(ExtractBase(DstPath)));
        AllocateTxId();
    }

    void AllocateTxId() {
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
        Become(&TThis::StateAllocateTxId);
    }

    STATEFN(StateAllocateTxId) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        TxId = ev->Get()->TxId;
        PipeCache = ev->Get()->Services.LeaderPipeCache;
        CreateDst();
    }

    void CreateDst() {
        auto ev = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(TxId, SchemeShardId);
        *ev->Record.AddTransaction() = TxBody;

        Send(PipeCache, new TEvPipeCache::TEvForward(ev.Release(), SchemeShardId, true));
        Become(&TThis::StateCreateDst);
    }

    STATEFN(StateCreateDst) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            hFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            sFunc(TEvents::TEvWakeup, AllocateTxId);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());
        const auto& record = ev->Get()->Record;

        switch (record.GetStatus()) {
        case NKikimrScheme::StatusAccepted:
            DstPathId = TPathId(SchemeShardId, record.GetPathId());
            Y_VERIFY_DEBUG(TxId == record.GetTxId());
            return SubscribeTx(record.GetTxId());
        case NKikimrScheme::StatusMultipleModifications:
            if (record.HasPathCreateTxId()) {
                NeedToCheck = true;
                return SubscribeTx(record.GetPathCreateTxId());
            } else {
                return Error(record.GetStatus(), record.GetReason());
            }
            break;
        case NKikimrScheme::StatusAlreadyExists:
            return DescribeDstPath();
        default:
            return Error(record.GetStatus(), record.GetReason());
        }
    }

    void SubscribeTx(ui64 txId) {
        LOG_D("Subscribe tx"
            << ": txId# " << txId);
        Send(PipeCache, new TEvPipeCache::TEvForward(new TEvSchemeShard::TEvNotifyTxCompletion(txId), SchemeShardId));
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        if (NeedToCheck) {
            DescribeDstPath();
        } else {
            Success();
        }
    }

    void DescribeDstPath() {
        Send(PipeCache, new TEvPipeCache::TEvForward(new TEvSchemeShard::TEvDescribeScheme(DstPath), SchemeShardId));
        Become(&TThis::StateDescribeDstPath);
    }

    STATEFN(StateDescribeDstPath) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            sFunc(TEvents::TEvWakeup, DescribeDstPath);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());
        const auto& record = ev->Get()->GetRecord();

        switch (record.GetStatus()) {
            case NKikimrScheme::StatusSuccess: {
                TString error;
                if (!CheckScheme(record.GetPathDescription(), error)) {
                    return Error(NKikimrScheme::StatusSchemeError, error);
                } else {
                    DstPathId = TPathId(record.GetPathOwnerId(), record.GetPathId());
                    return Success();
                }
                break;
            }
            case NKikimrScheme::StatusPathDoesNotExist:
                return AllocateTxId();
            case NKikimrScheme::StatusSchemeError:
            case NKikimrScheme::StatusAccessDenied:
            case NKikimrScheme::StatusRedirectDomain:
            case NKikimrScheme::StatusNameConflict:
            case NKikimrScheme::StatusInvalidParameter:
            case NKikimrScheme::StatusPreconditionFailed:
                return Error(record.GetStatus(), record.GetReason());
            default:
                return Retry();
        }
    }

    bool CheckScheme(const NKikimrSchemeOp::TPathDescription& desc, TString& error) const {
        switch (Kind) {
        case TReplication::ETargetKind::Table:
            return CheckTableScheme(desc.GetTable(), error);
        }
    }

    bool CheckTableScheme(const NKikimrSchemeOp::TTableDescription& got, TString& error) const {
        const auto& expected = TxBody.GetCreateTable();

        // check key
        if (expected.KeyColumnNamesSize() != got.KeyColumnNamesSize()) {
            error = TStringBuilder() << "Key columns size mismatch"
                << ": expected: " << expected.KeyColumnNamesSize()
                << ", got: " << got.KeyColumnNamesSize();
            return false;
        }

        for (ui32 i = 0; i < expected.KeyColumnNamesSize(); ++i) {
            if (expected.GetKeyColumnNames(i) != got.GetKeyColumnNames(i)) {
                error = TStringBuilder() << "Key column name mismatch"
                    << ": position: " << i
                    << ", expected: " << expected.GetKeyColumnNames(i)
                    << ", got: " << got.GetKeyColumnNames(i);
                return false;
            }
        }

        // check columns
        THashMap<TStringBuf, TStringBuf> columns;
        for (const auto& column : got.GetColumns()) {
            columns.emplace(column.GetName(), column.GetType());
        }

        if (expected.ColumnsSize() != columns.size()) {
            error = TStringBuilder() << "Columns size mismatch"
                << ": expected: " << expected.ColumnsSize()
                << ", got: " << columns.size();
            return false;
        }

        for (const auto& column : expected.GetColumns()) {
            auto it = columns.find(column.GetName());
            if (it == columns.end()) {
                error = TStringBuilder() << "Cannot find column"
                    << ": name: " << column.GetName();
                return false;
            }

            if (column.GetType() != it->second) {
                error = TStringBuilder() << "Column type mismatch"
                    << ": name: " << column.GetName()
                    << ", expected: " << column.GetType()
                    << ", got: " << it->second;
                return false;
            }
        }

        // check indexes
        THashMap<TStringBuf, const NKikimrSchemeOp::TIndexDescription*> indexes;
        for (const auto& index : got.GetTableIndexes()) {
            indexes.emplace(index.GetName(), &index);
        }

        if (expected.TableIndexesSize() != indexes.size()) {
            error = TStringBuilder() << "Indexes size mismatch"
                << ": expected: " << expected.TableIndexesSize()
                << ", got: " << indexes.size();
            return false;
        }

        for (const auto& index : expected.GetTableIndexes()) {
            auto it = indexes.find(index.GetName());
            if (it == indexes.end()) {
                error = TStringBuilder() << "Cannot find index"
                    << ": name: " << index.GetName();
                return false;
            }

            if (index.GetType() != it->second->GetType()) {
                error = TStringBuilder() << "Index type mismatch"
                    << ": name: " << index.GetName()
                    << ", expected: " << NKikimrSchemeOp::EIndexType_Name(index.GetType())
                    << ", got: " << NKikimrSchemeOp::EIndexType_Name(it->second->GetType());
                return false;
            }

            if (index.KeyColumnNamesSize() != it->second->KeyColumnNamesSize()) {
                error = TStringBuilder() << "Index key columns size mismatch"
                    << ": name: " << index.GetName()
                    << ", expected: " << index.KeyColumnNamesSize()
                    << ", got: " << it->second->KeyColumnNamesSize();
                return false;
            }

            for (ui32 i = 0; i < index.KeyColumnNamesSize(); ++i) {
                if (index.GetKeyColumnNames(i) != it->second->GetKeyColumnNames(i)) {
                    error = TStringBuilder() << "Index key column name mismatch"
                        << ": name: " << index.GetName()
                        << ", position: " << i
                        << ", expected: " << index.GetKeyColumnNames(i)
                        << ", got: " << it->second->GetKeyColumnNames(i);
                    return false;
                }
            }

            if (index.DataColumnNamesSize() != it->second->DataColumnNamesSize()) {
                error = TStringBuilder() << "Index data columns size mismatch"
                    << ": name: " << index.GetName()
                    << ", expected: " << index.DataColumnNamesSize()
                    << ", got: " << it->second->DataColumnNamesSize();
                return false;
            }

            for (ui32 i = 0; i < index.DataColumnNamesSize(); ++i) {
                if (index.GetDataColumnNames(i) != it->second->GetDataColumnNames(i)) {
                    error = TStringBuilder() << "Index data column name mismatch"
                        << ": name: " << index.GetName()
                        << ", position: " << i
                        << ", expected: " << index.GetDataColumnNames(i)
                        << ", got: " << it->second->GetDataColumnNames(i);
                    return false;
                }
            }
        }

        return true;
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        if (SchemeShardId == ev->Get()->TabletId) {
            return;
        }

        Retry();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());
        Retry();
    }

    void Success() {
        Y_VERIFY(DstPathId);
        LOG_I("Success"
            << ": dstPathId# " << DstPathId);

        Send(Parent, new TEvPrivate::TEvCreateDstResult(ReplicationId, TargetId, DstPathId));
        PassAway();
    }

    void Error(NKikimrScheme::EStatus status, const TString& error) {
        LOG_E("Error"
            << ": status# " << status
            << ", reason# " << error);

        Send(Parent, new TEvPrivate::TEvCreateDstResult(ReplicationId, TargetId, status, error));
        PassAway();
    }

    void Retry() {
        LOG_D("Retry");
        Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_DST_CREATOR;
    }

    explicit TDstCreator(
            const TActorId& parent,
            ui64 schemeShardId,
            const TActorId& proxy,
            ui64 rid,
            ui64 tid,
            TReplication::ETargetKind kind,
            const TString& srcPath,
            const TString& dstPath)
        : Parent(parent)
        , SchemeShardId(schemeShardId)
        , YdbProxy(proxy)
        , ReplicationId(rid)
        , TargetId(tid)
        , Kind(kind)
        , SrcPath(srcPath)
        , DstPath(dstPath)
        , LogPrefix("DstCreator", ReplicationId, TargetId)
    {
        TxBody.SetWorkingDir(ToString(ExtractParent(DstPath)));
    }

    void Bootstrap() {
        DescribeSrcPath();
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const ui64 SchemeShardId;
    const TActorId YdbProxy;
    const ui64 ReplicationId;
    const ui64 TargetId;
    const TReplication::ETargetKind Kind;
    const TString SrcPath;
    const TString DstPath;
    const TActorLogPrefix LogPrefix;

    ui64 TxId = 0;
    NKikimrSchemeOp::TModifyScheme TxBody;
    TActorId PipeCache;
    bool NeedToCheck = false;
    TPathId DstPathId;

}; // TDstCreator

IActor* CreateDstCreator(const TActorId& parent, ui64 schemeShardId, const TActorId& proxy,
        ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TString& srcPath, const TString& dstPath)
{
    return new TDstCreator(parent, schemeShardId, proxy, rid, tid, kind, srcPath, dstPath);
}

}
