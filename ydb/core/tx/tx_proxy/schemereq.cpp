#include "proxy.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/docapi/traits.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/protobuf_printer/security_printer.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>

#include <util/string/cast.h>

namespace NKikimr {
namespace NTxProxy {

template<typename TDerived>
struct TBaseSchemeReq: public TActorBootstrapped<TDerived> {
    using TBase = TActorBootstrapped<TDerived>;

    using TEvSchemeShardPropose = NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction;

    const TTxProxyServices Services;
    const ui64 TxId;
    THolder<TEvTxProxyReq::TEvSchemeRequest> SchemeRequest;
    TIntrusivePtr<TTxProxyMon> TxProxyMon;

    TInstant WallClockStarted;

    TActorId Source;
    TActorId PipeClient;

    struct TPathToResolve {
        NKikimrSchemeOp::EOperationType OperationRelated;

        TVector<TString> Path;
        bool RequiredRedirect = true;
        ui32 RequiredAccess = NACLib::EAccessRights::NoAccess;

        std::optional<NKikimrSchemeOp::TModifyACL> RequiredGrandAccess;

        TPathToResolve(NKikimrSchemeOp::EOperationType opType)
            : OperationRelated(opType)
        {
        }
    };

    TVector<TPathToResolve> ResolveForACL;

    std::optional<NACLib::TUserToken> UserToken;

    TBaseSchemeReq(const TTxProxyServices &services, ui64 txid, TAutoPtr<TEvTxProxyReq::TEvSchemeRequest> request, const TIntrusivePtr<TTxProxyMon> &txProxyMon)
        : Services(services)
        , TxId(txid)
        , SchemeRequest(request)
        , TxProxyMon(txProxyMon)
        , Source(SchemeRequest->Ev->Sender)
    {
        ++*TxProxyMon->SchemeReqInFly;
    }

    auto& GetRequestEv() { return *SchemeRequest->Ev->Get(); }

    auto& GetRequestProto() { return GetRequestEv().Record; }

    auto& GetModifyScheme() { return *GetRequestProto().MutableTransaction()->MutableModifyScheme(); }

    auto& GetModifications() { return *GetRequestProto().MutableTransaction()->MutableTransactionalModification(); }


    void SendPropose(TAutoPtr<TEvSchemeShardPropose> req, ui64 shardToRequest, const TActorContext &ctx) {
        Y_ABORT_UNLESS(!PipeClient);

        if (UserToken) {
            req->Record.SetUserToken(UserToken->SerializeAsString());
        }

        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {.RetryLimitCount = 3};
        PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, shardToRequest, clientConfig));
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " SEND to# " << shardToRequest << " shardToRequest " << req->ToString());
        NTabletPipe::SendData(ctx, PipeClient, req.Release());
    }

    static bool IsSplitMergeFromSchemeShard(const NKikimrSchemeOp::TModifyScheme& modifyScheme) {
        return modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions
                && modifyScheme.GetSplitMergeTablePartitions().HasTableLocalId();
    }

    void SendSplitMergePropose(const TActorContext &ctx) {
        ui64 shardToRequest =  GetModifyScheme().GetSplitMergeTablePartitions().GetSchemeshardId();

        // TODO: Need to check superuser permissions

        auto req = MakeHolder<TEvSchemeShardPropose>(TxId, shardToRequest);
        req->Record.AddTransaction()->MergeFrom(GetModifyScheme());

        SendPropose(req.Release(), shardToRequest, ctx);
    }

    void ExtractUserToken() {
        if (!GetRequestProto().GetUserToken().empty()) {
            UserToken = NACLib::TUserToken(GetRequestProto().GetUserToken());
        }
    }

    static TVector<TString> Merge(const TVector<TString>& l, TVector<TString>&& r) {
        TVector<TString> result = l;
        std::move(r.begin(), r.end(), std::back_inserter(result));
        return result;
    }

    static TString& GetPathNameForScheme(NKikimrSchemeOp::TModifyScheme& modifyScheme) {
        switch (modifyScheme.GetOperationType()) {
        case NKikimrSchemeOp::ESchemeOpMkDir:
            return *modifyScheme.MutableMkDir()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreateTable:
            return *modifyScheme.MutableCreateTable()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup:
            return *modifyScheme.MutableCreatePersQueueGroup()->MutableName();

        case NKikimrSchemeOp::ESchemeOpDropTable:
        case NKikimrSchemeOp::ESchemeOpDropPersQueueGroup:
        case NKikimrSchemeOp::ESchemeOpRmDir:
        case NKikimrSchemeOp::ESchemeOpDropSubDomain:
        case NKikimrSchemeOp::ESchemeOpDropBlockStoreVolume:
        case NKikimrSchemeOp::ESchemeOpDropKesus:
        case NKikimrSchemeOp::ESchemeOpForceDropSubDomain:
        case NKikimrSchemeOp::ESchemeOpDropSolomonVolume:
        case NKikimrSchemeOp::ESchemeOpForceDropUnsafe:
        case NKikimrSchemeOp::ESchemeOpDropTableIndex:
        case NKikimrSchemeOp::ESchemeOpForceDropExtSubDomain:
        case NKikimrSchemeOp::ESchemeOpDropFileStore:
        case NKikimrSchemeOp::ESchemeOpDropColumnStore:
        case NKikimrSchemeOp::ESchemeOpDropColumnTable:
        case NKikimrSchemeOp::ESchemeOpDropSequence:
        case NKikimrSchemeOp::ESchemeOpDropReplication:
        case NKikimrSchemeOp::ESchemeOpDropReplicationCascade:
        case NKikimrSchemeOp::ESchemeOpDropBlobDepot:
        case NKikimrSchemeOp::ESchemeOpDropExternalTable:
        case NKikimrSchemeOp::ESchemeOpDropExternalDataSource:
        case NKikimrSchemeOp::ESchemeOpDropView:
        case NKikimrSchemeOp::ESchemeOpDropResourcePool:
            return *modifyScheme.MutableDrop()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterTable:
            return *modifyScheme.MutableAlterTable()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup:
            return *modifyScheme.MutableAlterPersQueueGroup()->MutableName();

        case NKikimrSchemeOp::ESchemeOpModifyACL:
            return *modifyScheme.MutableModifyACL()->MutableName();

        case NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions:
            Y_ABORT("no implementation for ESchemeOpSplitMergeTablePartitions");

        case NKikimrSchemeOp::ESchemeOpBackup:
            return *modifyScheme.MutableBackup()->MutableTableName();

        case NKikimrSchemeOp::ESchemeOpCreateSubDomain:
        case NKikimrSchemeOp::ESchemeOpAlterSubDomain:
        case NKikimrSchemeOp::ESchemeOpCreateExtSubDomain:
        case NKikimrSchemeOp::ESchemeOpAlterExtSubDomain:
            return *modifyScheme.MutableSubDomain()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterExtSubDomainCreateHive:
            Y_ABORT("no implementation for ESchemeOpAlterExtSubDomainCreateHive");

        case NKikimrSchemeOp::ESchemeOpCreateRtmrVolume:
            return *modifyScheme.MutableCreateRtmrVolume()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume:
            return *modifyScheme.MutableCreateBlockStoreVolume()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterBlockStoreVolume:
            return *modifyScheme.MutableAlterBlockStoreVolume()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAssignBlockStoreVolume:
            return *modifyScheme.MutableAssignBlockStoreVolume()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreateKesus:
        case NKikimrSchemeOp::ESchemeOpAlterKesus:
            return *modifyScheme.MutableKesus()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreateSolomonVolume:
            return *modifyScheme.MutableCreateSolomonVolume()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterUserAttributes:
            return *modifyScheme.MutableAlterUserAttributes()->MutablePathName();

        case NKikimrSchemeOp::ESchemeOpCreateIndexedTable:
            return *modifyScheme.MutableCreateIndexedTable()->MutableTableDescription()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreateTableIndex:
            return *modifyScheme.MutableCreateTableIndex()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables:
            Y_ABORT("no implementation for ESchemeOpCreateConsistentCopyTables");

        case NKikimrSchemeOp::ESchemeOp_DEPRECATED_35:
            Y_ABORT("no implementation for ESchemeOp_DEPRECATED_35");

        case NKikimrSchemeOp::ESchemeOpUpgradeSubDomain:
        case NKikimrSchemeOp::ESchemeOpUpgradeSubDomainDecision:
            return *modifyScheme.MutableUpgradeSubDomain()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreateColumnBuild:
        case NKikimrSchemeOp::ESchemeOpCreateIndexBuild:
            Y_ABORT("no implementation for ESchemeOpCreateIndexBuild/ESchemeOpCreateColumnBuild");

        case NKikimrSchemeOp::ESchemeOpInitiateBuildIndexMainTable:
            Y_ABORT("no implementation for ESchemeOpInitiateBuildIndexMainTable");

        case NKikimrSchemeOp::ESchemeOpCreateLock:
            Y_ABORT("no implementation for ESchemeOpCreateLock");

        case NKikimrSchemeOp::ESchemeOpApplyIndexBuild:
            Y_ABORT("no implementation for ESchemeOpApplyIndexBuild");

        case NKikimrSchemeOp::ESchemeOpFinalizeBuildIndexMainTable:
            Y_ABORT("no implementation for ESchemeOpFinalizeBuildIndexMainTable");

        case NKikimrSchemeOp::ESchemeOpAlterTableIndex:
            return *modifyScheme.MutableAlterTableIndex()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterSolomonVolume:
            return *modifyScheme.MutableAlterSolomonVolume()->MutableName();

        case NKikimrSchemeOp::ESchemeOpDropLock:
            Y_ABORT("no implementation for ESchemeOpDropLock");

        case NKikimrSchemeOp::ESchemeOpFinalizeBuildIndexImplTable:
            Y_ABORT("no implementation for ESchemeOpFinalizeBuildIndexImplTable");

        case NKikimrSchemeOp::ESchemeOpInitiateBuildIndexImplTable:
            Y_ABORT("no implementation for ESchemeOpInitiateBuildIndexImplTable");

        case NKikimrSchemeOp::ESchemeOpDropIndex:
            return *modifyScheme.MutableDropIndex()->MutableTableName();

        case NKikimrSchemeOp::ESchemeOpDropTableIndexAtMainTable:
            Y_ABORT("no implementation for ESchemeOpDropTableIndexAtMainTable");

        case NKikimrSchemeOp::ESchemeOpCancelIndexBuild:
            return *modifyScheme.MutableCancelIndexBuild()->MutableTablePath();

        case NKikimrSchemeOp::ESchemeOpCreateFileStore:
            return *modifyScheme.MutableCreateFileStore()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterFileStore:
            return *modifyScheme.MutableAlterFileStore()->MutableName();

        case NKikimrSchemeOp::ESchemeOpRestore:
            return *modifyScheme.MutableRestore()->MutableTableName();

        case NKikimrSchemeOp::ESchemeOpCreateColumnStore:
            return *modifyScheme.MutableCreateColumnStore()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterColumnStore:
            return *modifyScheme.MutableAlterColumnStore()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreateColumnTable:
            return *modifyScheme.MutableCreateColumnTable()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterColumnTable:
            return *modifyScheme.MutableAlterColumnTable()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterLogin:
            Y_ABORT("no implementation for ESchemeOpAlterLogin");

        case NKikimrSchemeOp::ESchemeOpCreateCdcStream:
            return *modifyScheme.MutableCreateCdcStream()->MutableTableName();

        case NKikimrSchemeOp::ESchemeOpCreateCdcStreamImpl:
            Y_ABORT("no implementation for ESchemeOpCreateCdcStreamImpl");

        case NKikimrSchemeOp::ESchemeOpCreateCdcStreamAtTable:
            return *modifyScheme.MutableCreateCdcStream()->MutableTableName();

        case NKikimrSchemeOp::ESchemeOpAlterCdcStream:
            return *modifyScheme.MutableAlterCdcStream()->MutableTableName();

        case NKikimrSchemeOp::ESchemeOpAlterCdcStreamImpl:
            Y_ABORT("no implementation for ESchemeOpAlterCdcStreamImpl");

        case NKikimrSchemeOp::ESchemeOpAlterCdcStreamAtTable:
            return *modifyScheme.MutableAlterCdcStream()->MutableTableName();

        case NKikimrSchemeOp::ESchemeOpDropCdcStream:
            return *modifyScheme.MutableDropCdcStream()->MutableTableName();

        case NKikimrSchemeOp::ESchemeOpDropCdcStreamImpl:
            Y_ABORT("no implementation for ESchemeOpDropCdcStreamImpl");

        case NKikimrSchemeOp::ESchemeOpDropCdcStreamAtTable:
            return *modifyScheme.MutableDropCdcStream()->MutableTableName();

        case NKikimrSchemeOp::ESchemeOpMoveTable:
            Y_ABORT("no implementation for ESchemeOpMoveTable");

        case NKikimrSchemeOp::ESchemeOpMoveTableIndex:
            Y_ABORT("no implementation for ESchemeOpMoveTableIndex");

        case NKikimrSchemeOp::ESchemeOpMoveIndex:
            Y_ABORT("no implementation for ESchemeOpMoveIndex");

        case NKikimrSchemeOp::ESchemeOpCreateSequence:
        case NKikimrSchemeOp::ESchemeOpAlterSequence:
            return *modifyScheme.MutableSequence()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreateReplication:
        case NKikimrSchemeOp::ESchemeOpAlterReplication:
            return *modifyScheme.MutableReplication()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreateBlobDepot:
        case NKikimrSchemeOp::ESchemeOpAlterBlobDepot:
            return *modifyScheme.MutableBlobDepot()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreateExternalTable:
            return *modifyScheme.MutableCreateExternalTable()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterExternalTable:
            Y_ABORT("no implementation for ESchemeOpAlterExternalTable");

        case NKikimrSchemeOp::ESchemeOpCreateExternalDataSource:
            return *modifyScheme.MutableCreateExternalDataSource()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterExternalDataSource:
            Y_ABORT("no implementation for ESchemeOpAlterExternalDataSource");

        case NKikimrSchemeOp::ESchemeOpCreateView:
            return *modifyScheme.MutableCreateView()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterView:
            Y_ABORT("no implementation for ESchemeOpAlterView");

        case NKikimrSchemeOp::ESchemeOpCreateContinuousBackup:
            return *modifyScheme.MutableCreateContinuousBackup()->MutableTableName();

        case NKikimrSchemeOp::ESchemeOpAlterContinuousBackup:
            return *modifyScheme.MutableAlterContinuousBackup()->MutableTableName();

        case NKikimrSchemeOp::ESchemeOpDropContinuousBackup:
            return *modifyScheme.MutableDropContinuousBackup()->MutableTableName();

        case NKikimrSchemeOp::ESchemeOpCreateResourcePool:
            return *modifyScheme.MutableCreateResourcePool()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterResourcePool:
            return *modifyScheme.MutableCreateResourcePool()->MutableName();

        case NKikimrSchemeOp::ESchemeOpRestoreIncrementalBackup:
            return *modifyScheme.MutableRestoreIncrementalBackup()->MutableSrcTableName();
        }
    }

    static bool IsCreateRequest(const NKikimrSchemeOp::TModifyScheme& modifyScheme) {
        switch (modifyScheme.GetOperationType()) {
        // Tenants are always created using cluster's root as working dir, skip it
        //case NKikimrSchemeOp::ESchemeOpCreateSubDomain:
        //case NKikimrSchemeOp::ESchemeOpCreateExtSubDomain:
        case NKikimrSchemeOp::ESchemeOpMkDir:
        case NKikimrSchemeOp::ESchemeOpCreateTable:
        case NKikimrSchemeOp::ESchemeOpCreateIndexedTable:
        case NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup:
        case NKikimrSchemeOp::ESchemeOpCreateKesus:
        case NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume:
        case NKikimrSchemeOp::ESchemeOpCreateFileStore:
        case NKikimrSchemeOp::ESchemeOpCreateSolomonVolume:
        case NKikimrSchemeOp::ESchemeOpCreateRtmrVolume:
        case NKikimrSchemeOp::ESchemeOpCreateColumnStore:
        case NKikimrSchemeOp::ESchemeOpCreateColumnTable:
        case NKikimrSchemeOp::ESchemeOpCreateExternalTable:
        case NKikimrSchemeOp::ESchemeOpCreateExternalDataSource:
        case NKikimrSchemeOp::ESchemeOpCreateView:
        case NKikimrSchemeOp::ESchemeOpCreateResourcePool:
            return true;
        default:
            return false;
        }
    }

    static bool NeedAdjustPathNames(const NKikimrSchemeOp::TModifyScheme& modifyScheme) {
        return IsCreateRequest(modifyScheme);
    }

    static THolder<NSchemeCache::TSchemeCacheNavigate> ResolveRequestForAdjustPathNames(NKikimrSchemeOp::TModifyScheme& scheme) {
        auto parts = Merge(SplitPath(scheme.GetWorkingDir()), SplitPath(GetPathNameForScheme(scheme)));
        if (parts.size() < 2) {
            return {};
        }

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        TVector<TString> path;

        for (auto it = parts.begin(); it != parts.end() - 1; ++it) {
            path.emplace_back(*it);

            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Path = path;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
            entry.SyncVersion = true;
            entry.RedirectRequired = false;
            request->ResultSet.emplace_back(entry);
        }

        return std::move(request);
    }

    void ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status,
        const NKikimrScheme::TEvModifySchemeTransactionResult* shardResult,
        const NYql::TIssue* issue,
        const TActorContext& ctx)
    {
        auto *result = new TEvTxUserProxy::TEvProposeTransactionStatus(status);
        if (issue) {
            NYql::IssueToMessage(*issue, result->Record.AddIssues());
        }
        if (shardResult) {
            if (shardResult->HasTxId())
                result->Record.SetTxId(shardResult->GetTxId());

            if (shardResult->HasSchemeshardId())
                result->Record.SetSchemeShardTabletId(shardResult->GetSchemeshardId());

            result->Record.SetSchemeShardStatus(shardResult->GetStatus());

            if (shardResult->HasReason() && !shardResult->GetReason().empty())
                result->Record.SetSchemeShardReason(shardResult->GetReason());

            if (shardResult->HasPathId()) {
                result->Record.SetPathId(shardResult->GetPathId());
            }

            if (shardResult->HasPathCreateTxId()) {
                result->Record.SetPathCreateTxId(shardResult->GetPathCreateTxId());
            }

            if (shardResult->HasPathDropTxId()) {
                result->Record.SetPathDropTxId(shardResult->GetPathDropTxId());
            }

            for (const auto& issue : shardResult->GetIssues()) {
                auto newIssue = result->Record.AddIssues();
                newIssue->CopyFrom(issue);
            }
        } else {
            switch (status) {
                case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError:
                    // (xenoxeno) for compatibility with KQP and maybe others...
                    result->Record.SetSchemeShardStatus(NKikimrScheme::EStatus::StatusPathDoesNotExist);
                    result->Record.SetSchemeShardReason("Path does not exist");
                    break;
                case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable:
                    result->Record.SetSchemeShardStatus(NKikimrScheme::EStatus::StatusNotAvailable);
                    result->Record.SetSchemeShardReason("Schemeshard not available");
                    break;
                default:
                    break;
            }
        }
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " SEND to# " << Source.ToString() << " Source " << result->ToString());

        if (result->Record.GetSchemeShardReason()) {
            auto issueStatus = NKikimrIssues::TIssuesIds::DEFAULT_ERROR;
            if (result->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusPathDoesNotExist) {
                issueStatus = NKikimrIssues::TIssuesIds::PATH_NOT_EXIST;
            }
            auto issue = MakeIssue(std::move(issueStatus), result->Record.GetSchemeShardReason());
            NYql::IssueToMessage(issue, result->Record.AddIssues());
        }
        ctx.Send(Source, result);
    }

    void ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status, const TActorContext &ctx)
    {
        ReportStatus(status, nullptr, nullptr, ctx);
    }

    void Bootstrap(const TActorContext&) {
        ExtractUserToken();
    }

    void Die(const TActorContext &ctx) override {
        --*TxProxyMon->SchemeReqInFly;

        if (PipeClient) {
            NTabletPipe::CloseClient(ctx, PipeClient);
            PipeClient = TActorId();
        }

        TBase::Die(ctx);
    }

    // KIKIMR-12624 move that logic to the schemeshard
    bool CheckTablePrereqs(const NKikimrSchemeOp::TTableDescription &desc, const TString &path, const TActorContext& ctx) {
        // check ad-hoc prereqs for table alter/creation

        // 1. split and external blobs must not be used simultaneously
        if (desc.HasPartitionConfig()) {
            const auto &partition = desc.GetPartitionConfig();
            if (partition.HasPartitioningPolicy() && partition.GetPartitioningPolicy().GetSizeToSplit() > 0) {
                if (PartitionConfigHasExternalBlobsEnabled(partition)) {
                    LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY, "Actor#" << ctx.SelfID.ToString() << " txid# " << TxId << " must not use auto-split and external blobs simultaneously, path# " << path);
                    return false;
                }
            }
        }

        return true;
    }

    bool ExamineTables(NKikimrSchemeOp::TModifyScheme& pbModifyScheme, const TActorContext& ctx) {
        switch (pbModifyScheme.GetOperationType()) {
            case NKikimrSchemeOp::ESchemeOpAlterTable: {
                auto path = JoinPath({pbModifyScheme.GetWorkingDir(), GetPathNameForScheme(pbModifyScheme)});
                if (!CheckTablePrereqs(pbModifyScheme.GetAlterTable(), path, ctx)) {
                    return false;
                }
                break;
            }
            case NKikimrSchemeOp::ESchemeOpCreateTable: {
                auto path = JoinPath({pbModifyScheme.GetWorkingDir(), GetPathNameForScheme(pbModifyScheme)});
                if (!CheckTablePrereqs(pbModifyScheme.GetCreateTable(), path, ctx)) {
                    return false;
                }
                break;
            }
            case NKikimrSchemeOp::ESchemeOpCreateIndexedTable: {
                auto path = JoinPath({pbModifyScheme.GetWorkingDir(), GetPathNameForScheme(pbModifyScheme)});
                if (!CheckTablePrereqs(pbModifyScheme.GetCreateIndexedTable().GetTableDescription(), path, ctx)) {
                    return false;
                }
                break;
            }
            default:
                break;
        }

        return true;
    }

    bool ExtractResolveForACL(NKikimrSchemeOp::TModifyScheme& pbModifyScheme) {
        ui32 accessToUserAttrs = pbModifyScheme.HasAlterUserAttributes() ? NACLib::EAccessRights::WriteUserAttributes : NACLib::EAccessRights::NoAccess;
        auto workingDir = SplitPath(pbModifyScheme.GetWorkingDir());

        switch (pbModifyScheme.GetOperationType()) {
        case NKikimrSchemeOp::ESchemeOpUpgradeSubDomain:
        case NKikimrSchemeOp::ESchemeOpUpgradeSubDomainDecision:
        case NKikimrSchemeOp::ESchemeOpAlterSubDomain:
        case NKikimrSchemeOp::ESchemeOpAlterExtSubDomain:
        {
            auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequiredAccess = NACLib::EAccessRights::CreateDatabase | NACLib::EAccessRights::AlterSchema | accessToUserAttrs;
            toResolve.RequiredRedirect = false;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpCreateSubDomain:
        case NKikimrSchemeOp::ESchemeOpCreateExtSubDomain:
        {
            auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
            toResolve.Path = workingDir;
            toResolve.RequiredAccess = NACLib::EAccessRights::CreateDatabase | accessToUserAttrs;
            toResolve.RequiredRedirect = false;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpAlterUserAttributes:
        {
            auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequiredAccess = NACLib::EAccessRights::WriteUserAttributes | accessToUserAttrs;
            toResolve.RequiredRedirect = false;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions: {
            auto& path = pbModifyScheme.GetSplitMergeTablePartitions().GetTablePath();
            TString baseDir = ToString(ExtractParent(path)); // why baseDir?

            auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
            toResolve.Path = SplitPath(baseDir);
            toResolve.RequiredAccess = NACLib::EAccessRights::NoAccess; // why not?
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpAlterTable:
        case NKikimrSchemeOp::ESchemeOpDropIndex:
        case NKikimrSchemeOp::ESchemeOpCreateCdcStream:
        case NKikimrSchemeOp::ESchemeOpAlterCdcStream:
        case NKikimrSchemeOp::ESchemeOpDropCdcStream:
        case NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup:
        case NKikimrSchemeOp::ESchemeOpAlterBlockStoreVolume:
        case NKikimrSchemeOp::ESchemeOpAssignBlockStoreVolume:
        case NKikimrSchemeOp::ESchemeOpAlterFileStore:
        case NKikimrSchemeOp::ESchemeOpAlterKesus:
        case NKikimrSchemeOp::ESchemeOpBackup:
        case NKikimrSchemeOp::ESchemeOpAlterSolomonVolume:
        case NKikimrSchemeOp::ESchemeOpAlterColumnStore:
        case NKikimrSchemeOp::ESchemeOpAlterColumnTable:
        case NKikimrSchemeOp::ESchemeOpAlterSequence:
        case NKikimrSchemeOp::ESchemeOpAlterReplication:
        case NKikimrSchemeOp::ESchemeOpAlterBlobDepot:
        case NKikimrSchemeOp::ESchemeOpAlterExternalTable:
        case NKikimrSchemeOp::ESchemeOpAlterExternalDataSource:
        case NKikimrSchemeOp::ESchemeOpCreateContinuousBackup:
        case NKikimrSchemeOp::ESchemeOpAlterContinuousBackup:
        case NKikimrSchemeOp::ESchemeOpDropContinuousBackup:
        case NKikimrSchemeOp::ESchemeOpAlterResourcePool:
        case NKikimrSchemeOp::ESchemeOpRestoreIncrementalBackup:
        {
            auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequiredAccess = NACLib::EAccessRights::AlterSchema | accessToUserAttrs;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpRmDir:
        case NKikimrSchemeOp::ESchemeOpDropBlockStoreVolume:
        case NKikimrSchemeOp::ESchemeOpDropFileStore:
        case NKikimrSchemeOp::ESchemeOpDropKesus:
        case NKikimrSchemeOp::ESchemeOpDropPersQueueGroup:
        case NKikimrSchemeOp::ESchemeOpDropTable:
        case NKikimrSchemeOp::ESchemeOpDropSolomonVolume:
        case NKikimrSchemeOp::ESchemeOpDropColumnStore:
        case NKikimrSchemeOp::ESchemeOpDropColumnTable:
        case NKikimrSchemeOp::ESchemeOpDropSequence:
        case NKikimrSchemeOp::ESchemeOpDropReplication:
        case NKikimrSchemeOp::ESchemeOpDropReplicationCascade:
        case NKikimrSchemeOp::ESchemeOpDropBlobDepot:
        case NKikimrSchemeOp::ESchemeOpDropExternalTable:
        case NKikimrSchemeOp::ESchemeOpDropExternalDataSource:
        case NKikimrSchemeOp::ESchemeOpDropView:
        case NKikimrSchemeOp::ESchemeOpDropResourcePool:
        {
            auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequiredAccess = NACLib::EAccessRights::RemoveSchema;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpDropSubDomain:
        case NKikimrSchemeOp::ESchemeOpForceDropSubDomain:
        case NKikimrSchemeOp::ESchemeOpForceDropExtSubDomain: {
            auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequiredAccess = NACLib::EAccessRights::DropDatabase;
            toResolve.RequiredRedirect = false;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpForceDropUnsafe: {
            auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequiredAccess = NACLib::EAccessRights::DropDatabase | NACLib::EAccessRights::RemoveSchema;
            toResolve.RequiredRedirect = false;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpModifyACL: {
            auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequiredAccess = NACLib::EAccessRights::GrantAccessRights | accessToUserAttrs;
            toResolve.RequiredGrandAccess = pbModifyScheme.GetModifyACL();
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpCreateTable: {
            auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
            toResolve.Path = workingDir;
            toResolve.RequiredAccess = NACLib::EAccessRights::CreateTable | accessToUserAttrs;
            ResolveForACL.push_back(toResolve);

            if (pbModifyScheme.GetCreateTable().HasCopyFromTable()) {
                auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
                toResolve.Path = SplitPath(pbModifyScheme.GetCreateTable().GetCopyFromTable());
                toResolve.RequiredAccess = NACLib::EAccessRights::SelectRow;
                ResolveForACL.push_back(toResolve);
            }
            break;
        }
        case NKikimrSchemeOp::ESchemeOpCreateIndexedTable:
        case NKikimrSchemeOp::ESchemeOpCreateColumnStore:
        case NKikimrSchemeOp::ESchemeOpCreateColumnTable:
        case NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume:
        case NKikimrSchemeOp::ESchemeOpCreateFileStore:
        case NKikimrSchemeOp::ESchemeOpCreateRtmrVolume:
        case NKikimrSchemeOp::ESchemeOpCreateKesus:
        case NKikimrSchemeOp::ESchemeOpCreateSolomonVolume:
        case NKikimrSchemeOp::ESchemeOpCreateSequence:
        case NKikimrSchemeOp::ESchemeOpCreateReplication:
        case NKikimrSchemeOp::ESchemeOpCreateBlobDepot:
        case NKikimrSchemeOp::ESchemeOpCreateExternalTable:
        case NKikimrSchemeOp::ESchemeOpCreateExternalDataSource:
        case NKikimrSchemeOp::ESchemeOpCreateView:
        case NKikimrSchemeOp::ESchemeOpCreateResourcePool:
        {
            auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
            toResolve.Path = workingDir;
            toResolve.RequiredAccess = NACLib::EAccessRights::CreateTable | accessToUserAttrs;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables: {
            for (auto& item: pbModifyScheme.GetCreateConsistentCopyTables().GetCopyTableDescriptions()) {
                {
                    auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
                    toResolve.Path = SplitPath(item.GetSrcPath());
                    toResolve.RequiredAccess = NACLib::EAccessRights::SelectRow;
                    ResolveForACL.push_back(toResolve);
                }
                {
                    auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
                    auto dstDir = ToString(ExtractParent(item.GetDstPath()));
                    toResolve.Path = SplitPath(dstDir);
                    toResolve.RequiredAccess = NACLib::EAccessRights::CreateTable;
                    ResolveForACL.push_back(toResolve);
                }
            }
            break;
        }
        case NKikimrSchemeOp::ESchemeOpMoveTable: {
            auto& descr = pbModifyScheme.GetMoveTable();
            {
                auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
                toResolve.Path = SplitPath(descr.GetSrcPath());
                toResolve.RequiredAccess = NACLib::EAccessRights::SelectRow | NACLib::EAccessRights::RemoveSchema;
                ResolveForACL.push_back(toResolve);
            }
            {
                auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
                auto dstDir = ToString(ExtractParent(descr.GetDstPath()));
                toResolve.Path = SplitPath(dstDir);
                toResolve.RequiredAccess = NACLib::EAccessRights::CreateTable;
                ResolveForACL.push_back(toResolve);
            }
            break;
        }
        case NKikimrSchemeOp::ESchemeOpMoveIndex: {
            auto& descr = pbModifyScheme.GetMoveIndex();
            {
                auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
                toResolve.Path = SplitPath(descr.GetTablePath());
                toResolve.RequiredAccess = NACLib::EAccessRights::AlterSchema;
                ResolveForACL.push_back(toResolve);
            }
            break;
        }
        case NKikimrSchemeOp::ESchemeOpMkDir:
        {
            auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
            toResolve.Path = workingDir;
            toResolve.RequiredAccess = NACLib::EAccessRights::CreateDirectory | accessToUserAttrs;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup:
        {
            auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
            toResolve.Path = workingDir;
            toResolve.RequiredAccess = NACLib::EAccessRights::CreateQueue | accessToUserAttrs;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpAlterLogin:
        {
            auto toResolve = TPathToResolve(pbModifyScheme.GetOperationType());
            toResolve.Path = workingDir;
            toResolve.RequiredAccess = NACLib::EAccessRights::AlterSchema | accessToUserAttrs;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpCreateTableIndex:
        case NKikimrSchemeOp::ESchemeOpDropTableIndex:
        case NKikimrSchemeOp::ESchemeOp_DEPRECATED_35:
        case NKikimrSchemeOp::ESchemeOpCreateColumnBuild:
        case NKikimrSchemeOp::ESchemeOpCreateIndexBuild:
        case NKikimrSchemeOp::ESchemeOpInitiateBuildIndexMainTable:
        case NKikimrSchemeOp::ESchemeOpCreateLock:
        case NKikimrSchemeOp::ESchemeOpApplyIndexBuild:
        case NKikimrSchemeOp::ESchemeOpFinalizeBuildIndexMainTable:
        case NKikimrSchemeOp::ESchemeOpAlterTableIndex:
        case NKikimrSchemeOp::ESchemeOpDropLock:
        case NKikimrSchemeOp::ESchemeOpFinalizeBuildIndexImplTable:
        case NKikimrSchemeOp::ESchemeOpInitiateBuildIndexImplTable:
        case NKikimrSchemeOp::ESchemeOpDropTableIndexAtMainTable:
        case NKikimrSchemeOp::ESchemeOpCancelIndexBuild:
        case NKikimrSchemeOp::ESchemeOpRestore:
        case NKikimrSchemeOp::ESchemeOpCreateCdcStreamImpl:
        case NKikimrSchemeOp::ESchemeOpCreateCdcStreamAtTable:
        case NKikimrSchemeOp::ESchemeOpAlterCdcStreamImpl:
        case NKikimrSchemeOp::ESchemeOpAlterCdcStreamAtTable:
        case NKikimrSchemeOp::ESchemeOpDropCdcStreamImpl:
        case NKikimrSchemeOp::ESchemeOpDropCdcStreamAtTable:
        case NKikimrSchemeOp::ESchemeOpMoveTableIndex:
        case NKikimrSchemeOp::ESchemeOpAlterExtSubDomainCreateHive:
        case NKikimrSchemeOp::ESchemeOpAlterView:
            return false;
        }
        return true;
    }

    THolder<NSchemeCache::TSchemeCacheNavigate> ResolveRequestForACL() {
        if (!ResolveForACL) {
            return {};
        }

        for(auto& toReq: ResolveForACL) {
            if (toReq.Path.empty()) {
                return {};
            }
        }

        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
        request->DatabaseName = GetRequestProto().GetDatabaseName();

        for(auto& toReq: ResolveForACL) {
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            entry.Path = toReq.Path;
            entry.RedirectRequired = toReq.RequiredRedirect;
            entry.SyncVersion = true;
            entry.ShowPrivatePath = true;

            request->ResultSet.emplace_back(std::move(entry));
        }

        return std::move(request);
    }

    ui64 GetShardToRequest(NSchemeCache::TSchemeCacheNavigate::TEntry& resolveResult, const TPathToResolve& resolveTask) {
        if (!resolveResult.DomainInfo->Params.HasSchemeShard()) {
            return resolveResult.DomainInfo->DomainKey.OwnerId;
        }

        if (resolveTask.OperationRelated == NKikimrSchemeOp::ESchemeOpAlterUserAttributes) {
            // ESchemeOpAlterUserAttributes applies on GSS when path is DB
            // but on GSS in other cases
            if (IsDB(resolveResult)) {
                return resolveResult.DomainInfo->DomainKey.OwnerId;
            } else {
                return resolveResult.DomainInfo->Params.GetSchemeShard();
            }
        }

        if (resolveResult.RedirectRequired) {
            return resolveResult.DomainInfo->Params.GetSchemeShard();
        } else {
            return resolveResult.DomainInfo->DomainKey.OwnerId;
        }
    }

    void InterpretResolveError(const NSchemeCache::TSchemeCacheNavigate* navigate, const TActorContext &ctx) {
        for (const auto& entry: navigate->ResultSet) {
            switch (entry.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                continue;

            case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied: {
                const ui32 access = NACLib::EAccessRights::DescribeSchema;
                LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY,
                            "Access denied for " << (UserToken ? UserToken->GetUserSID() : "empty")
                            << " with access " << NACLib::AccessRightsToString(access)
                            << " to path " << JoinPath(entry.Path) << " because the base path");
                const TString errString = TStringBuilder()
                    << "Access denied for " << (UserToken ? UserToken->GetUserSID() : "empty")
                    << " to path " << JoinPath(entry.Path);
                auto issue = MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, errString);
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, nullptr, &issue, ctx);
                break;
            }
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                TxProxyMon->ResolveKeySetWrongRequest->Inc();
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, ctx);
                break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotPath:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                TxProxyMon->ResolveKeySetWrongRequest->Inc();
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, ctx);
                break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                TxProxyMon->ResolveKeySetFail->Inc();
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable, ctx);
                break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Unknown:
                TxProxyMon->ResolveKeySetFail->Inc();
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, ctx);
                break;
            }

            return;
        }

        LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY, "Unexpected response from scheme cache"
            << ": " << navigate->ToString(*AppData()->TypeRegistry));
        Y_DEBUG_ABORT("Unreachable");

        TxProxyMon->ResolveKeySetFail->Inc();
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, ctx);
    }

    static bool IsDB(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        switch (entry.Kind) {
        case NSchemeCache::TSchemeCacheNavigate::KindSubdomain:
        case NSchemeCache::TSchemeCacheNavigate::KindExtSubdomain:
            return true;
        default:
            return false;
        }
    }

    bool CheckACL(const NSchemeCache::TSchemeCacheNavigate::TResultSet& resolveSet, const TActorContext &ctx) {
        auto resolveIt = resolveSet.begin();
        auto requestIt = ResolveForACL.begin();

        while (resolveIt != resolveSet.end() && requestIt != ResolveForACL.end()) {
            const NSchemeCache::TSchemeCacheNavigate::TEntry& entry = *resolveIt;
            const TPathToResolve& request = *requestIt;

            ui32 access = requestIt->RequiredAccess;

            // request more rights if dst path is DB
            if (request.OperationRelated == NKikimrSchemeOp::ESchemeOpAlterUserAttributes) {
                if (IsDB(entry)) {
                    access |= NACLib::EAccessRights::GenericManage;
                }
            }

            if (access == NACLib::EAccessRights::NoAccess || !entry.SecurityObject) {
                ++resolveIt;
                ++requestIt;
                continue;
            }

            if (!entry.SecurityObject->CheckAccess(access, *UserToken)) {
                LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY,
                            "Access denied for " << UserToken->GetUserSID()
                            << " with access " << NACLib::AccessRightsToString(access)
                            << " to path " << JoinPath(entry.Path));

                const TString errString = TStringBuilder()
                    << "Access denied for " << UserToken->GetUserSID()
                    << " to path " << JoinPath(entry.Path);
                auto issue = MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, errString);
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, nullptr, &issue, ctx);
                return false;
            }

            if (request.OperationRelated == NKikimrSchemeOp::ESchemeOpModifyACL) {
                const auto& modifyACL = *request.RequiredGrandAccess;
                if (UserToken->IsExist(entry.SecurityObject->GetOwnerSID())) {
                    ++resolveIt;
                    ++requestIt;
                    continue;
                }

                if (!modifyACL.GetNewOwner().empty()) {
                    const TString errString = TStringBuilder()
                        << "Access denied for " << UserToken->GetUserSID()
                        << " to change ownership of " << JoinPath(entry.Path)
                        << " to " << modifyACL.GetNewOwner();
                    LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY, errString);

                    auto issue = MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, errString);
                    ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, nullptr, &issue, ctx);
                    return false;
                }

                NACLib::TDiffACL diffACL(modifyACL.GetDiffACL());
                if (!entry.SecurityObject->CheckGrantAccess(diffACL, *UserToken)) {
                    LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY,
                                "Access denied for " << UserToken->GetUserSID()
                                << " with diff ACL access " << NACLib::AccessRightsToString(NACLib::EAccessRights::GrantAccessRights)
                                << " to path " << JoinPath(entry.Path));

                    const TString errString = TStringBuilder()
                        << "Access denied for " << UserToken->GetUserSID()
                        << " to path " << JoinPath(entry.Path);
                    auto issue = MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, errString);
                    ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, nullptr, &issue, ctx);
                    return false;
                }
            }

            ++resolveIt;
            ++requestIt;
        }

        return true;
    }

    static bool IsDocApiRestricted(const NKikimrTxUserProxy::TEvProposeTransaction& tx) {
        if (tx.GetRequestType() == NDocApi::RequestType) {
            return false;
        }

        switch (tx.GetTransaction().GetModifyScheme().GetOperationType()) {
        case NKikimrSchemeOp::ESchemeOpCreateTable: // CopyTable
        case NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables:
        case NKikimrSchemeOp::ESchemeOpDropTable:
            return false;
        default:
            return true;
        }
    }

    bool CheckDocApi(const NSchemeCache::TSchemeCacheNavigate::TResultSet& resolveSet, const TActorContext &ctx) {
        for (const auto& entry: resolveSet) {
            if (entry.Attributes.contains(NDocApi::VersionAttribute)) {
                auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, TStringBuilder()
                    << "Document API table cannot be modified"
                    << ": "<< CanonizePath(entry.Path));
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest, nullptr, &issue, ctx);
                return false;
            }
        }
        return true;
    }


    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " HANDLE EvClientConnected");
        Y_ABORT_UNLESS(msg->ClientId == PipeClient);

        if (msg->Status != NKikimrProto::OK) {
            ReportStatus(TEvTxUserProxy::TResultStatus::ProxyShardNotAvailable, ctx);
            return Die(ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " HANDLE EvClientDestroyed");
        TEvTabletPipe::TEvClientDestroyed *msg = ev->Get();
        Y_ABORT_UNLESS(msg->ClientId == PipeClient);

        ReportStatus(TEvTxUserProxy::TResultStatus::ProxyShardNotAvailable, ctx);
        return Die(ctx);
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr &ev, const TActorContext &ctx) {
        const NKikimrScheme::TEvModifySchemeTransactionResult &record = ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " Status " << record.GetStatus() << " HANDLE "<< ev->Get()->ToString());

        TxProxyMon->SchemeRequestLatency->Collect((ctx.Now() - WallClockStarted).MilliSeconds());

        switch (record.GetStatus()) {
        case NKikimrScheme::EStatus::StatusAlreadyExists:
        case NKikimrScheme::EStatus::StatusSuccess:
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete, &record, nullptr, ctx);
            break;
        case NKikimrScheme::EStatus::StatusAccepted:
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress, &record, nullptr, ctx);
            break;
        default:
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError, &record, nullptr, ctx);
            break;
        }

        return Die(ctx);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev, const TActorContext &ctx) {
        NSchemeCache::TSchemeCacheNavigate *navigate = ev->Get()->Request.Get();

        TxProxyMon->CacheRequestLatency->Collect((ctx.Now() - WallClockStarted).MilliSeconds());

        LOG_LOG_S_SAMPLED_BY(ctx, (navigate->ErrorCount == 0 ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_NOTICE),
            NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " HANDLE EvNavigateKeySetResult TFlatSchemeReq marker# P5"
            << " ErrorCount# " << navigate->ErrorCount);

        Y_ABORT_UNLESS(!navigate->ResultSet.empty());

        if (navigate->ErrorCount > 0) {
            InterpretResolveError(navigate, ctx);
            return Die(ctx);
        }

        Y_ABORT_UNLESS(!navigate->ResultSet.empty());
        Y_ABORT_UNLESS(navigate->ResultSet.size() == ResolveForACL.size());

        ui64 shardToRequest = GetShardToRequest(*navigate->ResultSet.begin(), *ResolveForACL.begin());

        auto request = MakeHolder<TEvSchemeShardPropose>(TxId, shardToRequest);
        if (UserToken) {
            request->Record.SetOwner(UserToken->GetUserSID());

            if (!CheckACL(navigate->ResultSet, ctx)) {
                return Die(ctx);
            }
        }

        if (IsDocApiRestricted(SchemeRequest->Ev->Get()->Record)) {
            if (!CheckDocApi(navigate->ResultSet, ctx)) {
                    return Die(ctx);
            }
        }

        request->Record.SetPeerName(GetRequestProto().GetPeerName());
        if (GetRequestEv().HasModifyScheme()) {
            request->Record.AddTransaction()->MergeFrom(GetModifyScheme());
        } else {
            request->Record.MutableTransaction()->MergeFrom(GetModifications());
        }

        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                    "Actor# " << ctx.SelfID.ToString()
                              << " HANDLE EvNavigateKeySetResult,"
                              << " txid# " << TxId
                              << " shardToRequest# " << shardToRequest
                              << " DomainKey# " << navigate->ResultSet.begin()->DomainInfo->DomainKey
                              << " DomainInfo.Params# " << navigate->ResultSet.begin()->DomainInfo->Params.ShortDebugString()
                              << " RedirectRequired# " <<  (navigate->ResultSet.begin()->RedirectRequired ? "true" : "false"));


        SendPropose(request.Release(), shardToRequest, ctx);
        static_cast<TDerived*>(this)->Become(&TDerived::StateWaitPrepare);
    }
};

//////////////////////////////////////////////////////////////
/// \brief The TFlatSchemeReq struct
/// struct for Trasaction with one ModifySchema
///
struct TFlatSchemeReq : public TBaseSchemeReq<TFlatSchemeReq> {
    using TBase = TBaseSchemeReq<TFlatSchemeReq>;

    void Bootstrap(const TActorContext &ctx);
    void ProcessRequest(const TActorContext &ctx);

    void HandleWorkingDir(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev, const TActorContext &ctx);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_PROXY_SCHEMEREQ;
    }

    TFlatSchemeReq(const TTxProxyServices &services, ui64 txid, TAutoPtr<TEvTxProxyReq::TEvSchemeRequest> request, const TIntrusivePtr<TTxProxyMon> &txProxyMon)
        : TBase(services, txid, request, txProxyMon)
    {}

    void Die(const TActorContext &ctx) override {
        TBase::Die(ctx);
    }

    STFUNC(StateWaitResolveWorkingDir) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleWorkingDir);
        }
    }

    STFUNC(StateWaitResolve) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        }
    }

    STFUNC(StateWaitPrepare) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        }
    }
};

void TFlatSchemeReq::Bootstrap(const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                "Actor# " << ctx.SelfID.ToString()
                          << " txid# " << TxId
                          << " Bootstrap EvSchemeRequest"
                          << " record: " << SecureDebugString(GetRequestProto()));
    Y_ABORT_UNLESS(GetRequestEv().HasModifyScheme());
    Y_ABORT_UNLESS(!GetRequestEv().HasTransactionalModification());

    WallClockStarted = ctx.Now();

    TBase::Bootstrap(ctx);

    if (IsSplitMergeFromSchemeShard(GetModifyScheme())) {
        SendSplitMergePropose(ctx);
        Become(&TThis::StateWaitPrepare);
        return;
    }

    if (!ExamineTables(GetModifyScheme(), ctx)) {
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::NotImplemented, ctx);
        TxProxyMon->ResolveKeySetWrongRequest->Inc();
        return Die(ctx);
    }

    if (NeedAdjustPathNames(GetModifyScheme())) {
        auto resolveRequest = ResolveRequestForAdjustPathNames(GetModifyScheme());
        if (!resolveRequest) {
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, ctx);
            TxProxyMon->ResolveKeySetWrongRequest->Inc();
            return Die(ctx);
        }

        ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(resolveRequest));
        Become(&TThis::StateWaitResolveWorkingDir);
        return;
    }

    ProcessRequest(ctx);
 }

void TFlatSchemeReq::ProcessRequest(const TActorContext &ctx) {
    if (!ExtractResolveForACL(GetModifyScheme())) {
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::NotImplemented, ctx);
        TxProxyMon->ResolveKeySetWrongRequest->Inc();
        return Die(ctx);
    }

    auto resolveRequest = ResolveRequestForACL();
    if (!resolveRequest) {
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, ctx);
        TxProxyMon->ResolveKeySetWrongRequest->Inc();
        return Die(ctx);
    }

    LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId << " TEvNavigateKeySet requested from SchemeCache");
    ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(resolveRequest));
    Become(&TThis::StateWaitResolve);
    return;
}

void TFlatSchemeReq::HandleWorkingDir(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                "Actor# " << ctx.SelfID.ToString()
                          << " txid# " << TxId
                          << " HANDLE EvNavigateKeySetResult TFlatSchemeReq marker# P6");
    Y_ABORT_UNLESS(NeedAdjustPathNames(GetModifyScheme()));

    const auto& resultSet = ev->Get()->Request->ResultSet;

    const TVector<TString>* workingDir = nullptr;
    for (auto it = resultSet.rbegin(); it != resultSet.rend(); ++it) {
        if (it->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            workingDir = &it->Path;
            break;
        }
    }

    auto parts = Merge(SplitPath(GetModifyScheme().GetWorkingDir()), SplitPath(GetPathNameForScheme(GetModifyScheme())));

    if (!workingDir || workingDir->size() >= parts.size()) {
        const TString errText = TStringBuilder()
            << "Cannot resolve working dir"
            << " workingDir# " << (workingDir ? JoinPath(*workingDir) : "null")
            << " path# " << JoinPath(parts);
        LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY, errText);

        TxProxyMon->ResolveKeySetWrongRequest->Inc();
        const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, errText);
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, nullptr, &issue, ctx);
        return Die(ctx);
    }

    GetModifyScheme().SetWorkingDir(CombinePath(workingDir->begin(), workingDir->end()));
    GetPathNameForScheme(GetModifyScheme()) = CombinePath(parts.begin() + workingDir->size(), parts.end(), false);

    ProcessRequest(ctx);
}

//////////////////////////////////////////////////////////////
/// \brief The TFlatSchemeTransactionalReq struct
/// struct for Trasaction with several ModifySchema
///
struct TSchemeTransactionalReq : public TBaseSchemeReq<TSchemeTransactionalReq> {
    using TBase = TBaseSchemeReq<TSchemeTransactionalReq>;

    void Bootstrap(const TActorContext &ctx);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_PROXY_SCHEMEREQ;
    }

    TSchemeTransactionalReq(const TTxProxyServices &services, ui64 txid, TAutoPtr<TEvTxProxyReq::TEvSchemeRequest> request, const TIntrusivePtr<TTxProxyMon> &txProxyMon)
        : TBase(services, txid, request, txProxyMon)
    {}

    void Die(const TActorContext &ctx) override {
        TBase::Die(ctx);
    }

    STFUNC(StateWaitResolve) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        }
    }

    STFUNC(StateWaitPrepare) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        }
    }
};

void TSchemeTransactionalReq::Bootstrap(const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                "Actor# " << ctx.SelfID.ToString()
                          << " txid# " << TxId
                          << " Bootstrap EvSchemeRequest"
                          << " record: " << SecureDebugString(GetRequestProto()));
    Y_ABORT_UNLESS(!GetRequestEv().HasModifyScheme());
    Y_ABORT_UNLESS(GetRequestEv().HasTransactionalModification());

    WallClockStarted = ctx.Now();

    TBase::Bootstrap(ctx);

    for(auto& scheme: GetModifications()) {
        if (!ExamineTables(scheme, ctx)) {
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::NotImplemented, ctx);
            TxProxyMon->ResolveKeySetWrongRequest->Inc();
            return Die(ctx);
        }
    }

    for(auto& scheme: GetModifications()) {
        if (!ExtractResolveForACL(scheme)) {
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::NotImplemented, ctx);
            TxProxyMon->ResolveKeySetWrongRequest->Inc();
            return Die(ctx);
        }
    }

    auto resolveRequest = ResolveRequestForACL();
    if (!resolveRequest) {
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, ctx);
        TxProxyMon->ResolveKeySetWrongRequest->Inc();
        return Die(ctx);
    }

    LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId << " TEvNavigateKeySet requested from SchemeCache");
    ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(resolveRequest));
    Become(&TThis::StateWaitResolve);
    return;
}

IActor* CreateTxProxyFlatSchemeReq(const TTxProxyServices &services, const ui64 txid, TAutoPtr<TEvTxProxyReq::TEvSchemeRequest> request, const TIntrusivePtr<NKikimr::NTxProxy::TTxProxyMon>& mon) {
    if (request->Ev->Get()->HasModifyScheme()) {
        return new TFlatSchemeReq(services, txid, request, mon);
    } else {
        return new TSchemeTransactionalReq(services, txid, request, mon);
    }
}

}
}
