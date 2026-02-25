#include "proxy.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/base/local_user_token.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/docapi/traits.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/security/sasl/events.h>
#include <ydb/core/security/sasl/hasher.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/library/login/login.h>
#include <ydb/library/login/protos/login.pb.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/login/hashes_checker/hashes_checker.h>
#include <ydb/library/protobuf_printer/security_printer.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>

#include <util/string/cast.h>

namespace {

const TVector<NLoginProto::EHashType::HashType> HASHES_TO_COMPUTE = {
    NLoginProto::EHashType::Argon,
    NLoginProto::EHashType::ScramSha256,
};

}

namespace NKikimr {
namespace NTxProxy {

TString GetUserSID(const std::optional<NACLib::TUserToken>& userToken) {
    return (userToken ? userToken->GetUserSID() : "<empty>");
}

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
    ui64 SchemeshardIdToRequest;

    struct TPathToResolve {
        const NKikimrSchemeOp::TModifyScheme& ModifyScheme;
        ui32 RequireAccess = NACLib::EAccessRights::NoAccess;

        // Params for NSchemeCache::TSchemeCacheNavigate::TEntry
        TVector<TString> Path;
        bool RequireRedirect = true;

        TPathToResolve(const NKikimrSchemeOp::TModifyScheme& modifyScheme)
            : ModifyScheme(modifyScheme)
        {
        }
    };

    TVector<TPathToResolve> ResolveForACL;

    std::optional<NACLib::TUserToken> UserToken;
    bool CheckAdministrator = false;
    bool CheckDatabaseAdministrator = false;
    bool IsClusterAdministrator = false;
    bool IsDatabaseAdministrator = false;
    NACLib::TSID DatabaseOwner;
    NLoginProto::TSecurityState DatabaseSecurityState;

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

    THolder<TEvSchemeShardPropose> MakePropose(ui64 schemeshardIdToRequest) {
        auto request = MakeHolder<TEvSchemeShardPropose>(TxId, schemeshardIdToRequest);

        if (UserToken) {
            request->Record.SetOwner(UserToken->GetUserSID());
        }

        request->Record.SetPeerName(GetRequestProto().GetPeerName());
        if (GetRequestEv().HasModifyScheme()) {
            request->Record.AddTransaction()->MergeFrom(GetModifyScheme());
        } else {
            request->Record.MutableTransaction()->MergeFrom(GetModifications());
        }

        return request;
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
        case NKikimrSchemeOp::ESchemeOpDropTransfer:
        case NKikimrSchemeOp::ESchemeOpDropTransferCascade:
        case NKikimrSchemeOp::ESchemeOpDropBlobDepot:
        case NKikimrSchemeOp::ESchemeOpDropExternalTable:
        case NKikimrSchemeOp::ESchemeOpDropExternalDataSource:
        case NKikimrSchemeOp::ESchemeOpDropView:
        case NKikimrSchemeOp::ESchemeOpDropResourcePool:
        case NKikimrSchemeOp::ESchemeOpDropSysView:
        case NKikimrSchemeOp::ESchemeOpDropSecret:
        case NKikimrSchemeOp::ESchemeOpDropStreamingQuery:
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

        case NKikimrSchemeOp::ESchemeOpCreateSetConstraintInitiate:
            return *modifyScheme.MutableSetColumnConstraintsInitiate()->MutableTableName();

        case NKikimrSchemeOp::ESchemeOpCreateColumnBuild:
            Y_ABORT("no implementation for ESchemeOpCreateColumnBuild");

        case NKikimrSchemeOp::ESchemeOpDropColumnBuild:
            Y_ABORT("no implementation for ESchemeOpDropColumnBuild");

        case NKikimrSchemeOp::ESchemeOpCreateIndexBuild:
            Y_ABORT("no implementation for ESchemeOpCreateIndexBuild");

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

        case NKikimrSchemeOp::ESchemeOpRotateCdcStream:
            return *modifyScheme.MutableRotateCdcStream()->MutableTableName();

        case NKikimrSchemeOp::ESchemeOpRotateCdcStreamImpl:
            Y_ABORT("no implementation for ESchemeOpRotateCdcStreamImpl");

        case NKikimrSchemeOp::ESchemeOpRotateCdcStreamAtTable:
            return *modifyScheme.MutableRotateCdcStream()->MutableTableName();

        case NKikimrSchemeOp::ESchemeOpMoveTable:
            Y_ABORT("no implementation for ESchemeOpMoveTable");

        case NKikimrSchemeOp::ESchemeOpMoveTableIndex:
            Y_ABORT("no implementation for ESchemeOpMoveTableIndex");

        case NKikimrSchemeOp::ESchemeOpMoveIndex:
            Y_ABORT("no implementation for ESchemeOpMoveIndex");

        case NKikimrSchemeOp::ESchemeOpMoveSequence:
            Y_ABORT("no implementation for ESchemeOpMoveSequence");

        case NKikimrSchemeOp::ESchemeOpCreateSequence:
        case NKikimrSchemeOp::ESchemeOpAlterSequence:
            return *modifyScheme.MutableSequence()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreateReplication:
        case NKikimrSchemeOp::ESchemeOpAlterReplication:
        case NKikimrSchemeOp::ESchemeOpCreateTransfer:
        case NKikimrSchemeOp::ESchemeOpAlterTransfer:
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

        case NKikimrSchemeOp::ESchemeOpCreateSecret:
            return *modifyScheme.MutableCreateSecret()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterSecret:
            return *modifyScheme.MutableAlterSecret()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreateResourcePool:
            return *modifyScheme.MutableCreateResourcePool()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterResourcePool:
            return *modifyScheme.MutableCreateResourcePool()->MutableName();

        case NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups:
        case NKikimrSchemeOp::ESchemeOpRestoreIncrementalBackupAtTable:
        // TODO verify all logic based on this, it may be irrelevant
            return *modifyScheme.MutableRestoreMultipleIncrementalBackups()->MutableSrcTablePaths(0);

        case NKikimrSchemeOp::ESchemeOpCreateBackupCollection:
            return *modifyScheme.MutableCreateBackupCollection()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterBackupCollection:
            return *modifyScheme.MutableAlterBackupCollection()->MutableName();

        case NKikimrSchemeOp::ESchemeOpDropBackupCollection:
            return *modifyScheme.MutableDropBackupCollection()->MutableName();

        case NKikimrSchemeOp::ESchemeOpBackupBackupCollection:
            return *modifyScheme.MutableBackupBackupCollection()->MutableName();

        case NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection:
            return *modifyScheme.MutableBackupIncrementalBackupCollection()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreateLongIncrementalBackupOp:
            return *modifyScheme.MutableBackupIncrementalBackupCollection()->MutableName();

        case NKikimrSchemeOp::ESchemeOpRestoreBackupCollection:
            return *modifyScheme.MutableRestoreBackupCollection()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreateLongIncrementalRestoreOp:
            return *modifyScheme.MutableRestoreBackupCollection()->MutableName();

        case NKikimrSchemeOp::ESchemeOpCreateSysView:
            return *modifyScheme.MutableCreateSysView()->MutableName();

        case NKikimrSchemeOp::ESchemeOpChangePathState:
            return *modifyScheme.MutableChangePathState()->MutablePath();

        case NKikimrSchemeOp::ESchemeOpIncrementalRestoreFinalize:
            return *modifyScheme.MutableIncrementalRestoreFinalize()->MutableTargetTablePaths(0);

        case NKikimrSchemeOp::ESchemeOpCreateStreamingQuery:
            return *modifyScheme.MutableCreateStreamingQuery()->MutableName();

        case NKikimrSchemeOp::ESchemeOpAlterStreamingQuery:
            return *modifyScheme.MutableCreateStreamingQuery()->MutableName();

        case NKikimrSchemeOp::ESchemeOpTruncateTable:
            return *modifyScheme.MutableTruncateTable()->MutableTableName();
        }
        Y_UNREACHABLE();
    }

    static void SetPathNameForScheme(NKikimrSchemeOp::TModifyScheme& modifyScheme, const TString& name) {
        GetPathNameForScheme(modifyScheme) = name;
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
        case NKikimrSchemeOp::ESchemeOpCreateBackupCollection:
        case NKikimrSchemeOp::ESchemeOpCreateSysView:
        case NKikimrSchemeOp::ESchemeOpCreateSecret:
        case NKikimrSchemeOp::ESchemeOpCreateStreamingQuery:
            return true;
        default:
            return false;
        }
    }

    static bool NeedAdjustPathNames(const NKikimrSchemeOp::TModifyScheme& modifyScheme) {
        return IsCreateRequest(modifyScheme);
    }

    static TVector<TString> GetFullPath(NKikimrSchemeOp::TModifyScheme& scheme) {
        switch (scheme.GetOperationType()) {
        case NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups:
        case NKikimrSchemeOp::ESchemeOpRestoreIncrementalBackupAtTable:
            return SplitPath(GetPathNameForScheme(scheme));
        default:
            return Merge(SplitPath(scheme.GetWorkingDir()), SplitPath(GetPathNameForScheme(scheme)));
        }
    }

    static THolder<NSchemeCache::TSchemeCacheNavigate> ResolveRequestForAdjustPathNames(
        const TString& database, NKikimrSchemeOp::TModifyScheme& scheme)
    {
        auto parts = GetFullPath(scheme);
        if (parts.size() < 2) {
            return {};
        }

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = database;

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

            if (shardResult->HasOperationId()) {
                result->Record.SetSchemeShardOperationId(shardResult->GetOperationId());
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
        if (result->Record.GetSchemeShardReason()) {
            auto issueStatus = NKikimrIssues::TIssuesIds::DEFAULT_ERROR;
            if (result->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusPathDoesNotExist) {
                issueStatus = NKikimrIssues::TIssuesIds::PATH_NOT_EXIST;
            }
            auto issue = MakeIssue(std::move(issueStatus), result->Record.GetSchemeShardReason());
            NYql::IssueToMessage(issue, result->Record.AddIssues());
        }
        if (result->Record.IssuesSize() > 0) {
            LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                << ", issues: " << result->Record.GetIssues());
        }
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " SEND to# " << Source.ToString() << " Source " << result->ToString());
        ctx.Send(Source, result);
    }

    void ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status, const TActorContext &ctx)
    {
        ReportStatus(status, nullptr, nullptr, ctx);
    }

    void Bootstrap(const TActorContext& ctx) {
        ExtractUserToken();

        CheckAdministrator = AppData()->FeatureFlags.GetEnableStrictUserManagement();
        CheckDatabaseAdministrator = CheckAdministrator && AppData()->FeatureFlags.GetEnableDatabaseAdmin();

        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " Bootstrap,"
            << " UserSID: " << GetUserSID(UserToken)
            << " CheckAdministrator: " << CheckAdministrator
            << " CheckDatabaseAdministrator: " << CheckDatabaseAdministrator
        );

        // Resolve database to get its owner and be able to detect if user is the database admin
        if (UserToken) {
            IsClusterAdministrator = NKikimr::IsAdministrator(AppData(), &UserToken.value());
            LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                << " Bootstrap,"
                << " UserSID: " << GetUserSID(UserToken)
                << " IsClusterAdministrator: " << IsClusterAdministrator
            );

            // Cluster admin trumps database admin, database owner check is needed only for database admin.
            if (!IsClusterAdministrator && CheckDatabaseAdministrator) {
                auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
                request->DatabaseName = GetRequestProto().GetDatabaseName();

                auto& entry = request->ResultSet.emplace_back();
                entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
                entry.Path = NKikimr::SplitPath(request->DatabaseName);

                ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));

                static_cast<TDerived*>(this)->Become(&TDerived::StateWaitResolveDatabase);
                return;
            }
        }

        static_cast<TDerived*>(this)->Start(ctx);
    }

    void Die(const TActorContext &ctx) override {
        --*TxProxyMon->SchemeReqInFly;

        if (PipeClient) {
            NTabletPipe::CloseClient(ctx, PipeClient);
            PipeClient = TActorId();
        }

        TBase::Die(ctx);
    }

    void RunPasswordHasher(const TActorContext &ctx, const TString& username, const TString& password) {
        const auto& passwordComplexityProto = AppData()->AuthConfig.GetPasswordComplexity();
        NLogin::TPasswordComplexity passwordComplexity({
            .MinLength = passwordComplexityProto.GetMinLength(),
            .MinLowerCaseCount = passwordComplexityProto.GetMinLowerCaseCount(),
            .MinUpperCaseCount = passwordComplexityProto.GetMinUpperCaseCount(),
            .MinNumbersCount = passwordComplexityProto.GetMinNumbersCount(),
            .MinSpecialCharsCount = passwordComplexityProto.GetMinSpecialCharsCount(),
            .SpecialChars = passwordComplexityProto.GetSpecialChars(),
            .CanContainUsername = passwordComplexityProto.GetCanContainUsername(),
        });

        NSasl::TStaticCredentials creds(username, password);
        TBase::Register(NSasl::CreateHasher(ctx.SelfID, creds, HASHES_TO_COMPUTE, std::move(passwordComplexity)).release());
        return;
    }

    // KIKIMR-12624 move that logic to the schemeshard
    bool CheckTablePrereqs(const NKikimrSchemeOp::TTableDescription &desc, const TString &path, const TActorContext& ctx) {
        // check ad-hoc prereqs for table alter/creation

        // 1. split and external blobs must not be used simultaneously
        if (desc.HasPartitionConfig()) {
            const auto &partition = desc.GetPartitionConfig();
            if (partition.HasPartitioningPolicy() && partition.GetPartitioningPolicy().GetSizeToSplit() > 0) {
                if (PartitionConfigHasExternalBlobsEnabled(partition)) {
                    LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY, "Actor#" << ctx.SelfID.ToString() << " txid# " << TxId
                        << " must not use auto-split and external blobs simultaneously, path# " << path
                    );
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
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequireAccess = NACLib::EAccessRights::CreateDatabase | NACLib::EAccessRights::AlterSchema | accessToUserAttrs;
            toResolve.RequireRedirect = false;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpCreateSubDomain:
        case NKikimrSchemeOp::ESchemeOpCreateExtSubDomain:
        {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = workingDir;
            toResolve.RequireAccess = NACLib::EAccessRights::CreateDatabase | accessToUserAttrs;
            toResolve.RequireRedirect = false;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpAlterUserAttributes:
        {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequireAccess = NACLib::EAccessRights::WriteUserAttributes | accessToUserAttrs;
            toResolve.RequireRedirect = false;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions: {
            auto& path = pbModifyScheme.GetSplitMergeTablePartitions().GetTablePath();
            TString baseDir = ToString(ExtractParent(path)); // why baseDir?

            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = SplitPath(baseDir);
            toResolve.RequireAccess = NACLib::EAccessRights::NoAccess; // why not?
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpAlterTable:
        case NKikimrSchemeOp::ESchemeOpDropIndex:
        case NKikimrSchemeOp::ESchemeOpCreateCdcStream:
        case NKikimrSchemeOp::ESchemeOpAlterCdcStream:
        case NKikimrSchemeOp::ESchemeOpDropCdcStream:
        case NKikimrSchemeOp::ESchemeOpRotateCdcStream:
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
        case NKikimrSchemeOp::ESchemeOpAlterBackupCollection:
        case NKikimrSchemeOp::ESchemeOpAlterSecret:
        case NKikimrSchemeOp::ESchemeOpAlterStreamingQuery:
        {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequireAccess = NACLib::EAccessRights::AlterSchema | accessToUserAttrs;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpCreateSecret:
        {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = workingDir;
            toResolve.RequireAccess = NACLib::EAccessRights::CreateTable | accessToUserAttrs;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpAlterTransfer:
        {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequireAccess = NACLib::EAccessRights::AlterSchema | accessToUserAttrs;
            ResolveForACL.push_back(toResolve);

            auto& config = pbModifyScheme.GetReplication().GetConfig();
            auto& target = config.GetTransferSpecific().GetTarget();

            std::vector<TString> pathForChecking;
            if (target.HasDstPath()) {
                pathForChecking.push_back(target.GetDstPath());
            }
            if (target.HasDirectoryPath()) {
                pathForChecking.push_back(target.GetDirectoryPath());
            }

            for (const auto& path : pathForChecking) {
                auto toWriteTable = TPathToResolve(pbModifyScheme);
                toWriteTable.Path = SplitPath(path);
                toWriteTable.RequireAccess = NACLib::EAccessRights::UpdateRow;
                ResolveForACL.push_back(toWriteTable);
            }

            break;
        }
        case NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups:
        {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = SplitPath(GetPathNameForScheme(pbModifyScheme));
            toResolve.RequireAccess = NACLib::EAccessRights::AlterSchema | accessToUserAttrs;
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
        case NKikimrSchemeOp::ESchemeOpDropTransfer:
        case NKikimrSchemeOp::ESchemeOpDropTransferCascade:
        case NKikimrSchemeOp::ESchemeOpDropBlobDepot:
        case NKikimrSchemeOp::ESchemeOpDropExternalTable:
        case NKikimrSchemeOp::ESchemeOpDropExternalDataSource:
        case NKikimrSchemeOp::ESchemeOpDropView:
        case NKikimrSchemeOp::ESchemeOpDropResourcePool:
        case NKikimrSchemeOp::ESchemeOpDropBackupCollection:
        case NKikimrSchemeOp::ESchemeOpDropSecret:
        case NKikimrSchemeOp::ESchemeOpDropStreamingQuery:
        {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequireAccess = NACLib::EAccessRights::RemoveSchema;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpDropSubDomain:
        case NKikimrSchemeOp::ESchemeOpForceDropSubDomain:
        case NKikimrSchemeOp::ESchemeOpForceDropExtSubDomain: {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequireAccess = NACLib::EAccessRights::DropDatabase;
            toResolve.RequireRedirect = false;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpForceDropUnsafe: {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequireAccess = NACLib::EAccessRights::DropDatabase | NACLib::EAccessRights::RemoveSchema;
            toResolve.RequireRedirect = false;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpModifyACL: {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequireAccess = NACLib::EAccessRights::GrantAccessRights | accessToUserAttrs;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpCreateTable: {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = workingDir;
            toResolve.RequireAccess = NACLib::EAccessRights::CreateTable | accessToUserAttrs;
            ResolveForACL.push_back(toResolve);

            if (pbModifyScheme.GetCreateTable().HasCopyFromTable()) {
                auto toResolve = TPathToResolve(pbModifyScheme);
                toResolve.Path = SplitPath(pbModifyScheme.GetCreateTable().GetCopyFromTable());
                toResolve.RequireAccess = NACLib::EAccessRights::SelectRow;
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
        case NKikimrSchemeOp::ESchemeOpCreateBackupCollection:
        case NKikimrSchemeOp::ESchemeOpCreateStreamingQuery:
        {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = workingDir;
            toResolve.RequireAccess = NACLib::EAccessRights::CreateTable | accessToUserAttrs;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpCreateTransfer:
        {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = workingDir;
            toResolve.RequireAccess = NACLib::EAccessRights::CreateTable | accessToUserAttrs;
            ResolveForACL.push_back(toResolve);

            auto& config = pbModifyScheme.GetReplication().GetConfig();
            auto& target = config.GetTransferSpecific().GetTarget();

            auto toWriteTable = TPathToResolve(pbModifyScheme);
            toWriteTable.Path = SplitPath(target.GetDstPath());
            toWriteTable.RequireAccess = NACLib::EAccessRights::UpdateRow;
            ResolveForACL.push_back(toWriteTable);

            if (target.HasDirectoryPath()) {
                auto toWriteDir = TPathToResolve(pbModifyScheme);
                toWriteDir.Path = SplitPath(target.GetDirectoryPath());
                toWriteDir.RequireAccess = NACLib::EAccessRights::UpdateRow;
                ResolveForACL.push_back(toWriteDir);
            }

            break;
        }
        case NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables: {
            for (auto& item: pbModifyScheme.GetCreateConsistentCopyTables().GetCopyTableDescriptions()) {
                {
                    auto toResolve = TPathToResolve(pbModifyScheme);
                    toResolve.Path = SplitPath(item.GetSrcPath());
                    toResolve.RequireAccess = NACLib::EAccessRights::SelectRow;
                    ResolveForACL.push_back(toResolve);
                }
                {
                    auto toResolve = TPathToResolve(pbModifyScheme);
                    auto dstDir = ToString(ExtractParent(item.GetDstPath()));
                    toResolve.Path = SplitPath(dstDir);
                    toResolve.RequireAccess = NACLib::EAccessRights::CreateTable;
                    ResolveForACL.push_back(toResolve);
                }
            }
            break;
        }
        case NKikimrSchemeOp::ESchemeOpBackupBackupCollection: {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = workingDir;
            auto collectionPath = SplitPath(pbModifyScheme.GetBackupBackupCollection().GetName());
            std::move(collectionPath.begin(), collectionPath.end(), std::back_inserter(toResolve.Path));
            toResolve.RequireAccess = NACLib::EAccessRights::GenericWrite;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection: {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = workingDir;
            auto collectionPath = SplitPath(pbModifyScheme.GetBackupIncrementalBackupCollection().GetName());
            std::move(collectionPath.begin(), collectionPath.end(), std::back_inserter(toResolve.Path));
            toResolve.RequireAccess = NACLib::EAccessRights::GenericWrite;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpCreateLongIncrementalBackupOp: {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = workingDir;
            auto collectionPath = SplitPath(pbModifyScheme.GetBackupIncrementalBackupCollection().GetName());
            std::move(collectionPath.begin(), collectionPath.end(), std::back_inserter(toResolve.Path));
            toResolve.RequireAccess = NACLib::EAccessRights::GenericWrite;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpRestoreBackupCollection: {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = workingDir;
            auto collectionPath = SplitPath(pbModifyScheme.GetRestoreBackupCollection().GetName());
            std::move(collectionPath.begin(), collectionPath.end(), std::back_inserter(toResolve.Path));
            toResolve.RequireAccess = NACLib::EAccessRights::GenericWrite;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpCreateLongIncrementalRestoreOp: {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = workingDir;
            auto collectionPath = SplitPath(pbModifyScheme.GetRestoreBackupCollection().GetName());
            std::move(collectionPath.begin(), collectionPath.end(), std::back_inserter(toResolve.Path));
            toResolve.RequireAccess = NACLib::EAccessRights::GenericWrite;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpMoveTable: {
            auto& descr = pbModifyScheme.GetMoveTable();
            {
                auto toResolve = TPathToResolve(pbModifyScheme);
                toResolve.Path = SplitPath(descr.GetSrcPath());
                toResolve.RequireAccess = NACLib::EAccessRights::SelectRow | NACLib::EAccessRights::RemoveSchema;
                ResolveForACL.push_back(toResolve);
            }
            {
                auto toResolve = TPathToResolve(pbModifyScheme);
                auto dstDir = ToString(ExtractParent(descr.GetDstPath()));
                toResolve.Path = SplitPath(dstDir);
                toResolve.RequireAccess = NACLib::EAccessRights::CreateTable;
                ResolveForACL.push_back(toResolve);
            }
            break;
        }
        case NKikimrSchemeOp::ESchemeOpMoveIndex: {
            auto& descr = pbModifyScheme.GetMoveIndex();
            {
                auto toResolve = TPathToResolve(pbModifyScheme);
                toResolve.Path = SplitPath(descr.GetTablePath());
                toResolve.RequireAccess = NACLib::EAccessRights::AlterSchema;
                ResolveForACL.push_back(toResolve);
            }
            break;
        }
        case NKikimrSchemeOp::ESchemeOpMkDir:
        {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = workingDir;
            toResolve.RequireAccess = NACLib::EAccessRights::CreateDirectory | accessToUserAttrs;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup:
        {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = workingDir;
            toResolve.RequireAccess = NACLib::EAccessRights::CreateQueue | accessToUserAttrs;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpAlterLogin:
        {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = workingDir;
            toResolve.RequireAccess = NACLib::EAccessRights::AlterSchema;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpMoveSequence: {
            auto& descr = pbModifyScheme.GetMoveSequence();
            {
                auto toResolve = TPathToResolve(pbModifyScheme);
                toResolve.Path = SplitPath(descr.GetSrcPath());
                toResolve.RequireAccess = NACLib::EAccessRights::RemoveSchema;
                ResolveForACL.push_back(toResolve);
            }
            {
                auto toResolve = TPathToResolve(pbModifyScheme);
                auto dstDir = ToString(ExtractParent(descr.GetDstPath()));
                toResolve.Path = SplitPath(dstDir);
                toResolve.RequireAccess = NACLib::EAccessRights::CreateTable | accessToUserAttrs;
                ResolveForACL.push_back(toResolve);
            }
            break;
        }
        // TODO(n00bcracker): add processing after support on client side
        case NKikimrSchemeOp::ESchemeOpCreateSysView:
        case NKikimrSchemeOp::ESchemeOpDropSysView:
            return false;
        case NKikimrSchemeOp::ESchemeOpChangePathState: {
        case NKikimrSchemeOp::ESchemeOpCreateSetConstraintInitiate:
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequireAccess = NACLib::EAccessRights::AlterSchema | accessToUserAttrs;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpTruncateTable: {
            auto toResolve = TPathToResolve(pbModifyScheme);
            toResolve.Path = Merge(workingDir, SplitPath(GetPathNameForScheme(pbModifyScheme)));
            toResolve.RequireAccess = NACLib::EAccessRights::EraseRow;
            ResolveForACL.push_back(toResolve);
            break;
        }
        case NKikimrSchemeOp::ESchemeOpCreateTableIndex:
        case NKikimrSchemeOp::ESchemeOpDropTableIndex:
        case NKikimrSchemeOp::ESchemeOp_DEPRECATED_35:
        case NKikimrSchemeOp::ESchemeOpCreateColumnBuild:
        case NKikimrSchemeOp::ESchemeOpDropColumnBuild:
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
        case NKikimrSchemeOp::ESchemeOpRotateCdcStreamImpl:
        case NKikimrSchemeOp::ESchemeOpRotateCdcStreamAtTable:
        case NKikimrSchemeOp::ESchemeOpMoveTableIndex:
        case NKikimrSchemeOp::ESchemeOpAlterExtSubDomainCreateHive:
        case NKikimrSchemeOp::ESchemeOpAlterView:
        case NKikimrSchemeOp::ESchemeOpRestoreIncrementalBackupAtTable:
        case NKikimrSchemeOp::ESchemeOpIncrementalRestoreFinalize:
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
            entry.RedirectRequired = toReq.RequireRedirect;
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

        if (resolveTask.ModifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterUserAttributes) {
            // ESchemeOpAlterUserAttributes applies on GSS when path is DB
            // but on TSS in other cases
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

    TString MakeAccessDeniedError(const TActorContext& ctx, const TVector<TString>& path, const TString& part) {
        const TString msg = TStringBuilder() << "Access denied for " << GetUserSID(UserToken)
            << " on path " << CanonizePath(JoinPath(path))
        ;
        LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << ", " << msg << ", " << part
        );
        return msg;
    }
    TString MakeAccessDeniedError(const TActorContext& ctx, const TString& part) {
        const TString msg = TStringBuilder() << "Access denied for " << GetUserSID(UserToken);
        LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << ", " << msg << ", " << part
        );
        return msg;
    }

    void InterpretResolveError(const NSchemeCache::TSchemeCacheNavigate* navigate, const TActorContext &ctx) {
        for (const auto& entry: navigate->ResultSet) {
            switch (entry.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                continue;

            case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied: {
                const ui32 access = NACLib::EAccessRights::DescribeSchema;
                const auto errString = MakeAccessDeniedError(ctx, entry.Path, TStringBuilder()
                    << "with access " << NACLib::AccessRightsToString(access) << ": base path is inaccessible"
                );
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
            case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                TxProxyMon->ResolveKeySetFail->Inc();
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable, ctx);
                break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable:
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

    bool CheckAccess(const NSchemeCache::TSchemeCacheNavigate::TResultSet& resolveSet, const TActorContext &ctx) {
        const bool checkAdmin = (CheckAdministrator || CheckDatabaseAdministrator);
        const bool isAdmin = (IsClusterAdministrator || IsDatabaseAdministrator);

        auto resolveIt = resolveSet.begin();
        auto requestIt = ResolveForACL.begin();

        while (resolveIt != resolveSet.end() && requestIt != ResolveForACL.end()) {
            const NSchemeCache::TSchemeCacheNavigate::TEntry& entry = *resolveIt;
            const TPathToResolve& request = *requestIt;
            const auto& modifyScheme = request.ModifyScheme;

            bool allowACLBypass = false;

            // Check admin restrictions and special cases
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterLogin) {
                // EnableStrictUserManagement == false:
                //   - any user can manage users|groups, but require AlterSchema right
                //
                // EnableStrictUserManagement == true:
                //   - user can change password for himself, and does not require AlterSchema right
                //   - only admins can manage users|groups, and does not require AlterSchema right
                //   - database admin can't change other database admins
                //   - database admin can't change database admin group
                //   - database admin can't change database owner
                //
                const auto& alterLogin = modifyScheme.GetAlterLogin();

                // User changes password for himself (and only password)
                bool isUserChangesOwnPassword = [](const auto& alterLogin, const NACLib::TSID& subjectSid) {
                    if (alterLogin.GetAlterCase() == NKikimrSchemeOp::TAlterLogin::kModifyUser) {
                        const auto& targetUser = alterLogin.GetModifyUser();
                        if (targetUser.HasPassword() && !targetUser.HasCanLogin()) {
                            return (subjectSid == targetUser.GetUser());
                        }
                    }
                    return false;
                }(alterLogin, UserToken->GetUserSID());

                bool allowManageUser = !checkAdmin || isAdmin || isUserChangesOwnPassword;

                if (!allowManageUser) {
                    const auto errString = MakeAccessDeniedError(ctx, "attempt to manage user");
                    auto issue = MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, errString);
                    ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, nullptr, &issue, ctx);
                    return false;
                }

                allowACLBypass = (checkAdmin && isAdmin) || isUserChangesOwnPassword;

                // Database admin is not allowed to manage group of database admins or change other database admins
                // (its the privilege of cluster admins).
                if (checkAdmin && IsDatabaseAdministrator) {
                    TString group;
                    switch (alterLogin.GetAlterCase()) {
                        case NKikimrSchemeOp::TAlterLogin::kAddGroupMembership:
                            group = alterLogin.GetAddGroupMembership().GetGroup();
                            break;
                        case NKikimrSchemeOp::TAlterLogin::kRemoveGroupMembership:
                            group = alterLogin.GetRemoveGroupMembership().GetGroup();
                            break;
                        case NKikimrSchemeOp::TAlterLogin::kRemoveGroup:
                            group = alterLogin.GetRemoveGroup().GetGroup();
                            break;
                        case NKikimrSchemeOp::TAlterLogin::kRenameGroup:
                            group = alterLogin.GetRenameGroup().GetGroup();
                            break;
                        default:
                            break;
                    }
                    if (!group.empty() && group == DatabaseOwner) {
                        const auto errString = MakeAccessDeniedError(ctx, entry.Path, TStringBuilder()
                            << "attempt to administer database admin group by the database admin"
                        );
                        auto issue = MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, errString);
                        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, nullptr, &issue, ctx);
                        return false;
                    }

                    bool isAdminChangesOwnPasswordHash = [](const auto& alterLogin, const NACLib::TSID& subjectSid) {
                        if (alterLogin.GetAlterCase() == NKikimrSchemeOp::TAlterLogin::kModifyUser) {
                            const auto& targetUser = alterLogin.GetModifyUser();
                            if (targetUser.HasHashedPassword() && !targetUser.HasCanLogin()) {
                                return (subjectSid == targetUser.GetUser());
                            }
                        }
                        return false;
                    }(alterLogin, UserToken->GetUserSID());

                    // Database admin still can change its own password
                    if (alterLogin.GetAlterCase() == NKikimrSchemeOp::TAlterLogin::kModifyUser
                        && !isUserChangesOwnPassword && !isAdminChangesOwnPasswordHash) {
                        const auto& targetUser = alterLogin.GetModifyUser();
                        const auto targetUserToken = NKikimr::BuildLocalUserToken(DatabaseSecurityState, targetUser.GetUser());
                        if (UserToken->GetUserSID() == targetUser.GetUser()) {
                            const auto errString = MakeAccessDeniedError(ctx, entry.Path, TStringBuilder()
                                << "attempt to change self login attributes managed by the cluster admin"
                            );
                            auto issue = MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, errString);
                            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, nullptr, &issue, ctx);
                            return false;
                        } else if (NKikimr::IsDatabaseAdministrator(&targetUserToken, DatabaseOwner)) {
                            const auto errString = MakeAccessDeniedError(ctx, entry.Path, TStringBuilder()
                                << "attempt to change other database admins"
                            );
                            auto issue = MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, errString);
                            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, nullptr, &issue, ctx);
                            return false;
                        }
                    }
                }

            } else if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpModifyACL) {
                // Only the owner of the schema object (path) can transfer their ownership away.
                // Or admins (if configured so).
                const auto& newOwner = modifyScheme.GetModifyACL().GetNewOwner();
                if (!newOwner.empty()) {
                    // This modifyACL is changing the owner
                    auto isObjectOwner = [](const auto& userToken, const NACLib::TSID& owner) {
                        return userToken->IsExist(owner);
                    };
                    const auto& owner = entry.Self->Info.GetOwner();
                    const bool allow = (isAdmin || isObjectOwner(UserToken, owner));
                    if (!allow) {
                        const auto errString = MakeAccessDeniedError(ctx, entry.Path, TStringBuilder()
                            << "attempt to change ownership"
                            << " from " << owner
                            << " to " << newOwner
                        );
                        auto issue = MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, errString);
                        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, nullptr, &issue, ctx);
                        return false;
                    }

                    // Database admin is not allowed to change ownership of its own database
                    if (IsDatabaseAdministrator && IsDB(entry)) {
                        const auto errString = MakeAccessDeniedError(ctx, entry.Path, TStringBuilder()
                            << "attempt to change database ownership by the database admin"
                            << " from " << owner
                            << " to " << newOwner
                        );
                        auto issue = MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, errString);
                        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, nullptr, &issue, ctx);
                        return false;
                    }
                }
            } else if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterExtSubDomain) {
                if (IsDB(entry) && !IsClusterAdministrator) {
                    const auto errString = MakeAccessDeniedError(ctx, entry.Path, TStringBuilder()
                        << "only cluster admins can alter databases"
                    );
                    auto issue = MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, errString);
                    ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, nullptr, &issue, ctx);
                    return false;
                }
            }

            ui32 access = requestIt->RequireAccess;

            // request more rights if dst path is DB
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterUserAttributes) {
                if (IsDB(entry)) {
                    access |= NACLib::EAccessRights::GenericManage;
                }
            }

            if (allowACLBypass || access == NACLib::EAccessRights::NoAccess || !entry.SecurityObject) {
                ++resolveIt;
                ++requestIt;
                continue;
            }

            if (!entry.SecurityObject->CheckAccess(access, *UserToken)) {
                const auto errString = MakeAccessDeniedError(ctx, entry.Path, TStringBuilder()
                    << "with access " << NACLib::AccessRightsToString(access)
                );
                auto issue = MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, errString);
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, nullptr, &issue, ctx);
                return false;
            }

            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpModifyACL) {
                const auto& modifyACL = modifyScheme.GetModifyACL();

                if (!modifyACL.GetDiffACL().empty()) {
                    NACLib::TDiffACL diffACL(modifyACL.GetDiffACL());
                    if (!entry.SecurityObject->CheckGrantAccess(diffACL, *UserToken)) {
                        const auto errString = MakeAccessDeniedError(ctx, entry.Path, TStringBuilder()
                            << "with diff ACL access " << NACLib::AccessRightsToString(NACLib::EAccessRights::GrantAccessRights)
                        );
                        auto issue = MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, errString);
                        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, nullptr, &issue, ctx);
                        return false;
                    }
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

    void HandleResolveDatabase(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev, const TActorContext &ctx) {
        const NSchemeCache::TSchemeCacheNavigate& request = *ev->Get()->Request.Get();

        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " HandleResolveDatabase,"
            << " ResultSet size: " << request.ResultSet.size()
            << " ResultSet error count: " << request.ErrorCount
        );

        if (request.ResultSet.empty()) {
            const TString msg = TStringBuilder() << "Error resolving database " << request.DatabaseName << ": no response";
            LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                << ", " << msg
            );

            TxProxyMon->ResolveKeySetWrongRequest->Inc();

            const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, msg);
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, nullptr, &issue, ctx);
            return Die(ctx);
        }

        if (request.ErrorCount > 0) {
            InterpretResolveError(&request, ctx);
            return Die(ctx);
        }

        const auto& entry = request.ResultSet.front();

        DatabaseOwner = entry.Self->Info.GetOwner();
        DatabaseSecurityState = entry.DomainDescription->Description.GetSecurityState();

        IsDatabaseAdministrator = NKikimr::IsDatabaseAdministrator(&UserToken.value(), entry.Self->Info.GetOwner());

        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " HandleResolveDatabase,"
            << " UserSID: " << GetUserSID(UserToken)
            << " CheckAdministrator: " << CheckAdministrator
            << " CheckDatabaseAdministrator: " << CheckDatabaseAdministrator
            << " IsClusterAdministrator: " << IsClusterAdministrator
            << " IsDatabaseAdministrator: " << IsDatabaseAdministrator
            << " DatabaseOwner: " << DatabaseOwner
        );

        static_cast<TDerived*>(this)->Start(ctx);
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

        // Check user access level, permissions on scheme objects and other restrictions/permissions
        if (UserToken) {
            if (!CheckAccess(navigate->ResultSet, ctx)) {
                return Die(ctx);
            }
        }

        // Check doc-api restrictions on operations
        if (IsDocApiRestricted(SchemeRequest->Ev->Get()->Record)) {
            if (!CheckDocApi(navigate->ResultSet, ctx)) {
                return Die(ctx);
            }
        }

        SchemeshardIdToRequest = GetShardToRequest(*navigate->ResultSet.begin(), *ResolveForACL.begin());

        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                    "Actor# " << ctx.SelfID.ToString()
                              << " HANDLE EvNavigateKeySetResult,"
                              << " txid# " << TxId
                              << " shardToRequest# " << SchemeshardIdToRequest
                              << " DomainKey# " << navigate->ResultSet.begin()->DomainInfo->DomainKey
                              << " DomainInfo.Params# " << navigate->ResultSet.begin()->DomainInfo->Params.ShortDebugString()
                              << " RedirectRequired# " <<  (navigate->ResultSet.begin()->RedirectRequired ? "true" : "false"));

        // TSchemeTransactionalReq can't contain AlterLogin operations since it's used only for RenameTables requests
        if (GetRequestEv().HasModifyScheme()
            && GetModifyScheme().GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterLogin) {
            auto& alterLogin = *GetModifyScheme().MutableAlterLogin();
            switch (alterLogin.GetAlterCase()) {
            case NKikimrSchemeOp::TAlterLogin::kCreateUser:
            {
                auto& targetUser = *alterLogin.MutableCreateUser();
                if (targetUser.GetHashedPassword()) {
                    // to support compatibility between old and new hash formats
                    // TODO: remove after the end of old format support in local backups
                    auto hashes = NLogin::ConvertHashes(targetUser.GetHashedPassword());
                    if (hashes) {
                        targetUser.SetPassword(std::move(hashes->OldHashFormat));
                        targetUser.SetIsHashedPassword(true);
                        targetUser.SetHashedPassword(std::move(hashes->NewHashFormat));
                    } else {
                        auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                            "Unsupported format of hashed password");
                        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest, nullptr, &issue, ctx);
                        return Die(ctx);
                    }
                } else {
                    RunPasswordHasher(ctx, targetUser.GetUser(), targetUser.GetPassword());
                    return;
                }
                break;
            }
            case NKikimrSchemeOp::TAlterLogin::kModifyUser:
            {
                auto& targetUser = *alterLogin.MutableModifyUser();
                if (targetUser.HasHashedPassword()) {
                    // to support compatibility between old and new hash formats
                    // TODO: remove after the end of old format support in local backups
                    auto hashes = NLogin::ConvertHashes(targetUser.GetHashedPassword());
                    if (hashes) {
                        targetUser.SetPassword(std::move(hashes->OldHashFormat));
                        targetUser.SetIsHashedPassword(true);
                        targetUser.SetHashedPassword(std::move(hashes->NewHashFormat));
                    } else {
                        auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                            "Unsupported format of hashed password");
                        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest, nullptr, &issue, ctx);
                        return Die(ctx);
                    }
                } else if (targetUser.HasPassword()) {
                    RunPasswordHasher(ctx, targetUser.GetUser(), targetUser.GetPassword());
                    return;
                }
                break;
            }
            default:
                break;
            }
        }

        auto request = MakePropose(SchemeshardIdToRequest);
        SendPropose(request.Release(), SchemeshardIdToRequest, ctx);
        static_cast<TDerived*>(this)->Become(&TDerived::StateWaitPrepare);
    }

    void Handle(NSasl::TEvSasl::TEvComputedHashes::TPtr &ev, const TActorContext &ctx) {
        auto* computedHashes = ev->Get();
        if (!computedHashes->Error.empty()) {
            auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, std::move(computedHashes->Error));
        }

        auto& alterLogin = *GetModifyScheme().MutableAlterLogin();
        switch (alterLogin.GetAlterCase()) {
        case NKikimrSchemeOp::TAlterLogin::kCreateUser:
        {
            auto& targetUser = *alterLogin.MutableCreateUser();
            targetUser.SetUser(std::move(computedHashes->PreparedUsername));
            targetUser.SetPassword(std::move(computedHashes->ArgonHash));
            targetUser.SetIsHashedPassword(true);
            targetUser.SetHashedPassword(std::move(computedHashes->Hashes));
            break;
        }
        case NKikimrSchemeOp::TAlterLogin::kModifyUser:
        {
            auto& targetUser = *alterLogin.MutableModifyUser();
            targetUser.SetUser(std::move(computedHashes->PreparedUsername));
            targetUser.SetPassword(std::move(computedHashes->ArgonHash));
            targetUser.SetIsHashedPassword(true);
            targetUser.SetHashedPassword(std::move(computedHashes->Hashes));
            break;
        }
        default:
            break;
        }

        auto request = MakePropose(SchemeshardIdToRequest);
        SendPropose(request.Release(), SchemeshardIdToRequest, ctx);
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
    void Start(const TActorContext &ctx);
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

    STFUNC(StateWaitResolveDatabase) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleResolveDatabase);
        }
    }

    STFUNC(StateWaitResolveWorkingDir) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleWorkingDir);
        }
    }

    STFUNC(StateWaitResolve) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            HFunc(NSasl::TEvSasl::TEvComputedHashes, Handle);
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
}

void TFlatSchemeReq::Start(const TActorContext &ctx) {
    //NOTE: split-merge operations here bypass access checks:
    // - internal requests should not follow general rules
    // - external requests are checked for admin rights elsewhere
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
        auto resolveRequest = ResolveRequestForAdjustPathNames(GetRequestProto().GetDatabaseName(), GetModifyScheme());
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
    bool lookupError = false;
    for (auto it = resultSet.rbegin(); it != resultSet.rend(); ++it) {
        if (it->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            workingDir = &it->Path;
            break;
        } else if (it->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError) {
            lookupError = true;
        }
    }

    auto parts = GetFullPath(GetModifyScheme());
    if (!workingDir && lookupError) {
        const auto errText = TStringBuilder()
            << "Cannot resolve working dir, lookup error"
            << " path# " << JoinPath(parts);
        LOG_INFO_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << ", " << errText
        );
        TxProxyMon->ResolveKeySetFail->Inc();
        const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::RESOLVE_LOOKUP_ERROR, errText);
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable, nullptr, &issue, ctx);
        return Die(ctx);
    }

    if (!workingDir || workingDir->size() >= parts.size()) {
        const TString errText = TStringBuilder()
            << "Cannot resolve working dir"
            << " workingDir# " << (workingDir ? CanonizePath(JoinPath(*workingDir)) : "null")
            << " path# " << JoinPath(parts);
        LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << ", " << errText
        );

        TxProxyMon->ResolveKeySetWrongRequest->Inc();
        const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, errText);
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, nullptr, &issue, ctx);
        return Die(ctx);
    }

    GetModifyScheme().SetWorkingDir(CombinePath(workingDir->begin(), workingDir->end()));
    SetPathNameForScheme(GetModifyScheme(), CombinePath(parts.begin() + workingDir->size(), parts.end(), false));

    ProcessRequest(ctx);
}

//////////////////////////////////////////////////////////////
/// \brief The TFlatSchemeTransactionalReq struct
/// struct for Trasaction with several ModifySchema
///
struct TSchemeTransactionalReq : public TBaseSchemeReq<TSchemeTransactionalReq> {
    using TBase = TBaseSchemeReq<TSchemeTransactionalReq>;

    void Bootstrap(const TActorContext &ctx);
    void Start(const TActorContext &ctx);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_PROXY_SCHEMEREQ;
    }

    TSchemeTransactionalReq(const TTxProxyServices &services, ui64 txid, TAutoPtr<TEvTxProxyReq::TEvSchemeRequest> request, const TIntrusivePtr<TTxProxyMon> &txProxyMon)
        : TBase(services, txid, request, txProxyMon)
    {}

    void Die(const TActorContext &ctx) override {
        TBase::Die(ctx);
    }

    STFUNC(StateWaitResolveDatabase) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleResolveDatabase);
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
}

void TSchemeTransactionalReq::Start(const TActorContext &ctx) {
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
