#include "kqp_executer.h"
#include "kqp_executer_impl.h"

#include <ydb/core/kqp/gateway/actors/analyze_actor.h>
#include <ydb/core/kqp/gateway/actors/scheme.h>
#include <ydb/core/kqp/gateway/local_rpc/helper.h>
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <ydb/core/kqp/session_actor/kqp_worker_common.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_build_index.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/services/metadata/abstract/kqp_common.h>


namespace NKikimr::NKqp {

using namespace NThreading;

namespace {

bool CheckAlterAccess(const NACLib::TUserToken& userToken, const NSchemeCache::TSchemeCacheNavigate* navigate) {
    bool isDatabaseEntry = true; // first entry is always database

    using TEntry = NSchemeCache::TSchemeCacheNavigate::TEntry;

    for (const TEntry& entry : navigate->ResultSet) {
        if (!entry.SecurityObject) {
            continue;
        }
        if (isDatabaseEntry) { // first entry is always database
            isDatabaseEntry = false;
            continue;
        }

        const ui32 access = NACLib::AlterSchema | NACLib::DescribeSchema;
        if (!entry.SecurityObject->CheckAccess(access, userToken)) {
            return false;
        }
    }

    return true;
}

class TKqpSchemeExecuter : public TActorBootstrapped<TKqpSchemeExecuter> {
    struct TEvPrivate {
        enum EEv {
            EvResult = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvMakeTempDirResult,
            EvMakeSessionDirResult,
            EvMakeCTASDirResult,
        };

        struct TEvResult : public TEventLocal<TEvResult, EEv::EvResult> {
            IKqpGateway::TGenericResult Result;
        };

        struct TEvMakeTempDirResult : public TEventLocal<TEvMakeTempDirResult, EEv::EvMakeTempDirResult> {
            IKqpGateway::TGenericResult Result;
        };

        struct TEvMakeSessionDirResult : public TEventLocal<TEvMakeSessionDirResult, EEv::EvMakeSessionDirResult> {
            IKqpGateway::TGenericResult Result;
        };

        struct TEvMakeCTASDirResult : public TEventLocal<TEvMakeCTASDirResult, EEv::EvMakeCTASDirResult> {
            IKqpGateway::TGenericResult Result;
        };
    };
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_EXECUTER_ACTOR;
    }

    TKqpSchemeExecuter(
        TKqpPhyTxHolder::TConstPtr phyTx, NKikimrKqp::EQueryType queryType, const TActorId& target, const TMaybe<TString>& requestType,
        const TString& database, TIntrusiveConstPtr<NACLib::TUserToken> userToken, const TString& clientAddress,
        bool temporary, bool createTmpDir, bool isCreateTableAs, TString tempDirName, TIntrusivePtr<TUserRequestContext> ctx,
        const TActorId& kqpTempTablesAgentActor)
        : PhyTx(phyTx)
        , QueryType(queryType)
        , Target(target)
        , Database(database)
        , UserToken(userToken)
        , ClientAddress(clientAddress)
        , Temporary(temporary)
        , CreateTmpDir(createTmpDir)
        , IsCreateTableAs(isCreateTableAs)
        , TempDirName(tempDirName)
        , RequestContext(std::move(ctx))
        , RequestType(requestType)
        , KqpTempTablesAgentActor(kqpTempTablesAgentActor)
    {
        YQL_ENSURE(RequestContext);
        YQL_ENSURE(PhyTx);
        YQL_ENSURE(PhyTx->GetType() == NKqpProto::TKqpPhyTx::TYPE_SCHEME);

        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>(
            nullptr,
            TEvKqpExecuter::TEvTxResponse::EExecutionType::Scheme);
    }

    void StartBuildOperation() {
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
        Become(&TKqpSchemeExecuter::ExecuteState);
    }

    void CreateTmpDirectory() {
        auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        auto& record = ev->Record;

        record.SetDatabaseName(Database);
        record.SetUserToken(NACLib::TSystemUsers::Tmp().SerializeAsString());
        record.SetPeerName(ClientAddress);

        auto* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(GetTmpDirPath(Database));
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMkDir);
        modifyScheme->SetAllowCreateInTempDir(false);
        modifyScheme->SetInternal(true);

        auto* makeDir = modifyScheme->MutableMkDir();
        makeDir->SetName(GetSessionDirName());

        NACLib::TDiffACL diffAcl;
        diffAcl.SetInterruptInheritance(true);
        auto* modifyAcl = modifyScheme->MutableModifyACL();
        modifyAcl->SetDiffACL(diffAcl.SerializeAsString());

        auto promise = NewPromise<IKqpGateway::TGenericResult>();
        IActor* requestHandler = new TSchemeOpRequestHandler(ev.Release(), promise, false);
        RegisterWithSameMailbox(requestHandler);

        auto actorSystem = TActivationContext::ActorSystem();
        auto selfId = SelfId();
        promise.GetFuture().Subscribe([actorSystem, selfId](const TFuture<IKqpGateway::TGenericResult>& future) {
            auto ev = MakeHolder<TEvPrivate::TEvMakeTempDirResult>();
            ev->Result = future.GetValue();
            actorSystem->Send(selfId, ev.Release());
        });
        Become(&TKqpSchemeExecuter::ExecuteState);
    }

    void CreateSessionDirectory() {
        auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        auto& record = ev->Record;

        record.SetDatabaseName(Database);
        record.SetUserToken(NACLib::TSystemUsers::Tmp().SerializeAsString());
        record.SetPeerName(ClientAddress);

        auto* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(GetSessionDirsBasePath(Database));
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMkDir);
        modifyScheme->SetAllowCreateInTempDir(false);
        modifyScheme->SetFailOnExist(true);

        auto* makeDir = modifyScheme->MutableMkDir();
        makeDir->SetName(TempDirName);
        ActorIdToProto(KqpTempTablesAgentActor, modifyScheme->MutableTempDirOwnerActorId());

        if (UserToken) {
            constexpr ui32 access = NACLib::EAccessRights::CreateDirectory
                | NACLib::EAccessRights::CreateTable
                | NACLib::EAccessRights::RemoveSchema
                | NACLib::EAccessRights::DescribeSchema;

            NACLib::TDiffACL diffAcl;
            diffAcl.AddAccess(
                NACLib::EAccessType::Allow,
                access,
                UserToken->GetUserSID());
            diffAcl.SetInterruptInheritance(true);
            auto* modifyAcl = modifyScheme->MutableModifyACL();
            modifyAcl->SetDiffACL(diffAcl.SerializeAsString());
        }

        auto promise = NewPromise<IKqpGateway::TGenericResult>();
        IActor* requestHandler = new TSchemeOpRequestHandler(ev.Release(), promise, false);
        RegisterWithSameMailbox(requestHandler);

        auto actorSystem = TlsActivationContext->ActorSystem();
        auto selfId = SelfId();
        promise.GetFuture().Subscribe([actorSystem, selfId](const TFuture<IKqpGateway::TGenericResult>& future) {
            auto ev = MakeHolder<TEvPrivate::TEvMakeSessionDirResult>();
            ev->Result = future.GetValue();
            actorSystem->Send(selfId, ev.Release());
        });
    }

    void FindWorkingDirForCTAS() {
        const auto& schemeOp = PhyTx->GetSchemeOperation();

        AFL_ENSURE(schemeOp.GetOperationCase() == NKqpProto::TKqpSchemeOperation::kAlterTable);
        const auto& alterTableModifyScheme = schemeOp.GetAlterTable();
        AFL_ENSURE(alterTableModifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpMoveTable);

        const auto dirPath = SplitPath(alterTableModifyScheme.GetMoveTable().GetDstPath());
        AFL_ENSURE(dirPath.size() >= 2);

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        TVector<TString> path;

        for (const auto& part : dirPath) {
            path.emplace_back(part);

            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Path = path;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
            entry.SyncVersion = true;
            entry.RedirectRequired = false;
            request->ResultSet.emplace_back(entry);
        }
        request->ResultSet.pop_back();

        auto ev = std::make_unique<TEvTxProxySchemeCache::TEvNavigateKeySet>(request);

        Send(MakeSchemeCacheID(), ev.release());
        Become(&TKqpSchemeExecuter::ExecuteState);
    }

    void HandleCTASWorkingDir(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& resultSet = ev->Get()->Request->ResultSet;

        const TVector<TString>* workingDir = nullptr;
        for (auto it = resultSet.rbegin(); it != resultSet.rend(); ++it) {
            if (it->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                workingDir = &it->Path;
                break;
            }
        }

        const auto& schemeOp = PhyTx->GetSchemeOperation();
        AFL_ENSURE(schemeOp.GetOperationCase() == NKqpProto::TKqpSchemeOperation::kAlterTable);
        const auto& alterTableModifyScheme = schemeOp.GetAlterTable();
        AFL_ENSURE(alterTableModifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpMoveTable);
        const auto dirPath = SplitPath(alterTableModifyScheme.GetMoveTable().GetDstPath());

        if (!workingDir) {
            const auto errText = TStringBuilder()
                << "Cannot resolve working dir."
                << " path# " << JoinPath(dirPath);
            KQP_STLOG_D(KQPSCHEME, errText);

            const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::RESOLVE_LOOKUP_ERROR, errText);
            return ReplyErrorAndDie(Ydb::StatusIds::BAD_REQUEST, issue);
        }
        AFL_ENSURE(!workingDir->empty() && workingDir->size() < dirPath.size());
        AFL_ENSURE(std::equal(
            workingDir->begin(),
            workingDir->end(),
            dirPath.begin(),
            dirPath.begin() + workingDir->size()));

        CreateCTASDirectory(
            TConstArrayRef<TString>(
                workingDir->begin(),
                workingDir->end()),
            TConstArrayRef<TString>(
                dirPath.begin() + workingDir->size(),
                dirPath.end()));
    }

    void CreateCTASDirectory(const TConstArrayRef<TString> workingDir, const TConstArrayRef<TString> dirPath) {
        AFL_ENSURE(!dirPath.empty());
        AFL_ENSURE(!workingDir.empty());

        auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        auto& record = ev->Record;

        auto actorSystem = TActivationContext::ActorSystem();
        auto selfId = SelfId();

        record.SetDatabaseName(Database);
        if (UserToken) {
            record.SetUserToken(UserToken->GetSerializedToken());
        }
        record.SetPeerName(ClientAddress);

        const auto& schemeOp = PhyTx->GetSchemeOperation();

        AFL_ENSURE(schemeOp.GetOperationCase() == NKqpProto::TKqpSchemeOperation::kAlterTable);
        const auto& alterTableModifyScheme = schemeOp.GetAlterTable();
        AFL_ENSURE(alterTableModifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpMoveTable);

        if (dirPath.size() == 1) {
            auto ev = MakeHolder<TEvPrivate::TEvMakeCTASDirResult>();
            ev->Result.SetSuccess();
            actorSystem->Send(selfId, ev.Release());
            Become(&TKqpSchemeExecuter::ExecuteState);
            return;
        }

        auto* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(CombinePath(workingDir.begin(), workingDir.end()));
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMkDir);

        auto* makeDir = modifyScheme->MutableMkDir();
        makeDir->SetName(CombinePath(dirPath.begin(), std::prev(dirPath.end()), false));

        auto promise = NewPromise<IKqpGateway::TGenericResult>();
        IActor* requestHandler = new TSchemeOpRequestHandler(ev.Release(), promise, false);
        RegisterWithSameMailbox(requestHandler);

        promise.GetFuture().Subscribe([actorSystem, selfId](const TFuture<IKqpGateway::TGenericResult>& future) {
            auto ev = MakeHolder<TEvPrivate::TEvMakeCTASDirResult>();
            ev->Result = future.GetValue();
            actorSystem->Send(selfId, ev.Release());
        });
        Become(&TKqpSchemeExecuter::ExecuteState);
    }

    void MakeSchemeOperationRequest() {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        auto ev = MakeHolder<TRequest>();
        ev->Record.SetDatabaseName(Database);
        if (UserToken) {
            ev->Record.SetUserToken(UserToken->GetSerializedToken());
        }
        ev->Record.SetPeerName(ClientAddress);

        if (RequestType) {
            ev->Record.SetRequestType(*RequestType);
        }

        const auto& schemeOp = PhyTx->GetSchemeOperation();
        switch (schemeOp.GetOperationCase()) {
            case NKqpProto::TKqpSchemeOperation::kCreateTable: {
                auto modifyScheme = schemeOp.GetCreateTable();
                AFL_ENSURE(!IsCreateTableAs || Temporary);
                if (Temporary) {
                    auto changePath = [this](NKikimrSchemeOp::TTableDescription* tableDesc) {
                        const auto fullPath = JoinPath({tableDesc->GetPath(), tableDesc->GetName()});
                        YQL_ENSURE(fullPath.size() > 1);
                        tableDesc->SetName(GetCreateTempTablePath(Database, TempDirName, fullPath));
                        tableDesc->SetPath(Database);
                    };

                    switch (modifyScheme.GetOperationType()) {
                        case NKikimrSchemeOp::ESchemeOpCreateTable: {
                            changePath(modifyScheme.MutableCreateTable());
                            break;
                        }
                        case NKikimrSchemeOp::ESchemeOpCreateIndexedTable: {
                            changePath(modifyScheme.MutableCreateIndexedTable()->MutableTableDescription());
                            break;
                        }
                        case NKikimrSchemeOp::ESchemeOpCreateColumnTable: {
                            modifyScheme.MutableCreateColumnTable()->SetName(
                                GetCreateTempTablePath(Database, TempDirName, modifyScheme.GetCreateColumnTable().GetName()));
                            break;
                        }
                        default:
                            YQL_ENSURE(false, "Unexpected operation type");
                    }

                    modifyScheme.SetAllowCreateInTempDir(true);
                }
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropTable: {
                const auto& modifyScheme = schemeOp.GetDropTable();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterTable: {
                const auto& modifyScheme = schemeOp.GetAlterTable();
                AFL_ENSURE(!IsCreateTableAs || modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpMoveTable);
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kBuildOperation: {
                return StartBuildOperation();
            }

            case NKqpProto::TKqpSchemeOperation::kCreateUser: {
                const auto& modifyScheme = schemeOp.GetCreateUser();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                ev->Record.SetDatabaseName(NSchemeHelpers::SelectDatabaseForAlterLoginOperations(AppData(), Database));
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterUser: {
                const auto& modifyScheme = schemeOp.GetAlterUser();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                ev->Record.SetDatabaseName(NSchemeHelpers::SelectDatabaseForAlterLoginOperations(AppData(), Database));
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropUser: {
                const auto& modifyScheme = schemeOp.GetDropUser();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                ev->Record.SetDatabaseName(NSchemeHelpers::SelectDatabaseForAlterLoginOperations(AppData(), Database));
                break;
            }
            case NKqpProto::TKqpSchemeOperation::kCreateExternalTable: {
                const auto& modifyScheme = schemeOp.GetCreateExternalTable();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }
            case NKqpProto::TKqpSchemeOperation::kAlterExternalTable: {
                const auto& modifyScheme = schemeOp.GetAlterExternalTable();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }
            case NKqpProto::TKqpSchemeOperation::kDropExternalTable: {
                const auto& modifyScheme = schemeOp.GetDropExternalTable();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kCreateGroup: {
                const auto& modifyScheme = schemeOp.GetCreateGroup();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                ev->Record.SetDatabaseName(NSchemeHelpers::SelectDatabaseForAlterLoginOperations(AppData(), Database));
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAddGroupMembership: {
                const auto& modifyScheme = schemeOp.GetAddGroupMembership();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                ev->Record.SetDatabaseName(NSchemeHelpers::SelectDatabaseForAlterLoginOperations(AppData(), Database));
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kRemoveGroupMembership: {
                const auto& modifyScheme = schemeOp.GetRemoveGroupMembership();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                ev->Record.SetDatabaseName(NSchemeHelpers::SelectDatabaseForAlterLoginOperations(AppData(), Database));
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kRenameGroup: {
                const auto& modifyScheme = schemeOp.GetRenameGroup();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                ev->Record.SetDatabaseName(NSchemeHelpers::SelectDatabaseForAlterLoginOperations(AppData(), Database));
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropGroup: {
                const auto& modifyScheme = schemeOp.GetDropGroup();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                ev->Record.SetDatabaseName(NSchemeHelpers::SelectDatabaseForAlterLoginOperations(AppData(), Database));
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kModifyPermissions: {
                const auto& modifyScheme = schemeOp.GetModifyPermissions();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kCreateColumnTable: {
                const auto& modifyScheme = schemeOp.GetCreateColumnTable();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterColumnTable: {
                const auto& modifyScheme = schemeOp.GetAlterColumnTable();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kCreateTableStore: {
                const auto& modifyScheme = schemeOp.GetCreateTableStore();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterTableStore: {
                const auto& modifyScheme = schemeOp.GetAlterTableStore();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropTableStore: {
                const auto& modifyScheme = schemeOp.GetDropTableStore();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kCreateSequence: {
                const auto& modifyScheme = schemeOp.GetCreateSequence();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropSequence: {
                const auto& modifyScheme = schemeOp.GetDropSequence();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kCreateReplication: {
                const auto& modifyScheme = schemeOp.GetCreateReplication();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterReplication: {
                const auto& modifyScheme = schemeOp.GetAlterReplication();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropReplication: {
                const auto& modifyScheme = schemeOp.GetDropReplication();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kCreateTransfer: {
                const auto& modifyScheme = schemeOp.GetCreateTransfer();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterTransfer: {
                const auto& modifyScheme = schemeOp.GetAlterTransfer();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropTransfer: {
                const auto& modifyScheme = schemeOp.GetDropTransfer();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterSequence: {
                const auto& modifyScheme = schemeOp.GetAlterSequence();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kCreateTopic: {
                const auto& modifyScheme = schemeOp.GetCreateTopic();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterTopic: {
                const auto& modifyScheme = schemeOp.GetAlterTopic();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropTopic: {
                const auto& modifyScheme = schemeOp.GetDropTopic();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAnalyzeTable: {
                const auto& analyzeOperation = schemeOp.GetAnalyzeTable();

                auto analyzePromise = NewPromise<IKqpGateway::TGenericResult>();

                TVector<TString> columns{analyzeOperation.columns().begin(), analyzeOperation.columns().end()};
                IActor* analyzeActor = new TAnalyzeActor(Database, analyzeOperation.GetTablePath(), columns, analyzePromise);

                auto actorSystem = TActivationContext::ActorSystem();
                AnalyzeActorId = RegisterWithSameMailbox(analyzeActor);

                auto selfId = SelfId();
                analyzePromise.GetFuture().Subscribe([actorSystem, selfId](const TFuture<IKqpGateway::TGenericResult>& future) {
                    auto ev = MakeHolder<TEvPrivate::TEvResult>();
                    ev->Result = future.GetValue();

                    actorSystem->Send(selfId, ev.Release());
                });

                Become(&TKqpSchemeExecuter::ExecuteState);
                return;

            }

            case NKqpProto::TKqpSchemeOperation::kCreateBackupCollection: {
                const auto& modifyScheme = schemeOp.GetCreateBackupCollection();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterBackupCollection: {
                const auto& modifyScheme = schemeOp.GetAlterBackupCollection();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropBackupCollection: {
                const auto& modifyScheme = schemeOp.GetDropBackupCollection();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kBackup: {
                const auto& modifyScheme = schemeOp.GetBackup();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kBackupIncremental: {
                const auto& modifyScheme = schemeOp.GetBackupIncremental();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kRestore: {
                const auto& modifyScheme = schemeOp.GetRestore();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterDatabase: {
                const auto& modifyScheme = schemeOp.GetAlterDatabase();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kCreateSecret: {
                const auto& modifyScheme = schemeOp.GetCreateSecret();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kAlterSecret: {
                const auto& modifyScheme = schemeOp.GetAlterSecret();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kDropSecret: {
                const auto& modifyScheme = schemeOp.GetDropSecret();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            case NKqpProto::TKqpSchemeOperation::kTruncateTable: {
                const auto& modifyScheme = schemeOp.GetTruncateTable();
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(modifyScheme);
                break;
            }

            default:
                InternalError(TStringBuilder() << "Unexpected scheme operation: "
                    << (ui32) schemeOp.GetOperationCase());
                return;
        }

        auto promise = NewPromise<IKqpGateway::TGenericResult>();

        bool successOnNotExist = false;
        bool failedOnAlreadyExists = false;
        // exists/not exists semantics supported only in the query service.
        if (IsQueryService()) {
            successOnNotExist = ev->Record.GetTransaction().GetModifyScheme().GetSuccessOnNotExist();
            failedOnAlreadyExists = ev->Record.GetTransaction().GetModifyScheme().GetFailedOnAlreadyExists();
        }

        const auto operationType = ev->Record.GetTransaction().GetModifyScheme().GetOperationType();

        IActor* requestHandler = new TSchemeOpRequestHandler(
            ev.Release(),
            promise,
            failedOnAlreadyExists,
            successOnNotExist
        );
        RegisterWithSameMailbox(requestHandler);

        auto actorSystem = TActivationContext::ActorSystem();
        auto selfId = SelfId();
        promise.GetFuture().Subscribe([actorSystem, selfId, operationType](const TFuture<IKqpGateway::TGenericResult>& future) {
            const auto& value = future.GetValue();
            auto ev = MakeHolder<TEvPrivate::TEvResult>();
            ev->Result.SetStatus(value.Status());

            if (value.Issues()) {
                NYql::TIssue rootIssue(TStringBuilder() << "Executing " << NKikimrSchemeOp::EOperationType_Name(operationType));
                rootIssue.SetCode(ev->Result.Status(), NYql::TSeverityIds::S_INFO);
                for (const auto& issue : value.Issues()) {
                    rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
                }
                ev->Result.AddIssue(rootIssue);
            }

            actorSystem->Send(selfId, ev.Release());
        });

        Become(&TKqpSchemeExecuter::ExecuteState);
    }

    void MakeObjectRequest() {
        const auto& schemeOp = PhyTx->GetSchemeOperation();
        NMetadata::IClassBehaviour::TPtr cBehaviour(NMetadata::IClassBehaviour::TFactory::Construct(schemeOp.GetObjectType()));
        if (!cBehaviour) {
            InternalError(TStringBuilder() << "Unsupported object type: \"" << schemeOp.GetObjectType() << "\"");
            return;
        }

        if (!cBehaviour->GetOperationsManager()) {
            InternalError(TStringBuilder() << "Object type \"" << schemeOp.GetObjectType() << "\" does not have manager for operations");
        }

        auto* actorSystem = TActivationContext::ActorSystem();
        auto selfId = SelfId();

        NMetadata::NModifications::IOperationsManager::TExternalModificationContext context;
        context.SetDatabase(Database);
        context.SetDatabaseId(RequestContext->DatabaseId);
        context.SetActorSystem(actorSystem);
        if (UserToken) {
            context.SetUserToken(*UserToken);
        }

        auto resultFuture = cBehaviour->GetOperationsManager()->ExecutePrepared(schemeOp, SelfId().NodeId(), cBehaviour, context);

        using TResultFuture = NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus>;
        resultFuture.Subscribe([actorSystem, selfId, objectType = schemeOp.GetObjectType()](const TResultFuture& f) {
            const auto& status = f.GetValue();
            auto ev = MakeHolder<TEvPrivate::TEvResult>();
            if (status.Ok()) {
                ev->Result.SetSuccess();
            } else {
                ev->Result.SetStatus(status.GetStatus());
                if (TString message = status.GetErrorMessage()) {
                    const auto createIssue = [status = status.GetStatus()](const TString& message) {
                        return NYql::TIssue(message).SetCode(status, NYql::TSeverityIds::S_ERROR);
                    };
                    ev->Result.AddIssue(createIssue(TStringBuilder() << "Executing operation with object \"" << objectType << "\"")
                        .AddSubIssue(MakeIntrusive<NYql::TIssue>(createIssue(status.GetErrorMessage())))
                    );
                }
            }
            actorSystem->Send(selfId, ev.Release());
        });

        Become(&TKqpSchemeExecuter::ObjectExecuteState);
    }

    void Bootstrap() {
        const auto& schemeOp = PhyTx->GetSchemeOperation();
        if (schemeOp.GetObjectType()) {
            MakeObjectRequest();
        } else if (IsCreateTableAs && schemeOp.GetOperationCase() == NKqpProto::TKqpSchemeOperation::kAlterTable) {
            FindWorkingDirForCTAS();
        } else {
            if (CreateTmpDir) {
                AFL_ENSURE(Temporary);
                CreateTmpDirectory();
            } else {
                MakeSchemeOperationRequest();
            }
        }
    }

public:
    STATEFN(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPrivate::TEvResult, HandleExecute);
                hFunc(TEvPrivate::TEvMakeTempDirResult, Handle);
                hFunc(TEvPrivate::TEvMakeSessionDirResult, Handle);
                hFunc(TEvPrivate::TEvMakeCTASDirResult, Handle);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
                hFunc(NSchemeShard::TEvIndexBuilder::TEvCreateResponse, Handle);
                hFunc(TEvTabletPipe::TEvClientConnected, Handle);
                hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
                hFunc(NSchemeShard::TEvIndexBuilder::TEvGetResponse, Handle);
                hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
                hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
                default:
                    UnexpectedEvent("ExecuteState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            InternalError(e.what());
        }
    }

    STATEFN(ObjectExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPrivate::TEvResult, HandleExecute);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                default:
                    UnexpectedEvent("ObjectExecuteState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            InternalError(e.what());
        }
    }

    void Handle(TEvPrivate::TEvMakeTempDirResult::TPtr& result) {
        if (!result->Get()->Result.Success()) {
            InternalError(TStringBuilder()
                << "Error creating temporary directory: "
                << result->Get()->Result.Issues().ToString(true));
        }

        CreateSessionDirectory();
    }

    void Handle(TEvPrivate::TEvMakeSessionDirResult::TPtr& result) {
        if (!result->Get()->Result.Success()) {
            InternalError(TStringBuilder()
                << "Error creating directory for session " << TempDirName
                << ": " << result->Get()->Result.Issues().ToString(true));
        }
        MakeSchemeOperationRequest();
    }

    void Handle(TEvPrivate::TEvMakeCTASDirResult::TPtr& result) {
        if (!result->Get()->Result.Success()) {
            InternalError(TStringBuilder()
                << "Error creating directory:"
                << result->Get()->Result.Issues().ToString(true));
        }
        MakeSchemeOperationRequest();
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        const auto* msg = ev->Get();
        TxId = msg->TxId;

        Navigate(msg->Services.SchemeCache);
    }

    void Navigate(const TActorId& schemeCache) {
        const auto& schemeOp = PhyTx->GetSchemeOperation();
        const auto& buildOp = schemeOp.GetBuildOperation();
        const auto& path = buildOp.source_path();

        const auto paths = NKikimr::SplitPath(path);
        if (paths.empty()) {
            TString error = TStringBuilder() << "Failed to split table path " << path;
            return ReplyErrorAndDie(Ydb::StatusIds::BAD_REQUEST, NYql::TIssue(error));
        }

        auto request = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();

        request->DatabaseName = Database;
        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.Path = ::NKikimr::SplitPath(path);
        entry.RedirectRequired = false;

        {
            auto& entry = request->ResultSet.emplace_back();
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
            entry.Path = paths;
        }

        auto ev = std::make_unique<TEvTxProxySchemeCache::TEvNavigateKeySet>(request.release());
        Send(schemeCache, ev.release());
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&) {
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        KQP_STLOG_D(KQPSCHEME, "Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult",
            (error_count, ev->Get()->Request.Get()->ErrorCount));

        NSchemeCache::TSchemeCacheNavigate* resp = ev->Get()->Request.Get();

        if (IsCreateTableAs) {
            AFL_ENSURE(std::all_of(resp->ResultSet.begin(), resp->ResultSet.end(), [](const auto& entry) {
                return entry.Operation == NSchemeCache::TSchemeCacheNavigate::OpPath;
            }));
            HandleCTASWorkingDir(ev);
            return;
        }

        if (resp->ErrorCount > 0 || resp->ResultSet.empty()) {
            TStringBuilder builder;
            builder << "Unable to navigate:";

            for (const auto& entry : resp->ResultSet) {
                if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                    builder << " " << JoinPath(entry.Path) << " status: " << entry.Status;
                }
            }

            TString error(builder);
            KQP_STLOG_E(KQPSCHEME, error);
            return ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, NYql::TIssue(error));
        }

        AFL_ENSURE(resp->ResultSet.size() <= 2);

        if (UserToken && !UserToken->GetSerializedToken().empty() && !CheckAlterAccess(*UserToken, resp)) {
            KQP_STLOG_E(KQPSCHEME, "Access check failed");
            return ReplyErrorAndDie(Ydb::StatusIds::UNAUTHORIZED, NYql::TIssue("Unauthorized"));
        }

        auto domainInfo = resp->ResultSet.front().DomainInfo;
        if (!domainInfo) {
            KQP_STLOG_E(KQPSCHEME, "Got empty domain info");
            return ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssue("empty domain info"));
        }

        const auto& schemeOp = PhyTx->GetSchemeOperation();
        const auto& buildOp = schemeOp.GetBuildOperation();
        SetSchemeShardId(domainInfo->ExtractSchemeShard());
        auto req = std::make_unique<NSchemeShard::TEvIndexBuilder::TEvCreateRequest>(TxId, Database, buildOp);
        if (UserToken) {
            req->Record.SetUserSID(UserToken->GetUserSID());
        }
        ForwardToSchemeShard(std::move(req));
    }

    void PassAway() override {
        if (SchemePipeActorId_) {
            NTabletPipe::CloseClient(this->SelfId(), SchemePipeActorId_);
        }

        IActor::PassAway();
    }

    const TIntrusivePtr<TUserRequestContext>& GetUserRequestContext() const {
        return RequestContext;
    }

    void Handle(NSchemeShard::TEvIndexBuilder::TEvCreateResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record;
        const auto status = response.GetStatus();
        auto issuesProto = response.GetIssues();

        KQP_STLOG_D(KQPSCHEME, "Handle TEvIndexBuilder::TEvCreateResponse",
            (response, response.ShortUtf8DebugString()));

        if (status == Ydb::StatusIds::SUCCESS) {
            if (response.HasSchemeStatus() && response.GetSchemeStatus() == NKikimrScheme::EStatus::StatusAlreadyExists) {
                return ReplyErrorAndDie(status, &issuesProto);
            } else {
                DoSubscribe();
            }
        } else {
            ReplyErrorAndDie(status, &issuesProto);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                TStringBuilder() << "Tablet not available, status: " << (ui32)ev->Get()->Status));
            return ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, issues);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
            TStringBuilder() << "Connection to tablet was lost."));
        return ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, issues);
    }

    void GetIndexStatus() {
        auto request = std::make_unique<NSchemeShard::TEvIndexBuilder::TEvGetRequest>(Database, TxId);
        ForwardToSchemeShard(std::move(request));
    }

    void DoSubscribe() {
        auto request = std::make_unique<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(TxId);
        ForwardToSchemeShard(std::move(request));
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr&) {
        GetIndexStatus();
    }

    void SetSchemeShardId(ui64 schemeShardTabletId) {
        SchemeShardTabletId = schemeShardTabletId;
    }

    void Handle(NSchemeShard::TEvIndexBuilder::TEvGetResponse::TPtr& ev) {
        auto& record = ev->Get()->Record;
        KQP_STLOG_D(KQPSCHEME, "Handle TEvIndexBuilder::TEvGetResponse",
            (record, record.ShortDebugString()));
        if (record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            // Internal error: we made incorrect request to get status of index build operation
            NYql::TIssues responseIssues;
            NYql::IssuesFromMessage(record.GetIssues(), responseIssues);

            NYql::TIssue issue(TStringBuilder() << "Failed to get index build status. Status: " << record.GetStatus());
            for (const NYql::TIssue& i : responseIssues) {
                issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
            }

            NYql::TIssues issues;
            issues.AddIssue(std::move(issue));
            return InternalError(issues);
        }

        NKikimrIndexBuilder::TIndexBuild& indexBuildResult = *record.MutableIndexBuild();
        const Ydb::Table::IndexBuildState::State state = indexBuildResult.GetState();
        const Ydb::StatusIds::StatusCode buildStatus = state == Ydb::Table::IndexBuildState::STATE_DONE ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::PRECONDITION_FAILED;
        return ReplyErrorAndDie(buildStatus, indexBuildResult.MutableIssues());
    }

    template<typename TEv>
    void ForwardToSchemeShard(std::unique_ptr<TEv>&& ev) {
        if (!SchemePipeActorId_) {
            Y_ABORT_UNLESS(SchemeShardTabletId);
            NTabletPipe::TClientConfig clientConfig;
            clientConfig.RetryPolicy = {.RetryLimitCount = 3};
            SchemePipeActorId_ = RegisterWithSameMailbox(NTabletPipe::CreateClient(SelfId(), SchemeShardTabletId));
        }

        Y_ABORT_UNLESS(SchemePipeActorId_);
        NTabletPipe::SendData(SelfId(), SchemePipeActorId_, ev.release());
    }

    void HandleExecute(TEvPrivate::TEvResult::TPtr& ev) {
        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(GetYdbStatus(ev->Get()->Result));
        IssuesToMessage(ev->Get()->Result.Issues(), response.MutableIssues());

        Send(Target, ResponseEv.release());
        PassAway();
    }

    void HandleAbortExecution(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;
        NYql::TIssues issues = ev->Get()->GetIssues();
        KQP_STLOG_D(KQPSCHEME, "Got EvAbortExecution",
            (status, NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())),
            (issues, issues.ToOneLineString()));

        if (AnalyzeActorId) {
            auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(msg.GetStatusCode(), issues);
            Send(AnalyzeActorId, abortEv.Release());
        }

        ReplyErrorAndDie(NYql::NDq::DqStatusToYdbStatus(msg.GetStatusCode()), issues);
    }

private:

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> protoIssues;
        IssuesToMessage(issues, &protoIssues);
        ReplyErrorAndDie(status, &protoIssues);
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, const NYql::TIssue& issue) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> issues;
        IssueToMessage(issue, issues.Add());
        ReplyErrorAndDie(status, &issues);
    }

    void UnexpectedEvent(const TString& state, ui32 eventType) {
        KQP_STLOG_C(KQPSCHEME, "TKqpSchemeExecuter, unexpected event",
            (event_type, eventType),
            (state, state),
            (self_id, SelfId()));

        InternalError(TStringBuilder() << "Unexpected event at TKqpSchemeExecuter, state: " << state
            << ", event: " << eventType);
    }

    bool IsQueryService() const {

        switch(QueryType) {
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY:
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY:
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
                return true;
            default:
                return false;
        }

    }

    void InternalError(const NYql::TIssues& issues) {
        KQP_STLOG_E(KQPSCHEME, issues.ToOneLineString());
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::UNEXPECTED,
            "Internal error while executing scheme operation.");

        for (const NYql::TIssue& i : issues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, issue);
    }

    void InternalError(const TString& message) {
        InternalError(NYql::TIssues({NYql::TIssue(message)}));
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status,
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* issues)
    {
        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(status);
        response.MutableIssues()->Swap(issues);

        Send(Target, ResponseEv.release());
        PassAway();
    }

private:
    TKqpPhyTxHolder::TConstPtr PhyTx;
    const NKikimrKqp::EQueryType QueryType;
    const TActorId Target;
    const TString Database;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TString ClientAddress;
    std::unique_ptr<TEvKqpExecuter::TEvTxResponse> ResponseEv;
    bool Temporary;
    bool CreateTmpDir;
    bool IsCreateTableAs;
    TString TempDirName;
    ui64 TxId = 0;
    TActorId SchemePipeActorId_;
    ui64 SchemeShardTabletId = 0;
    TIntrusivePtr<TUserRequestContext> RequestContext;
    const TMaybe<TString> RequestType;
    const TActorId KqpTempTablesAgentActor;
    TActorId AnalyzeActorId;
};

} // namespace

IActor* CreateKqpSchemeExecuter(
    TKqpPhyTxHolder::TConstPtr phyTx, NKikimrKqp::EQueryType queryType, const TActorId& target,
    const TMaybe<TString>& requestType, const TString& database,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken, const TString& clientAddress,
    bool temporary, bool createTmpDir, bool isCreateTableAs,
    TString tempDirName, TIntrusivePtr<TUserRequestContext> ctx, const TActorId& kqpTempTablesAgentActor)
{
    return new TKqpSchemeExecuter(
        phyTx, queryType, target, requestType, database, userToken, clientAddress,
        temporary, createTmpDir, isCreateTableAs, tempDirName, std::move(ctx), kqpTempTablesAgentActor);
}

} // namespace NKikimr::NKqp
