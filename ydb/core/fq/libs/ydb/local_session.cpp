#include <ydb/core/fq/libs/ydb/local_session.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/core/fq/libs/ydb/query_actor.h>

#include <ydb/library/table_creator/table_creator.h>
#include <ydb/core/fq/libs/actors/logging/log.h>

namespace NFq {

namespace {

class TTableCreator : public NActors::TActorBootstrapped<TTableCreator> {
public:
    TTableCreator(
        const std::string& db,
        const std::string& path,
        NYdb::NTable::TTableDescription&& tableDesc,
        const NACLib::TDiffACL& acl,
        NThreading::TPromise<NYdb::TStatus> promise)
        : Db(db)
        , Path(path)
        , TableDesc(std::move(tableDesc))
        , Acl(acl)
        , Promise(promise) {
    }

    void Bootstrap() {
        Become(&TTableCreator::StateFunc);

        TVector<TString> keyColumns;
        for (const auto& key : TableDesc.GetPrimaryKeyColumns()) {
            keyColumns.push_back(key.c_str());
        }

        TVector<NKikimrSchemeOp::TColumnDescription> columns;
        for (const auto& column : TableDesc.GetTableColumns()) {
            NKikimrSchemeOp::TColumnDescription desc;
            desc.SetName(column.Name.c_str());
            bool optional = false;
            TString type;
            NYdb::TTypeParser typeParser{column.Type};
            if (typeParser.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
                optional = true;
                typeParser.OpenOptional();
            }
            if (typeParser.GetKind() == NYdb::TTypeParser::ETypeKind::Primitive) {
                type = (TStringBuilder() << typeParser.GetPrimitive());
            } else {
                Y_ABORT("not primitive type %s not suported yet", ToString(typeParser.GetPrimitive()).c_str());
            }
            desc.SetType(type);
            desc.SetNotNull(!optional); 
            columns.push_back(desc);
        }
        Cerr << "CreateTableCreator: Db " << Db << " Path " <<  Path << " " << Endl;
        Register(
            NKikimr::CreateTableCreator(
                NKikimr::SplitPath(Path),
                columns,
                keyColumns,
                NKikimrServices::STREAMS_STORAGE_SERVICE,
                Nothing(),
                {},
                /* isSystemUser */ true,
                Nothing(),
                Acl 
            )
        );
    }

private:
    void Handle(NKikimr::TEvTableCreator::TEvCreateTableResponse::TPtr& ev) {
        if (ev->Get()->Success) {
            Promise.SetValue(NYdb::TStatus(NYdb::EStatus::SUCCESS, {}));
        } else {
            Promise.SetValue(NYdb::TStatus(NYdb::EStatus::INTERNAL_ERROR, NYdb::NAdapters::ToSdkIssues(ev->Get()->Issues)));
        }
        PassAway();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NKikimr::TEvTableCreator::TEvCreateTableResponse, Handle);
    )

private:
    const std::string Db;
    const TString Path;
    const NYdb::NTable::TTableDescription TableDesc;
    NACLib::TDiffACL Acl;
    NThreading::TPromise<NYdb::TStatus> Promise;
};

struct TLocalSession : public ISession { 

    TLocalSession()
        : ActorSystem(NActors::TActivationContext::ActorSystem())
        , QuerySessionId(NActors::TActivationContext::AsActorContext().RegisterWithSameMailbox(MakeQuerySession().release())) {
    }

    ~TLocalSession() {
        ActorSystem->Send(QuerySessionId, new NActors::TEvents::TEvPoison());
    }

    NThreading::TFuture<NYdb::NTable::TDataQueryResult> ExecuteDataQuery(
        const TString& sql,
        NFq::ISession::TTxControl txControl,
        std::shared_ptr<NYdb::TParamsBuilder> params,
        const NYdb::NTable::TExecDataQuerySettings& execDataQuerySettings = {}) override {
        auto promise = NThreading::NewPromise<NYdb::NTable::TDataQueryResult>();
        ActorSystem->Send(QuerySessionId, new TEvQuerySession::TEvExecuteDataQuery(
            sql,
            params,
            TEvQuerySession::TTxControl{txControl.Begin_, txControl.Commit_, txControl.Continue_, txControl.SnapshotRead_},
            execDataQuerySettings,
            promise));

        if (txControl.Begin_) {
            HasTransaction = true;
        } else if (txControl.Commit_) {
            HasTransaction = false;
        }
        return promise.GetFuture();
    }

    NYdb::TAsyncStatus Rollback() override {
        ActorSystem->Send(QuerySessionId, new TEvQuerySession::TEvRollbackTransaction());
        HasTransaction = false;
        return NThreading::MakeFuture(NYdb::TStatus{NYdb::EStatus::SUCCESS, {}});
    }

    NYdb::TAsyncStatus CreateTable(const TString& db, const TString& path, NYdb::NTable::TTableDescription&& tableDesc, const NACLib::TDiffACL& acl) override {
        auto promise = NThreading::NewPromise<NYdb::TStatus>();
        NActors::TActivationContext::Register(new TTableCreator(db, path, std::move(tableDesc), acl, promise));
        return promise.GetFuture();
    }

    NYdb::TAsyncStatus DropTable( const TString& /*path*/) override {
        Y_ABORT("Not implemented");
        return NYdb::TAsyncStatus();
    }

    void UpdateTransaction(std::optional<NYdb::NTable::TTransaction> /*transaction*/) override {
        // nothing
    }
    
    bool HasActiveTransaction() const override {
        return HasTransaction;
    }

private:
    NActors::TActorSystem* ActorSystem = nullptr;
    NActors::TActorId QuerySessionId;
    bool HasTransaction = false;
};

} // namespace

ISession::TPtr CreateLocalSession() {
    return MakeIntrusive<TLocalSession>();
}

} // namespace NFq
