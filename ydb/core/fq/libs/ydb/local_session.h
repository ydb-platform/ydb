#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/core/fq/libs/ydb/session.h>
#include <ydb/core/fq/libs/ydb/query_actor.h>

#include <ydb/library/table_creator/table_creator.h>

namespace NFq {


class TTableCreator : public NActors::TActorBootstrapped<TTableCreator> {
public:
    explicit TTableCreator(const std::string& db, const std::string& path, NYdb::NTable::TTableDescription&& tableDesc,
        NThreading::TPromise<NYdb::TStatus> promise)
        : Db(db)
        , Path(path)
        , TableDesc(std::move(tableDesc))
        , Promise(promise)
    {
        Cerr << "TTableCreator() path = " << path << Endl;
    }

    void Bootstrap() {
        Become(&TTableCreator::StateFunc);

        TVector<TString> keyColumns;
        for (const auto& key : TableDesc.GetPrimaryKeyColumns()) {
            keyColumns.push_back(key.c_str());
        }
        TVector<TString> pathComponents;
        pathComponents.push_back(Path.c_str());

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
              //  Y_ABORT("primitive type %s not suported yet", ToString(typeParser.GetPrimitive()).c_str());
            }
            desc.SetType(type);
            desc.SetNotNull(!optional); 

            columns.push_back(desc);
        }
        Register(
            NKikimr::CreateTableCreator(
                pathComponents,
                //{ ".metadata", "checkpoints", "states" },
                columns,
                keyColumns,
                NKikimrServices::STREAMS_STORAGE_SERVICE,
                Nothing(),
                Db.c_str()
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
    const std::string Path;
    const NYdb::NTable::TTableDescription TableDesc;
    NThreading::TPromise<NYdb::TStatus> Promise;
};

struct TLocalSession : public ISession { 

    TLocalSession() {
      //  QuerySession = MakeQueryActor().release();
        QuerySessionId = NActors::TActivationContext::AsActorContext().RegisterWithSameMailbox(MakeQueryActor().release());
    }

    NThreading::TFuture<NYdb::NTable::TDataQueryResult> ExecuteDataQuery(
        const TString& sql,
        std::shared_ptr<NYdb::TParamsBuilder> params,
        NYdb::NTable::TTxControl txControl,
        NYdb::NTable::TExecDataQuerySettings execDataQuerySettings = NYdb::NTable::TExecDataQuerySettings()) override {
        Cerr << "TLocalSession::ExecuteDataQuery" << Endl;
        //return QueryActor->ExecuteDataQuery(sql, params,txControl);

        auto p = NThreading::NewPromise<NYdb::NTable::TDataQueryResult>();

        NActors::TActivationContext::AsActorContext().Send(QuerySessionId, new TEvQueryActor::TEvExecuteDataQuery(sql, params, txControl, execDataQuerySettings, p));
        return p.GetFuture();
    }

    void Finish(bool needRollback) override {
        // Cerr << "TLocalSession::Finish" << Endl;
        // QueryActor->Finish222(needRollback);
        NActors::TActivationContext::AsActorContext().Send(QuerySessionId, new TEvQueryActor::TEvFinish(needRollback));
    }

    ~TLocalSession() {
        // if (QueryActor) {
        //     Cerr << "TLocalSession::call finish" << Endl;
        //     Finish(false);
        // }
                Cerr << "~TLocalSession" << Endl;

        NActors::TActivationContext::AsActorContext().Send(QuerySessionId, new NActors::TEvents::TEvPoison());
    }

    NYdb::TAsyncStatus CreateTable(const std::string& db, const std::string& path, NYdb::NTable::TTableDescription&& tableDesc) override {
        auto promise = NThreading::NewPromise<NYdb::TStatus>();
        NActors::TActivationContext::Register(new TTableCreator(db, path, std::move(tableDesc), promise));
        return promise.GetFuture();
    }

private: 
  //  TQuerySession* QuerySession = nullptr;
    NActors::TActorId QuerySessionId;
};

} // namespace NFq
