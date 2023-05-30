#include "log_impl.h"
#include "local_pgwire_util.h"
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/pgproxy/pg_proxy_events.h>
#include <ydb/core/pgproxy/pg_proxy_types.h>
#define INCLUDE_YDB_INTERNAL_H
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>


namespace NLocalPgWire {

using namespace NActors;
using namespace NKikimr;

class TPgwireKqpProxy : public TActorBootstrapped<TPgwireKqpProxy> {
    using TBase = TActorBootstrapped<TPgwireKqpProxy>;

    NActors::TActorId RequestActorId_;
    ui64 RequestCookie_;
    bool InTransaction_;
    std::unordered_map<TString, TString> ConnectionParams_;
    TMap<ui32, NYdb::TResultSet> ResultSets_;
public:
    TPgwireKqpProxy(NActors::TActorId actorId, ui64 cookie, std::unordered_map<TString, TString> params)
        : RequestActorId_(actorId)
        , RequestCookie_(cookie)
        , ConnectionParams_(std::move(params))
    {}

    void Bootstrap() {
        Become(&TPgwireKqpProxy::StateWork);
    }

    void Handle(NKqp::TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        NYdb::TResultSet resultSet(std::move(*ev->Get()->Record.MutableResultSet()));
        ResultSets_.emplace(ev->Get()->Record.GetQueryResultIndex(), resultSet);

        BLOG_D(this->SelfId() << "Send stream data ack"
            << ", to: " << ev->Sender);

        auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
        resp->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        resp->Record.SetFreeSpace(std::numeric_limits<ui64>::max());
        Send(ev->Sender, resp.Release());
    }

    void FillResultSet(const NYdb::TResultSet& resultSet, NPG::TEvPGEvents::TEvQueryResponse* response) {
        {
            for (const NYdb::TColumn& column : resultSet.GetColumnsMeta()) {
                // TODO: fill data types and sizes
                response->DataFields.push_back({
                    .Name = column.Name,
                    .DataType = GetPgOidFromYdbType(column.Type),
                    // .DataTypeSize = column.Type.GetProto().Getpg_type().Gettyplen()
                });
            }
        }
        {
            NYdb::TResultSetParser parser(std::move(resultSet));
            while (parser.TryNextRow()) {
                response->DataRows.emplace_back();
                auto& row = response->DataRows.back();
                row.resize(parser.ColumnsCount());
                for (size_t index = 0; index < parser.ColumnsCount(); ++index) {
                    row[index] = ColumnValueToString(parser.ColumnParser(index));
                }
            }
        }
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        BLOG_D("Handling TEvKqp::TEvQueryResponse");
        NKikimrKqp::TEvQueryResponse& record = ev->Get()->Record.GetRef();

        auto response = std::make_unique<NPG::TEvPGEvents::TEvQueryResponse>();
        // HACK
        if (InTransaction_) {
            response->Tag = "BEGIN";
            response->TransactionStatus = 'T';
        }
        try {
            if (record.HasYdbStatus()) {
                if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
                    auto noScript = ConnectionParams_.find("no-script") != ConnectionParams_.end();
                    if (noScript) {
                        Y_ENSURE(record.GetResponse().GetYdbResults().empty());
                        if (!ResultSets_.empty()) {
                            FillResultSet(ResultSets_.begin()->second, response.get());
                        }
                    } else {
                        Y_ENSURE(ResultSets_.empty());
                        auto results = record.GetResponse().GetResults();
                        if (!results.empty()) {
                            auto ydbResult = record.MutableResponse()->MutableYdbResults()->Add();
                            NKqp::ConvertKqpQueryResultToDbResult(results.at(0), ydbResult);
                            FillResultSet(*ydbResult, response.get());
                        }
                    }

                    // HACK
                    response->Tag = TStringBuilder() << "SELECT " << response->DataRows.size();
                    // HACK
                } else {
                    NYql::TIssues issues;
                    NYql::IssuesFromMessage(record.GetResponse().GetQueryIssues(), issues);
                    NYdb::TStatus status(NYdb::EStatus(record.GetYdbStatus()), std::move(issues));
                    response->ErrorFields.push_back({'E', "ERROR"});
                    response->ErrorFields.push_back({'M', TStringBuilder() << status});
                }
            } else {
                response->ErrorFields.push_back({'E', "ERROR"});
                response->ErrorFields.push_back({'M', "No result received"});
            }
        } catch (const std::exception& e) {
            response->ErrorFields.push_back({'E', "ERROR"});
            response->ErrorFields.push_back({'M', e.what()});
        }
        BLOG_D("Finally replying to " << RequestActorId_);
        Send(RequestActorId_, response.release(), 0, RequestCookie_);
        //PassAway();
    }

    void Handle(NPG::TEvPGEvents::TEvQuery::TPtr& ev) {
        BLOG_D("TEvQuery, sender: " << ev->Sender << " , self: " << SelfId());
        auto query(ev->Get()->Message->GetQuery());
        if (IsQueryEmpty(query)) {
            auto response = std::make_unique<NPG::TEvPGEvents::TEvQueryResponse>();
            response->EmptyQuery = true;
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }
        TString database;
        if (ConnectionParams_.count("database")) {
            database = ConnectionParams_["database"];
        }
        TString token;
        if (ConnectionParams_.count("ydb-serialized-token")) {
            token = ConnectionParams_["ydb-serialized-token"];
        }

        auto event = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        NKikimrKqp::TQueryRequest& request = *event->Record.MutableRequest();
        request.SetQuery(ToPgSyntax(query, ConnectionParams_));
        request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        auto noScript = ConnectionParams_.find("no-script");
        if (noScript == ConnectionParams_.end()) {
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_SCRIPT);
        } else {
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
            request.MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
            request.MutableTxControl()->set_commit_tx(true);
        }
        request.SetKeepSession(false);
        request.SetDatabase(database);
        event->Record.SetUserToken(token);
        InTransaction_ = ev->Get()->Message->GetQuery().starts_with("BEGIN");
        ActorIdToProto(SelfId(), event->Record.MutableRequestActorId());
        BLOG_D("Sent event to kqpProxy, RequestActorId = " << RequestActorId_ << ", self: " << SelfId());
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.Release());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPG::TEvPGEvents::TEvQuery, Handle);
            hFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            hFunc(NKqp::TEvKqpExecuter::TEvStreamData, Handle);
        }
    }
};


NActors::IActor* CreatePgwireKqpProxy(NActors::TActorId actorId, ui64 cookie, std::unordered_map<TString, TString> params) {
    return new TPgwireKqpProxy(std::move(actorId), std::move(cookie), std::move(params));
}

} //namespace NLocalPgwire
