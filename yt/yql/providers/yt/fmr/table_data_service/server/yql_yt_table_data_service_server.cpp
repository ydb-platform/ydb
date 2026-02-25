#include "yql_yt_table_data_service_server.h"

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/yson/node/node_io.h>
#include <util/string/join.h>
#include <util/string/split.h>
#include <util/system/mutex.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_log_context.h>
#include <yt/yql/providers/yt/fmr/table_data_service/client/proto_helpers/yql_yt_table_data_service_proto_helpers.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_tvm_helpers.h>

namespace NYql::NFmr {

namespace {

using THandler = std::function<THttpResponse(THttpInput& input)>;

enum class ETableDataServiceRequestHandler {
    Put,
    Get,
    Delete,
    DeleteGroups,
    Clear,
    Ping
};

class TReplier: public TRequestReplier {
public:
    TReplier(
        std::unordered_map<ETableDataServiceRequestHandler, THandler>& handlers
        , IFmrTvmClient::TPtr tvmClient
        , const std::vector<TTvmId>& allowedSourceTvmIds
    )
        : Handlers_(handlers)
        , TvmClient_(tvmClient)
        , AllowedSourceTvmIds_(allowedSourceTvmIds)
    {
    }

    bool DoReply(const TReplyParams& params) override {
        TParsedHttpFull httpRequest(params.Input.FirstLine());
        auto handlerName = GetHandlerName(httpRequest);
        if (!handlerName) {
            params.Output << THttpResponse(HTTP_NOT_FOUND);
        } else {
            try {
                YQL_ENSURE(Handlers_.contains(*handlerName));
                if (*handlerName != ETableDataServiceRequestHandler::Ping) {
                    CheckTvmServiceTicket(params.Input.Headers(), TvmClient_, AllowedSourceTvmIds_);
                }
                auto callbackFunc = Handlers_[*handlerName];
                params.Output << callbackFunc(params.Input);
            } catch (...) {
                YQL_CLOG(ERROR, FastMapReduce) << "Error while processing url path " << httpRequest.Path << " is: " << CurrentExceptionMessage();
                THttpResponse response = THttpResponse(HTTP_BAD_REQUEST);
                response.SetContent(CurrentExceptionMessage());
                params.Output << response;
            }
        }
        return true;
    }

private:
    std::unordered_map<ETableDataServiceRequestHandler, THandler> Handlers_;
    IFmrTvmClient::TPtr TvmClient_;
    const std::vector<TTvmId> AllowedSourceTvmIds_;

    TMaybe<ETableDataServiceRequestHandler> GetHandlerName(TParsedHttpFull httpRequest) {
        TStringBuf queryPath;
        httpRequest.Path.SkipPrefix("/");
        queryPath = httpRequest.Path.NextTok('/');
        if (queryPath == "put_data") {
            YQL_ENSURE(httpRequest.Method == "POST");
            return ETableDataServiceRequestHandler::Put;
        } else if (queryPath == "delete_groups") {
            YQL_ENSURE(httpRequest.Method == "POST");
            return ETableDataServiceRequestHandler::DeleteGroups;
        } else if (queryPath == "delete_data") {
            YQL_ENSURE(httpRequest.Method == "DELETE");
            return ETableDataServiceRequestHandler::Delete;
        } else if (queryPath == "get_data") {
            YQL_ENSURE(httpRequest.Method == "GET");
            return ETableDataServiceRequestHandler::Get;
        } else if (queryPath == "clear") {
            YQL_ENSURE(httpRequest.Method == "POST");
            return ETableDataServiceRequestHandler::Clear;
        } else if (queryPath == "ping") {
            YQL_ENSURE(httpRequest.Method == "GET");
            return ETableDataServiceRequestHandler::Ping;
        }
        return Nothing();
    }
};

class TTableDataServiceServer: public THttpServer::ICallBack, public IRunnable {
public:
    TTableDataServiceServer(
        ILocalTableDataService::TPtr tableDataService,
        const TTableDataServiceServerSettings& settings,
        IFmrTvmClient::TPtr tvmClient = nullptr
    )
        : TableDataService_(tableDataService)
        , Host_(settings.Host)
        , Port_(settings.Port)
        , AllowedSourceTvmIds_(settings.AllowedSourceTvmIds)
        , TvmClient_(tvmClient)
    {
        THttpServer::TOptions opts;
        opts.AddBindAddress(Host_, Port_);
        HttpServer_ = MakeHolder<THttpServer>(this, opts.EnableKeepAlive(true).EnableCompression(true));

        THandler putTableDataServiceHandler = std::bind(&TTableDataServiceServer::PutTableDataServiceHandler, this, std::placeholders::_1);
        THandler getTableDataServiceHandler = std::bind(&TTableDataServiceServer::GetTableDataServiceHandler, this, std::placeholders::_1);
        THandler deleteTableDataServiceHandler = std::bind(&TTableDataServiceServer::DeleteTableDataServiceHandler, this, std::placeholders::_1);
        THandler deleteGroupsTableDataServiceHandler = std::bind(&TTableDataServiceServer::DeleteGroupsTableDataServiceHandler, this, std::placeholders::_1);
        THandler clearTableDataServiceHandler = std::bind(&TTableDataServiceServer::ClearTableDataServiceHander, this, std::placeholders::_1);
        THandler pingTableDataServiceHandler = std::bind(&TTableDataServiceServer::PingTableDataServiceHandler, this, std::placeholders::_1);

        Handlers_ = std::unordered_map<ETableDataServiceRequestHandler, THandler>{
            {ETableDataServiceRequestHandler::Put, putTableDataServiceHandler},
            {ETableDataServiceRequestHandler::Get, getTableDataServiceHandler},
            {ETableDataServiceRequestHandler::Delete, deleteTableDataServiceHandler},
            {ETableDataServiceRequestHandler::DeleteGroups, deleteGroupsTableDataServiceHandler},
            {ETableDataServiceRequestHandler::Clear, clearTableDataServiceHandler},
            {ETableDataServiceRequestHandler::Ping, pingTableDataServiceHandler}
        };
    }

    void Start() override {
        HttpServer_->Start();
        Cerr << "Table data service server is listnening on url " <<  "http://" + Host_ + ":" + ToString(Port_) << "\n";
    }

    void Stop() override {
        HttpServer_->Stop();
    }

    ~TTableDataServiceServer() override {
        Stop();
    }

    TClientRequest* CreateClient() override {
        return new TReplier(Handlers_, TvmClient_, AllowedSourceTvmIds_);
    }

private:
    std::unordered_map<ETableDataServiceRequestHandler, THandler> Handlers_;
    THolder<THttpServer> HttpServer_;
    ILocalTableDataService::TPtr TableDataService_;
    const TString Host_;
    const ui16 Port_;
    const std::vector<TTvmId> AllowedSourceTvmIds_;
    IFmrTvmClient::TPtr TvmClient_;

    struct TTableDataServiceKey {
        TString Group;
        TString ChunkId;
    };

    TTableDataServiceKey GetTableDataServiceKey(THttpInput& input) {
        TParsedHttpFull httpRequest(input.FirstLine());
        TCgiParameters queryParams(httpRequest.Cgi);
        YQL_ENSURE(queryParams.Has("group") && queryParams.Has("chunkId"));
        return TTableDataServiceKey{.Group = queryParams.Get("group"), .ChunkId = queryParams.Get("chunkId")};
    }

    THttpResponse PutTableDataServiceHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        TString ysonTableContent = input.ReadAll();
        auto tableDataServiceKey = GetTableDataServiceKey(input);
        TString group = tableDataServiceKey.Group, chunkId = tableDataServiceKey.ChunkId;
        bool putResult = TableDataService_->Put(group, chunkId, ysonTableContent).GetValueSync();
        auto protoPutResult = TableDataServicePutResponseToProto(putResult);
        YQL_CLOG(TRACE, FastMapReduce) << "Putting key in table service with group " << group << " and chunkId " << chunkId;
        THttpResponse httpResponse(HTTP_OK);
        httpResponse.SetContent(protoPutResult.SerializeAsStringOrThrow());
        httpResponse.SetContentType("application/x-protobuf");
        return httpResponse;
    }

    THttpResponse GetTableDataServiceHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        auto tableDataServiceKey = GetTableDataServiceKey(input);
        TString group = tableDataServiceKey.Group, chunkId = tableDataServiceKey.ChunkId;
        TMaybe<TString> getResult = TableDataService_->Get(group, chunkId).GetValueSync();
        auto protoGetResult = TableDataServiceGetResponseToProto(getResult);
        THttpResponse httpResponse(HTTP_OK);
        httpResponse.SetContent(protoGetResult.SerializeAsString());
        httpResponse.SetContentType("application/x-protobuf");
        YQL_CLOG(TRACE, FastMapReduce) << "Getting key in table service with group " << group << " and chunkId " << chunkId;
        return httpResponse;
    }

    THttpResponse DeleteTableDataServiceHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        auto tableDataServiceKey = GetTableDataServiceKey(input);
        TString group = tableDataServiceKey.Group, chunkId = tableDataServiceKey.ChunkId;
        TableDataService_->Delete(group, chunkId).GetValueSync();
        YQL_CLOG(TRACE, FastMapReduce) << "Deleting key in table service with group " << group << " and chunkId " << chunkId;
        return THttpResponse(HTTP_OK);
    }

    THttpResponse DeleteGroupsTableDataServiceHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        TString serializedProtoGroupDeletionRequest = input.ReadAll();
        NProto::TTableDataServiceGroupDeletionRequest protoGroupDeletionRequest;
        protoGroupDeletionRequest.ParseFromStringOrThrow(serializedProtoGroupDeletionRequest);
        auto deletionRequest = TableDataServiceGroupDeletionRequestFromProto(protoGroupDeletionRequest);
        TableDataService_->RegisterDeletion(deletionRequest).GetValueSync();
        YQL_CLOG(TRACE, FastMapReduce) << "Deleting groups in table data service " << JoinRange(' ', deletionRequest.begin(), deletionRequest.end());        return THttpResponse(HTTP_OK);
    }

    THttpResponse ClearTableDataServiceHander(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        YQL_CLOG(TRACE, FastMapReduce) << "Clearing table data service";
        TableDataService_->Clear().GetValueSync();
        return THttpResponse(HTTP_OK);
    }

    THttpResponse PingTableDataServiceHandler(THttpInput& /*input*/) {
        return THttpResponse(HTTP_OK);
    }
};

} // namespace

IFmrServer::TPtr MakeTableDataServiceServer(
    ILocalTableDataService::TPtr tableDataService,
    const TTableDataServiceServerSettings& settings,
    IFmrTvmClient::TPtr tvmClient
) {
    return MakeHolder<TTableDataServiceServer>(tableDataService, settings, tvmClient);
}

} // NYql::NFmr
