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

namespace NYql::NFmr {

namespace {

using THandler = std::function<THttpResponse(THttpInput& input)>;

enum class ETableDataServiceRequestHandler {
    Put,
    Get,
    Delete,
    DeleteGroups,
    Clear
};

class TReplier: public TRequestReplier {
public:
    TReplier(std::unordered_map<ETableDataServiceRequestHandler, THandler>& handlers)
        : Handlers_(handlers)
    {
    }

    bool DoReply(const TReplyParams& params) override {
        TParsedHttpFull httpRequest(params.Input.FirstLine());
        auto handlerName = GetHandlerName(httpRequest);
        if (!handlerName) {
            params.Output << THttpResponse(HTTP_NOT_FOUND);
        } else {
            YQL_ENSURE(Handlers_.contains(*handlerName));
            auto callbackFunc = Handlers_[*handlerName];
            params.Output << callbackFunc(params.Input);
        }
        return true;
    }

private:
    std::unordered_map<ETableDataServiceRequestHandler, THandler> Handlers_;

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
        }
        return Nothing();
    }
};

class TTableDataServiceServer: public THttpServer::ICallBack, public IRunnable {
public:
    TTableDataServiceServer(ILocalTableDataService::TPtr tableDataService, const TTableDataServiceServerSettings& settings)
        : TableDataService_(tableDataService),
        Host_(settings.Host),
        Port_(settings.Port),
        WorkerId_(settings.WorkerId),
        WorkersNum_(settings.WorkersNum)
    {
        YQL_ENSURE(WorkerId_ >= 0 && WorkerId_ < WorkersNum_);
        THttpServer::TOptions opts;
        opts.AddBindAddress(Host_, Port_);
        HttpServer_ = MakeHolder<THttpServer>(this, opts.EnableKeepAlive(true).EnableCompression(true));

        THandler putTableDataServiceHandler = std::bind(&TTableDataServiceServer::PutTableDataServiceHandler, this, std::placeholders::_1);
        THandler getTableDataServiceHandler = std::bind(&TTableDataServiceServer::GetTableDataServiceHandler, this, std::placeholders::_1);
        THandler deleteTableDataServiceHandler = std::bind(&TTableDataServiceServer::DeleteTableDataServiceHandler, this, std::placeholders::_1);
        THandler deleteGroupsTableDataServiceHandler = std::bind(&TTableDataServiceServer::DeleteGroupsTableDataServiceHandler, this, std::placeholders::_1);
        THandler clearTableDataServiceHandler = std::bind(&TTableDataServiceServer::ClearTableDataServiceHander, this, std::placeholders::_1);

        Handlers_ = std::unordered_map<ETableDataServiceRequestHandler, THandler>{
            {ETableDataServiceRequestHandler::Put, putTableDataServiceHandler},
            {ETableDataServiceRequestHandler::Get, getTableDataServiceHandler},
            {ETableDataServiceRequestHandler::Delete, deleteTableDataServiceHandler},
            {ETableDataServiceRequestHandler::DeleteGroups, deleteGroupsTableDataServiceHandler},
            {ETableDataServiceRequestHandler::Clear, clearTableDataServiceHandler}
        };
    }

    void Start() override {
        HttpServer_->Start();
        Cerr << "Table data service server with id " << WorkerId_ << " is listnening on url " <<  "http://" + Host_ + ":" + ToString(Port_) << "\n";
    }

    void Stop() override {
        HttpServer_->Stop();
    }

    ~TTableDataServiceServer() override {
        Stop();
    }

    TClientRequest* CreateClient() override {
        return new TReplier(Handlers_);
    }

private:
    std::unordered_map<ETableDataServiceRequestHandler, THandler> Handlers_;
    THolder<THttpServer> HttpServer_;
    ILocalTableDataService::TPtr TableDataService_;
    const TString Host_;
    const ui16 Port_;
    const ui64 WorkerId_;
    const ui64 WorkersNum_;

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
        TableDataService_->Put(group, chunkId, ysonTableContent).GetValueSync();
        YQL_CLOG(TRACE, FastMapReduce) << "Putting key in table service with group " << group << " and chunkId " << chunkId;
        return THttpResponse(HTTP_OK);
    }

    THttpResponse GetTableDataServiceHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        auto tableDataServiceKey = GetTableDataServiceKey(input);
        TString group = tableDataServiceKey.Group, chunkId = tableDataServiceKey.ChunkId;
        TString ysonTableContent;
        if (auto value = TableDataService_->Get(group, chunkId).GetValueSync()) {
            ysonTableContent = *value;
        }
        THttpResponse httpResponse(HTTP_OK);
        httpResponse.SetContent(ysonTableContent);
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
        auto deletionRequest = TTableDataServiceGroupDeletionRequestFromProto(protoGroupDeletionRequest);
        TableDataService_->RegisterDeletion(deletionRequest).GetValueSync();
        YQL_CLOG(TRACE, FastMapReduce) << "Deleting groups in table data service" << JoinRange(' ', deletionRequest.begin(), deletionRequest.end());        return THttpResponse(HTTP_OK);
    }

    THttpResponse ClearTableDataServiceHander(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        YQL_CLOG(TRACE, FastMapReduce) << "Clearing table data service";
        TableDataService_->Clear().GetValueSync();
        return THttpResponse(HTTP_OK);
    }
};

} // namespace

IFmrServer::TPtr MakeTableDataServiceServer(ILocalTableDataService::TPtr tableDataService, const TTableDataServiceServerSettings& settings) {
    return MakeHolder<TTableDataServiceServer>(tableDataService, settings);
}

} // NYql::NFmr
