#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/kqp_session_common/kqp_session_common.h>

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

namespace NYdb::inline Dev::NQuery {

class TSafeTSessionImplHolder;

class TSession::TImpl : public TKqpSessionCommon {
public:
    struct TAttachSessionArgs {
        TAttachSessionArgs(NThreading::TPromise<TCreateSessionResult> promise,
            std::string sessionId,
            std::string endpoint,
            std::shared_ptr<TQueryClient::TImpl> client,
            std::weak_ptr<ISessionClient> sessionClient)
            : Promise(promise)
            , SessionId(sessionId)
            , Endpoint(endpoint)
            , Client(client)
            , SessionClient(sessionClient)
        { }
        NThreading::TPromise<TCreateSessionResult> Promise;
        std::string SessionId;
        std::string Endpoint;
        std::shared_ptr<TQueryClient::TImpl> Client;
        std::weak_ptr<ISessionClient> SessionClient;
    };

    using TResponse = Ydb::Query::SessionState;
    using TStreamProcessorPtr = NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TPtr;
    TImpl(TStreamProcessorPtr ptr, const std::string& id, const std::string& endpoint, std::weak_ptr<ISessionClient> client);
    ~TImpl();

    static void MakeImplAsync(TStreamProcessorPtr processor, std::shared_ptr<TAttachSessionArgs> args);

private:
    static void NewSmartShared(TStreamProcessorPtr ptr, std::shared_ptr<TAttachSessionArgs> args, NYdb::TStatus status);

    static void StartAsyncRead(TStreamProcessorPtr ptr, std::weak_ptr<ISessionClient> client, std::shared_ptr<TSafeTSessionImplHolder> session);

private:
    TStreamProcessorPtr StreamProcessor_;
    std::shared_ptr<TSafeTSessionImplHolder> SessionHolder;
};

}
