#pragma once

#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/kqp_session_common/kqp_session_common.h>

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

namespace NYdb::NQuery {

class TSession::TImpl : public TKqpSessionCommon {
public:
    struct TAttachSessionArgs {
        TAttachSessionArgs(NThreading::TPromise<TCreateSessionResult> promise,
            TString sessionId,
            TString endpoint,
            std::shared_ptr<TQueryClient::TImpl> client)
            : Promise(promise)
            , SessionId(sessionId)
            , Endpoint(endpoint)
            , Client(client)
        { }
        NThreading::TPromise<TCreateSessionResult> Promise;
        TString SessionId;
        TString Endpoint;
        std::shared_ptr<TQueryClient::TImpl> Client;
    };

    using TResponse = Ydb::Query::SessionState;
    using TStreamProcessorPtr = NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TPtr;
    TImpl(TStreamProcessorPtr ptr, const TString& id, const TString& endpoint);
    ~TImpl();

    static void MakeImplAsync(TStreamProcessorPtr processor, std::shared_ptr<TAttachSessionArgs> args);

private:
    static void NewSmartShared(TStreamProcessorPtr ptr, std::shared_ptr<TAttachSessionArgs> args, NYdb::TStatus status);

private:
    TStreamProcessorPtr StreamProcessor_;
};

}
