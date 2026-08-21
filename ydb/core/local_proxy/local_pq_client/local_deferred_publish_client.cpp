#include "local_deferred_publish_client.h"
#include "local_topic_client_helpers.h"

#include <ydb/core/grpc_services/rpc_calls_topic_deferred_publish.h>
#include <ydb/core/grpc_services/service_topic_deferred_publish.h>

namespace NKikimr::NKqp {

namespace {

using namespace NGRpcService;
using namespace NRpcService;
using namespace NYdb;
using namespace NYdb::NTopic;

class TLocalDeferredPublishClient final : public TLocalTopicClientBase, public NYql::IDeferredPublishClient {
    using TBase = TLocalTopicClientBase;

public:
    using TBase::TBase;

    TAsyncBeginPublicationResult BeginPublication(const TString& extPublicationId, const TBeginPublicationSettings& settings) final {
        Y_UNUSED(extPublicationId, settings);
        Y_VALIDATE(false, __func__ << " is not implemented");
        TEvBeginPublicationRequest::TRequest request;
        request.set_ext_publication_id(extPublicationId);

        if (settings.WriterIdentity_) {
            request.set_writer_identity(*settings.WriterIdentity_);
        }

        return DoLocalRpcRequest<TEvBeginPublicationRequest, TBeginPublicationSettings>(std::move(request), settings, &DoBeginPublicationRequest).Apply([extPublicationId](const NThreading::TFuture<TLocalRpcOperationResult>& f) {
            const auto& [status, response] = f.GetValue();
            Ydb::Topic::DeferredPublish::BeginPublicationResult result;
            response.UnpackTo(&result);
            return TBeginPublicationResult(TStatus(status), TDeferredPublication(result.int_publication_id(), extPublicationId));
        });
    }

    TAsyncPublishResult Publish(const TDeferredPublication& publication, const TPublishSettings& settings) final {
        TEvPublishRequest::TRequest request;
        request.set_int_publication_id(publication.IntPublicationId);

        return DoLocalRpcRequest<TEvPublishRequest, TPublishSettings>(std::move(request), settings, &DoPublishRequest).Apply([](const NThreading::TFuture<TLocalRpcOperationResult>& f) {
            const auto& [status, response] = f.GetValue();
            return TPublishResult(TStatus(status));
        });
    }
};

} // anonymous namespace

NYql::IDeferredPublishClient::TPtr CreateLocalDeferredPublishClient(const TLocalTopicClientSettings& localSettings, const TCommonClientSettings& clientSettings) {
    return MakeIntrusive<TLocalDeferredPublishClient>(localSettings, clientSettings);
}

} // namespace NKikimr::NKqp
