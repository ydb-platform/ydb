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
        Y_VALIDATE(extPublicationId, "External publication id must be not empty");
        Y_VALIDATE(extPublicationId.size() <= TDeferredPublication::MaxExtPublicationIdLength, "External publication id is too large, max length is " << TDeferredPublication::MaxExtPublicationIdLength << ", got " << extPublicationId.size());

        TEvBeginPublicationRequest::TRequest request;
        request.set_ext_publication_id(extPublicationId);

        if (const auto& writerId = settings.WriterIdentity_) {
            Y_VALIDATE(writerId->size() <= TDeferredPublication::MaxExtPublicationIdLength, "Writer identity is too large, max length is " << TDeferredPublication::MaxExtPublicationIdLength << ", got " << writerId->size());
            request.set_writer_identity(*writerId);
        }

        return DoLocalRpcRequest<TEvBeginPublicationRequest, TBeginPublicationSettings>(std::move(request), settings, &DoBeginPublicationRequest).Apply([extPublicationId](const NThreading::TFuture<TLocalRpcOperationResult>& f) {
            const auto& [status, response] = f.GetValue();
            Ydb::Topic::DeferredPublish::BeginPublicationResult result;
            response.UnpackTo(&result);
            return TBeginPublicationResult(TStatus(status), TDeferredPublication(result.int_publication_id(), extPublicationId));
        });
    }

    TAsyncPublishResult Publish(const TDeferredPublication& publication, const TPublishSettings& settings) final {
        Y_VALIDATE(publication.IntPublicationId > 0, "Internal publication id must be positive");

        TEvPublishRequest::TRequest request;
        request.set_int_publication_id(publication.IntPublicationId);

        return DoLocalRpcRequest<TEvPublishRequest, TPublishSettings>(std::move(request), settings, &DoPublishRequest).Apply([](const NThreading::TFuture<TLocalRpcOperationResult>& f) {
            const auto& [status, response] = f.GetValue();
            return TPublishResult(TStatus(status));
        });
    }

    TAsyncCancelPublicationResult CancelPublication(const TDeferredPublication& publication, const TCancelPublicationSettings& settings) final {
        Y_VALIDATE(publication.IntPublicationId > 0, "Internal publication id must be positive");

        TEvCancelPublicationRequest::TRequest request;
        request.set_int_publication_id(publication.IntPublicationId);

        return DoLocalRpcRequest<TEvCancelPublicationRequest, TCancelPublicationSettings>(std::move(request), settings, &DoCancelPublicationRequest).Apply([](const NThreading::TFuture<TLocalRpcOperationResult>& f) {
            const auto& [status, _] = f.GetValue();
            return TCancelPublicationResult(TStatus(status));
        });
    }

    TAsyncListPublicationsResult ListPublications(const TListPublicationsSettings& settings) final {
        TEvListPublicationsRequest::TRequest request;

        if (const auto& writerId = settings.WriterIdentity_) {
            Y_VALIDATE(writerId->size() <= TDeferredPublication::MaxExtPublicationIdLength, "Writer identity is too large, max length is " << TDeferredPublication::MaxExtPublicationIdLength << ", got " << writerId->size());
            request.set_writer_identity(*writerId);
        }

        return DoLocalRpcRequest<TEvListPublicationsRequest, TListPublicationsSettings>(std::move(request), settings, &DoListPublicationsRequest).Apply([](const NThreading::TFuture<TLocalRpcOperationResult>& f) {
            const auto& [status, response] = f.GetValue();
            Ydb::Topic::DeferredPublish::ListPublicationsResult result;
            response.UnpackTo(&result);

            std::vector<TPublicationSummary> publications;
            publications.reserve(result.publications_size());
            for (const auto& publication : result.publications()) {
                TPublicationSummary summary;
                summary.IntPublicationId = publication.int_publication_id();
                summary.ExtPublicationId = publication.ext_publication_id();
                if (publication.has_writer_identity()) {
                    summary.WriterIdentity = publication.writer_identity();
                }
                publications.emplace_back(std::move(summary));
            }

            return TListPublicationsResult(TStatus(status), std::move(publications));
        });
    }
};

} // anonymous namespace

NYql::IDeferredPublishClient::TPtr CreateLocalDeferredPublishClient(const TLocalTopicClientSettings& localSettings, const TCommonClientSettings& clientSettings) {
    return MakeIntrusive<TLocalDeferredPublishClient>(localSettings, clientSettings);
}

} // namespace NKikimr::NKqp
