#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/deferred_publications.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/operation/operation.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/draft/ydb_topic_deferred_publish_v1.grpc.pb.h>
#include <ydb/public/api/protos/draft/ydb_topic_deferred_publish.pb.h>
#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/deferred_publication_ack_tracker.h>

namespace NYdb::inline Dev::NTopic {

namespace {

using namespace Ydb::Topic::DeferredPublish;

TStatus MakeBadRequestStatus(const std::string& message) {
    return TStatus(EStatus::BAD_REQUEST, NYdb::NIssue::TIssues{NYdb::NIssue::TIssue(message)});
}

template <typename TResult>
NThreading::TFuture<TResult> MakeBadRequestFuture(const std::string& message) {
    return NThreading::MakeFuture(TResult(MakeBadRequestStatus(message)));
}

NThreading::TFuture<TStatus> WaitAcksIfNeeded(const TDeferredPublication& publication) {
    return TDeferredPublication::TAccess::AckState(publication)->WaitAllAcks();
}

TPublicationSummary FromProto(const PublicationSummary& summary) {
    TPublicationSummary result;
    result.IntPublicationId = summary.int_publication_id();
    result.ExtPublicationId = summary.ext_publication_id();
    if (summary.has_writer_identity()) {
        result.WriterIdentity = summary.writer_identity();
    }
    return result;
}

TPublicationDescription FromProto(const DescribePublicationResult& result) {
    TPublicationDescription description;
    description.ExtPublicationId = result.ext_publication_id();
    if (result.has_writer_identity()) {
        description.WriterIdentity = result.writer_identity();
    }
    description.CreatedAt = ProtoTimestampToInstant(result.created_at());
    if (result.has_created_by()) {
        description.CreatedBy = result.created_by();
    }
    description.Destinations.reserve(result.destinations_size());
    for (const auto& destination : result.destinations()) {
        TDestination item;
        item.TopicPath = destination.topic_path();
        item.PartitionIds.reserve(destination.partition_ids_size());
        for (const auto partitionId : destination.partition_ids()) {
            item.PartitionIds.push_back(partitionId);
        }
        description.Destinations.push_back(std::move(item));
    }
    return description;
}

} // namespace

TBeginPublicationResult::TBeginPublicationResult(TStatus&& status, TDeferredPublication&& publication)
    : TStatus(std::move(status))
    , Publication_(std::move(publication))
{
}

const TDeferredPublication& TBeginPublicationResult::GetPublication() const {
    CheckStatusOk("TBeginPublicationResult::GetPublication");
    return Publication_;
}

uint64_t TBeginPublicationResult::GetIntPublicationId() const {
    return GetPublication().IntPublicationId;
}

TListPublicationsResult::TListPublicationsResult(TStatus&& status, std::vector<TPublicationSummary>&& publications)
    : TStatus(std::move(status))
    , Publications_(std::move(publications))
{
}

const std::vector<TPublicationSummary>& TListPublicationsResult::GetPublications() const {
    CheckStatusOk("TListPublicationsResult::GetPublications");
    return Publications_;
}

TDescribePublicationResult::TDescribePublicationResult(TStatus&& status, TPublicationDescription&& publication)
    : TStatus(std::move(status))
    , Publication_(std::move(publication))
{
}

const TPublicationDescription& TDescribePublicationResult::GetPublication() const {
    CheckStatusOk("TDescribePublicationResult::GetPublication");
    return Publication_;
}

class TDeferredPublishClient::TImpl : public TClientImplCommon<TDeferredPublishClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {
    }

    TAsyncBeginPublicationResult BeginPublication(
        const std::string& extPublicationId,
        const TBeginPublicationSettings& settings)
    {
        if (extPublicationId.empty()) {
            return MakeBadRequestFuture<TBeginPublicationResult>("ext_publication_id must not be empty");
        }
        if (extPublicationId.size() > MaxExtPublicationIdLength) {
            return MakeBadRequestFuture<TBeginPublicationResult>("ext_publication_id is too long");
        }
        if (settings.WriterIdentity_ && settings.WriterIdentity_->size() > MaxExtPublicationIdLength) {
            return MakeBadRequestFuture<TBeginPublicationResult>("writer_identity is too long");
        }

        auto request = MakeOperationRequest<BeginPublicationRequest>(settings);
        request.set_ext_publication_id(TStringType{extPublicationId});
        if (settings.WriterIdentity_) {
            request.set_writer_identity(TStringType{*settings.WriterIdentity_});
        }

        auto promise = NThreading::NewPromise<TBeginPublicationResult>();
        auto extractor = [promise, extPublicationId](google::protobuf::Any* any, TPlainStatus status) mutable {
            TStatus ydbStatus(std::move(status));
            if (!ydbStatus.IsSuccess()) {
                promise.SetValue(TBeginPublicationResult(std::move(ydbStatus)));
                return;
            }

            BeginPublicationResult result;
            if (any) {
                any->UnpackTo(&result);
            }

            TDeferredPublication publication(result.int_publication_id(), extPublicationId);
            promise.SetValue(TBeginPublicationResult(std::move(ydbStatus), std::move(publication)));
        };

        Connections_->RunDeferred<
            Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService,
            BeginPublicationRequest,
            BeginPublicationResponse>(
            std::move(request),
            extractor,
            &Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService::Stub::AsyncBeginPublication,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncPublishResult Publish(const TDeferredPublication& publication, const TPublishSettings& settings) {
        if (publication.IntPublicationId == 0) {
            return MakeBadRequestFuture<TPublishResult>("int_publication_id must be greater than zero");
        }

        auto request = MakeOperationRequest<PublishRequest>(settings);
        request.set_int_publication_id(publication.IntPublicationId);

        auto promise = NThreading::NewPromise<TPublishResult>();
        auto connections = Connections_;
        auto dbDriverState = DbDriverState_;
        auto rpcSettings = TRpcRequestSettings::Make(settings);

        WaitAcksIfNeeded(publication)
            .Subscribe([
                promise,
                request = std::move(request),
                connections,
                dbDriverState,
                rpcSettings
            ](const NThreading::TFuture<TStatus>& future) mutable {
                auto waitStatus = future.GetValue();
                if (!waitStatus.IsSuccess()) {
                    promise.SetValue(TPublishResult(TStatus(std::move(waitStatus))));
                    return;
                }

                auto extractor = [promise](google::protobuf::Any* any, TPlainStatus status) mutable {
                    Y_UNUSED(any);
                    promise.SetValue(TPublishResult(TStatus(std::move(status))));
                };

                connections->RunDeferred<
                    Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService,
                    PublishRequest,
                    PublishResponse>(
                    std::move(request),
                    extractor,
                    &Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService::Stub::AsyncPublish,
                    dbDriverState,
                    INITIAL_DEFERRED_CALL_DELAY,
                    rpcSettings);
            });

        return promise.GetFuture();
    }

    TAsyncCancelPublicationResult CancelPublication(
        const TDeferredPublication& publication,
        const TCancelPublicationSettings& settings)
    {
        if (publication.IntPublicationId == 0) {
            return MakeBadRequestFuture<TCancelPublicationResult>("int_publication_id must be greater than zero");
        }

        auto request = MakeOperationRequest<CancelPublicationRequest>(settings);
        request.set_int_publication_id(publication.IntPublicationId);

        auto promise = NThreading::NewPromise<TCancelPublicationResult>();
        auto connections = Connections_;
        auto dbDriverState = DbDriverState_;
        auto rpcSettings = TRpcRequestSettings::Make(settings);

        WaitAcksIfNeeded(publication)
            .Subscribe([
                promise,
                request = std::move(request),
                connections,
                dbDriverState,
                rpcSettings
            ](const NThreading::TFuture<TStatus>& future) mutable {
                auto waitStatus = future.GetValue();
                if (!waitStatus.IsSuccess()) {
                    promise.SetValue(TCancelPublicationResult(TStatus(std::move(waitStatus))));
                    return;
                }

                auto extractor = [promise](google::protobuf::Any* any, TPlainStatus status) mutable {
                    Y_UNUSED(any);
                    promise.SetValue(TCancelPublicationResult(TStatus(std::move(status))));
                };

                connections->RunDeferred<
                    Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService,
                    CancelPublicationRequest,
                    CancelPublicationResponse>(
                    std::move(request),
                    extractor,
                    &Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService::Stub::AsyncCancelPublication,
                    dbDriverState,
                    INITIAL_DEFERRED_CALL_DELAY,
                    rpcSettings);
            });

        return promise.GetFuture();
    }

    TAsyncListPublicationsResult ListPublications(const TListPublicationsSettings& settings) {
        if (settings.WriterIdentity_ && settings.WriterIdentity_->size() > MaxExtPublicationIdLength) {
            return MakeBadRequestFuture<TListPublicationsResult>("writer_identity is too long");
        }

        auto request = MakeOperationRequest<ListPublicationsRequest>(settings);
        if (settings.WriterIdentity_) {
            request.set_writer_identity(TStringType{*settings.WriterIdentity_});
        }

        auto promise = NThreading::NewPromise<TListPublicationsResult>();
        auto extractor = [promise](google::protobuf::Any* any, TPlainStatus status) mutable {
            ListPublicationsResult result;
            std::vector<TPublicationSummary> publications;
            if (any) {
                any->UnpackTo(&result);
                publications.reserve(result.publications_size());
                for (const auto& publication : result.publications()) {
                    publications.push_back(FromProto(publication));
                }
            }
            promise.SetValue(TListPublicationsResult(TStatus(std::move(status)), std::move(publications)));
        };

        Connections_->RunDeferred<
            Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService,
            ListPublicationsRequest,
            ListPublicationsResponse>(
            std::move(request),
            extractor,
            &Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService::Stub::AsyncListPublications,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncDescribePublicationResult DescribePublication(
        const TDeferredPublication& publication,
        const TDescribePublicationSettings& settings)
    {
        if (publication.IntPublicationId == 0) {
            return MakeBadRequestFuture<TDescribePublicationResult>("int_publication_id must be greater than zero");
        }

        auto request = MakeOperationRequest<DescribePublicationRequest>(settings);
        request.set_int_publication_id(publication.IntPublicationId);

        auto promise = NThreading::NewPromise<TDescribePublicationResult>();
        auto extractor = [promise](google::protobuf::Any* any, TPlainStatus status) mutable {
            DescribePublicationResult result;
            TPublicationDescription description;
            if (any) {
                any->UnpackTo(&result);
                description = FromProto(result);
            }
            promise.SetValue(TDescribePublicationResult(TStatus(std::move(status)), std::move(description)));
        };

        Connections_->RunDeferred<
            Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService,
            DescribePublicationRequest,
            DescribePublicationResponse>(
            std::move(request),
            extractor,
            &Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService::Stub::AsyncDescribePublication,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }
};

TDeferredPublishClient::TDeferredPublishClient(
    const TDriver& driver,
    const TCommonClientSettings& settings)
    : Impl_(std::make_shared<TImpl>(CreateInternalInterface(driver), settings))
{
}

TAsyncBeginPublicationResult TDeferredPublishClient::BeginPublication(
    const std::string& extPublicationId,
    const TBeginPublicationSettings& settings)
{
    return Impl_->BeginPublication(extPublicationId, settings);
}

TAsyncPublishResult TDeferredPublishClient::Publish(
    const TDeferredPublication& publication,
    const TPublishSettings& settings)
{
    return Impl_->Publish(publication, settings);
}

TAsyncCancelPublicationResult TDeferredPublishClient::CancelPublication(
    const TDeferredPublication& publication,
    const TCancelPublicationSettings& settings)
{
    return Impl_->CancelPublication(publication, settings);
}

TAsyncListPublicationsResult TDeferredPublishClient::ListPublications(
    const TListPublicationsSettings& settings)
{
    return Impl_->ListPublications(settings);
}

TAsyncDescribePublicationResult TDeferredPublishClient::DescribePublication(
    const TDeferredPublication& publication,
    const TDescribePublicationSettings& settings)
{
    return Impl_->DescribePublication(publication, settings);
}

} // namespace NYdb::NTopic
