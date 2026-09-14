#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/deferred_publication_limits.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>

#include <util/datetime/base.h>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace NYdb::inline Dev::NTopic {

constexpr size_t MaxExtPublicationIdLength = MaxDeferredPublishExtIdLength;

struct TBeginPublicationSettings : public TOperationRequestSettings<TBeginPublicationSettings> {
    using TSelf = TBeginPublicationSettings;

    FLUENT_SETTING_OPTIONAL(std::string, WriterIdentity);
};

struct TListPublicationsSettings : public TOperationRequestSettings<TListPublicationsSettings> {
    using TSelf = TListPublicationsSettings;

    FLUENT_SETTING_OPTIONAL(std::string, WriterIdentity);
};

struct TPublishSettings : public TOperationRequestSettings<TPublishSettings> {
    using TOperationRequestSettings<TPublishSettings>::TOperationRequestSettings;
};

struct TCancelPublicationSettings : public TOperationRequestSettings<TCancelPublicationSettings> {
    using TOperationRequestSettings<TCancelPublicationSettings>::TOperationRequestSettings;
};

struct TDescribePublicationSettings : public TOperationRequestSettings<TDescribePublicationSettings> {
    using TOperationRequestSettings<TDescribePublicationSettings>::TOperationRequestSettings;
};

struct TPublicationSummary {
    uint64_t IntPublicationId = 0;
    std::string ExtPublicationId;
    std::optional<std::string> WriterIdentity;
};

struct TDestination {
    std::string TopicPath;
    std::vector<int64_t> PartitionIds;
};

struct TPublicationDescription {
    std::string ExtPublicationId;
    std::optional<std::string> WriterIdentity;
    TInstant CreatedAt;
    std::optional<std::string> CreatedBy;
    std::vector<TDestination> Destinations;
};

struct TBeginPublicationResult : public TStatus {
    TBeginPublicationResult(TStatus&& status)
        : TStatus(std::move(status))
    {}

    TBeginPublicationResult(TStatus&& status, TDeferredPublication&& publication);

    const TDeferredPublication& GetPublication() const;

    uint64_t GetIntPublicationId() const;

private:
    TDeferredPublication Publication_;
};

struct TPublishResult : public TStatus {
    TPublishResult(TStatus&& status)
        : TStatus(std::move(status))
    {}
};

struct TCancelPublicationResult : public TStatus {
    TCancelPublicationResult(TStatus&& status)
        : TStatus(std::move(status))
    {}
};

struct TListPublicationsResult : public TStatus {
    TListPublicationsResult(TStatus&& status)
        : TStatus(std::move(status))
    {}

    TListPublicationsResult(TStatus&& status, std::vector<TPublicationSummary>&& publications);

    const std::vector<TPublicationSummary>& GetPublications() const;

private:
    std::vector<TPublicationSummary> Publications_;
};

struct TDescribePublicationResult : public TStatus {
    TDescribePublicationResult(TStatus&& status)
        : TStatus(std::move(status))
    {}

    TDescribePublicationResult(TStatus&& status, TPublicationDescription&& publication);

    const TPublicationDescription& GetPublication() const;

private:
    TPublicationDescription Publication_;
};

using TAsyncBeginPublicationResult = NThreading::TFuture<TBeginPublicationResult>;
using TAsyncPublishResult = NThreading::TFuture<TPublishResult>;
using TAsyncCancelPublicationResult = NThreading::TFuture<TCancelPublicationResult>;
using TAsyncListPublicationsResult = NThreading::TFuture<TListPublicationsResult>;
using TAsyncDescribePublicationResult = NThreading::TFuture<TDescribePublicationResult>;

class TDeferredPublishClient {
    class TImpl;

public:
    TDeferredPublishClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncBeginPublicationResult BeginPublication(
        const std::string& extPublicationId,
        const TBeginPublicationSettings& settings = {});

    TAsyncPublishResult Publish(
        const TDeferredPublication& publication,
        const TPublishSettings& settings = {});

    TAsyncCancelPublicationResult CancelPublication(
        const TDeferredPublication& publication,
        const TCancelPublicationSettings& settings = {});

    TAsyncListPublicationsResult ListPublications(
        const TListPublicationsSettings& settings = {});

    TAsyncDescribePublicationResult DescribePublication(
        const TDeferredPublication& publication,
        const TDescribePublicationSettings& settings = {});

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NTopic
