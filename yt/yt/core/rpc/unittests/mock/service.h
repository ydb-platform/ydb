#pragma once

#include <yt/yt/core/rpc/service.h>

#include <yt/yt/core/bus/bus.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TMockServiceContext
    : public IServiceContext
{
public:
    NProto::TRequestHeader RequestHeader_;
    std::vector<TSharedRef> ResponseAttachments_;

public:
    TMockServiceContext();

    MOCK_METHOD(
        const NProto::TRequestHeader&,
        GetRequestHeader,
        (),
        (const, override));

    MOCK_METHOD(
        TSharedRefArray,
        GetRequestMessage,
        (),
        (const, override));

    MOCK_METHOD(
        TRequestId,
        GetRequestId,
        (),
        (const, override));

    MOCK_METHOD(
        NBus::TBusNetworkStatistics,
        GetBusNetworkStatistics,
        (),
        (const, override));

    MOCK_METHOD(
        i64,
        GetTotalSize,
        (),
        (const, override));

    MOCK_METHOD(
        NYTree::IAttributeDictionary&,
        GetEndpointAttributes,
        (),
        (const, override));

    MOCK_METHOD(
        std::optional<TInstant>,
        GetStartTime,
        (),
        (const, override));

    MOCK_METHOD(
        std::optional<TDuration>,
        GetTimeout,
        (),
        (const, override));

    MOCK_METHOD(
        TInstant,
        GetArriveInstant,
        (),
        (const, override));

    MOCK_METHOD(
        std::optional<TInstant>,
        GetRunInstant,
        (),
        (const, override));

    MOCK_METHOD(
        std::optional<TInstant>,
        GetFinishInstant,
        (),
        (const, override));

    MOCK_METHOD(
        std::optional<TDuration>,
        GetWaitDuration,
        (),
        (const, override));

    MOCK_METHOD(
        std::optional<TDuration>,
        GetExecutionDuration,
        (),
        (const, override));

    MOCK_METHOD(
        NTracing::TTraceContextPtr,
        GetTraceContext,
        (),
        (const, override));

    MOCK_METHOD(
        std::optional<TDuration>,
        GetTraceContextTime,
        (),
        (const, override));

    MOCK_METHOD(
        bool,
        IsRetry,
        (),
        (const, override));

    MOCK_METHOD(
        TMutationId,
        GetMutationId,
        (),
        (const, override));

    MOCK_METHOD(
        std::string,
        GetService,
        (),
        (const, override));

    MOCK_METHOD(
        std::string,
        GetMethod,
        (),
        (const, override));

    MOCK_METHOD(
        TRealmId,
        GetRealmId,
        (),
        (const, override));

    MOCK_METHOD(
        const TAuthenticationIdentity&,
        GetAuthenticationIdentity,
        (),
        (const, override));

    MOCK_METHOD(
        bool,
        IsReplied,
        (),
        (const, override));

    MOCK_METHOD(
        void,
        Reply,
        (const TError& error),
        (override));

    MOCK_METHOD(
        void,
        Reply,
        (const TSharedRefArray& message),
        (override));

    MOCK_METHOD(
        void,
        SetComplete,
        (),
        (override));

    MOCK_METHOD(
        bool,
        IsCanceled,
        (),
        (const, override));

    MOCK_METHOD(
        void,
        SubscribeCanceled,
        (const TCallback<void(const TError&)>& callback),
        (override));

    MOCK_METHOD(
        void,
        UnsubscribeCanceled,
        (const TCallback<void(const TError&)>& callback),
        (override));

    MOCK_METHOD(
        void,
        SubscribeReplied,
        (const TCallback<void()>& callback),
        (override));

    MOCK_METHOD(
        void,
        UnsubscribeReplied,
        (const TCallback<void()>& callback),
        (override));

    MOCK_METHOD(
        void,
        Cancel,
        (),
        (override));

    MOCK_METHOD(
        TFuture<NYT::TSharedRefArray>,
        GetAsyncResponseMessage,
        (),
        (const, override));

    MOCK_METHOD(
        const TSharedRefArray&,
        GetResponseMessage,
        (),
        (const, override));

    MOCK_METHOD(
        const TError&,
        GetError,
        (),
        (const, override));

    MOCK_METHOD(
        TSharedRef,
        GetRequestBody,
        (),
        (const, override));

    MOCK_METHOD(
        TSharedRef,
        GetResponseBody,
        (),
        (override));

    MOCK_METHOD(
        void,
        SetResponseBody,
        (const TSharedRef& responseBody),
        (override));

    MOCK_METHOD(
        std::vector<TSharedRef>&,
        RequestAttachments,
        (),
        (override));

    MOCK_METHOD(
        NConcurrency::IAsyncZeroCopyInputStreamPtr,
        GetRequestAttachmentsStream,
        (),
        (override));

    MOCK_METHOD(
        std::vector<TSharedRef>&,
        ResponseAttachments,
        (),
        (override));

    MOCK_METHOD(
        NConcurrency::IAsyncZeroCopyOutputStreamPtr,
        GetResponseAttachmentsStream,
        (),
        (override));

    MOCK_METHOD(
        const NProto::TRequestHeader&,
        RequestHeader,
        (),
        (const, override));

    MOCK_METHOD(
        NProto::TRequestHeader&,
        RequestHeader,
        (),
        (override));

    MOCK_METHOD(
        void,
        SetRawRequestInfo,
        (TString info, bool incremental),
        (override));

    MOCK_METHOD(
        void,
        SetRawResponseInfo,
        (TString info, bool incremental),
        (override));

    MOCK_METHOD(
        const IMemoryUsageTrackerPtr&,
        GetMemoryUsageTracker,
        (),
        (const, override));

    MOCK_METHOD(
        const NLogging::TLogger&,
        GetLogger,
        (),
        (const, override));

    MOCK_METHOD(
        NLogging::ELogLevel,
        GetLogLevel,
        (),
        (const, override));

    MOCK_METHOD(
        bool,
        IsPooled,
        (),
        (const, override));

    MOCK_METHOD(
        NCompression::ECodec,
        GetResponseCodec,
        (),
        (const, override));

    MOCK_METHOD(
        void,
        SetResponseCodec,
        (NCompression::ECodec codec),
        (override));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
