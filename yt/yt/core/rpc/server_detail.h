#pragma once

#include "authentication_identity.h"
#include "server.h"
#include "service.h"
#include "config.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

// Magic constant! This is lower limit of memory allocated for request.
constexpr i64 TypicalRequestSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

//! \note Thread affinity: single-threaded (unless noted otherwise)
class TServiceContextBase
    : public virtual IServiceContext
{
public:
    const NProto::TRequestHeader& GetRequestHeader() const override;
    TSharedRefArray GetRequestMessage() const override;

    TRequestId GetRequestId() const override;
    NYT::NBus::TBusNetworkStatistics GetBusNetworkStatistics() const override;
    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override;
    const TString& GetEndpointDescription() const override;

    i64 GetTotalSize() const override;

    std::optional<TInstant> GetStartTime() const override;
    std::optional<TDuration> GetTimeout() const override;
    TInstant GetArriveInstant() const override;
    std::optional<TInstant> GetRunInstant() const override;
    std::optional<TInstant> GetFinishInstant() const override;
    std::optional<TDuration> GetWaitDuration() const override;
    std::optional<TDuration> GetExecutionDuration() const override;

    NTracing::TTraceContextPtr GetTraceContext() const override;
    std::optional<TDuration> GetTraceContextTime() const override;

    bool IsRetry() const override;
    TMutationId GetMutationId() const override;

    std::string GetService() const override;
    std::string GetMethod() const override;
    TRealmId GetRealmId() const override;
    const TAuthenticationIdentity& GetAuthenticationIdentity() const override;

    //! \note Thread affinity: any
    bool IsReplied() const override;

    void Reply(const TError& error) override;
    void Reply(const TSharedRefArray& responseMessage) override;
    using IServiceContext::Reply;

    void SetComplete() override;

    //! \note Thread affinity: any
    TFuture<TSharedRefArray> GetAsyncResponseMessage() const override;

    const TSharedRefArray& GetResponseMessage() const override;

    using TCanceledCallback = TCallback<void(const TError&)>;

    void SubscribeCanceled(const TCanceledCallback& callback) override;
    void UnsubscribeCanceled(const TCanceledCallback& callback) override;

    void SubscribeReplied(const TClosure& callback) override;
    void UnsubscribeReplied(const TClosure& callback) override;

    bool IsCanceled() const override;
    void Cancel() override;

    const TError& GetError() const override;

    TSharedRef GetRequestBody() const override;

    TSharedRef GetResponseBody() override;
    void SetResponseBody(const TSharedRef& responseBody) override;

    std::vector<TSharedRef>& RequestAttachments() override;
    NConcurrency::IAsyncZeroCopyInputStreamPtr GetRequestAttachmentsStream() override;

    std::vector<TSharedRef>& ResponseAttachments() override;
    NConcurrency::IAsyncZeroCopyOutputStreamPtr GetResponseAttachmentsStream() override;

    const NProto::TRequestHeader& RequestHeader() const override;
    NProto::TRequestHeader& RequestHeader() override;

    bool IsLoggingEnabled() const override;
    void SetRawRequestInfo(TString info, bool incremental) override;
    void SuppressMissingRequestInfoCheck() override;
    void SetRawResponseInfo(TString info, bool incremental) override;

    const IMemoryUsageTrackerPtr& GetMemoryUsageTracker() const override;

    const NLogging::TLogger& GetLogger() const override;
    NLogging::ELogLevel GetLogLevel() const override;

    bool IsPooled() const override;

    NCompression::ECodec GetResponseCodec() const override;
    void SetResponseCodec(NCompression::ECodec codec) override;

    bool IsResponseBodySerializedWithCompression() const override;
    void SetResponseBodySerializedWithCompression() override;

protected:
    std::unique_ptr<NProto::TRequestHeader> RequestHeader_;
    TSharedRefArray RequestMessage_;

    TMemoryUsageTrackerGuard RequestMemoryGuard_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;

    const NLogging::TLogger Logger;
    const NLogging::ELogLevel LogLevel_;

    // Set in #Initialize.
    bool LoggingEnabled_;
    TRequestId RequestId_;
    TRealmId RealmId_;

    TAuthenticationIdentity AuthenticationIdentity_;

    TSharedRef RequestBody_;
    std::vector<TSharedRef> RequestAttachments_;

    std::atomic<bool> Replied_ = false;
    TError Error_;

    i64 TotalSize_ = 0;

    TSharedRef ResponseBody_;
    std::vector<TSharedRef> ResponseAttachments_;

    bool RequestInfoSet_ = false;
    TCompactVector<TString, 4> RequestInfos_;
    TCompactVector<TString, 4> ResponseInfos_;

    NCompression::ECodec ResponseCodec_ = NCompression::ECodec::None;
    // COMPAT(danilalexeev)
    bool ResponseBodySerializedWithCompression_ = false;

    TSingleShotCallbackList<void()> RepliedList_;

    TServiceContextBase(
        std::unique_ptr<NProto::TRequestHeader> header,
        TSharedRefArray requestMessage,
        TMemoryUsageTrackerGuard memoryGuard,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        NLogging::TLogger logger,
        NLogging::ELogLevel logLevel);
    TServiceContextBase(
        TSharedRefArray requestMessage,
        TMemoryUsageTrackerGuard memoryGuard,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        NLogging::TLogger logger,
        NLogging::ELogLevel logLevel);

    virtual void DoReply() = 0;
    virtual void DoFlush();

    virtual void LogRequest() = 0;
    virtual void LogResponse() = 0;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ResponseLock_);
    TSharedRefArray ResponseMessage_; // cached
    mutable TPromise<TSharedRefArray> AsyncResponseMessage_; // created on-demand


    void Initialize();
    TSharedRefArray BuildResponseMessage();
    void ReplyEpilogue();
};

////////////////////////////////////////////////////////////////////////////////

class TServiceContextWrapper
    : public virtual IServiceContext
{
public:
    explicit TServiceContextWrapper(IServiceContextPtr underlyingContext);

    const NProto::TRequestHeader& GetRequestHeader() const override;
    TSharedRefArray GetRequestMessage() const override;

    NRpc::TRequestId GetRequestId() const override;
    NYT::NBus::TBusNetworkStatistics GetBusNetworkStatistics() const override;
    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override;
    const TString& GetEndpointDescription() const override;

    std::optional<TInstant> GetStartTime() const override;
    std::optional<TDuration> GetTimeout() const override;
    TInstant GetArriveInstant() const override;
    std::optional<TInstant> GetRunInstant() const override;
    std::optional<TInstant> GetFinishInstant() const override;
    std::optional<TDuration> GetWaitDuration() const override;
    std::optional<TDuration> GetExecutionDuration() const override;

    NTracing::TTraceContextPtr GetTraceContext() const override;
    std::optional<TDuration> GetTraceContextTime() const override;

    bool IsRetry() const override;
    TMutationId GetMutationId() const override;

    std::string GetService() const override;
    std::string GetMethod() const override;
    TRealmId GetRealmId() const override;
    const TAuthenticationIdentity& GetAuthenticationIdentity() const override;

    i64 GetTotalSize() const override;

    bool IsReplied() const override;
    void Reply(const TError& error) override;
    void Reply(const TSharedRefArray& responseMessage) override;

    void SetComplete() override;

    TFuture<TSharedRefArray> GetAsyncResponseMessage() const override;
    const TSharedRefArray& GetResponseMessage() const override;

    using TCanceledCallback = TCallback<void(const TError&)>;

    void SubscribeCanceled(const TCanceledCallback& callback) override;
    void UnsubscribeCanceled(const TCanceledCallback& callback) override;

    void SubscribeReplied(const TClosure& callback) override;
    void UnsubscribeReplied(const TClosure& callback) override;

    bool IsCanceled() const override;
    void Cancel() override;

    const TError& GetError() const override;

    TSharedRef GetRequestBody() const override;

    TSharedRef GetResponseBody() override;
    void SetResponseBody(const TSharedRef& responseBody) override;

    std::vector<TSharedRef>& RequestAttachments() override;
    NConcurrency::IAsyncZeroCopyInputStreamPtr GetRequestAttachmentsStream() override;

    std::vector<TSharedRef>& ResponseAttachments() override;
    NConcurrency::IAsyncZeroCopyOutputStreamPtr GetResponseAttachmentsStream() override;

    const NProto::TRequestHeader& RequestHeader() const override;

    NProto::TRequestHeader& RequestHeader() override;

    bool IsLoggingEnabled() const override;
    void SetRawRequestInfo(TString info, bool incremental) override;
    void SuppressMissingRequestInfoCheck() override;
    void SetRawResponseInfo(TString info, bool incremental) override;

    const IMemoryUsageTrackerPtr& GetMemoryUsageTracker() const override;

    const NLogging::TLogger& GetLogger() const override;
    NLogging::ELogLevel GetLogLevel() const override;

    bool IsPooled() const override;

    NCompression::ECodec GetResponseCodec() const override;
    void SetResponseCodec(NCompression::ECodec codec) override;

    bool IsResponseBodySerializedWithCompression() const override;
    void SetResponseBodySerializedWithCompression() override;

    const IServiceContextPtr& GetUnderlyingContext() const;

private:
    const IServiceContextPtr UnderlyingContext_;
};

////////////////////////////////////////////////////////////////////////////////

class TServerBase
    : public IServer
{
public:
    void RegisterService(IServicePtr service) override;
    bool UnregisterService(IServicePtr service) override;

    IServicePtr FindService(const TServiceId& serviceId) const override;
    IServicePtr GetServiceOrThrow(const TServiceId& serviceId) const override;

    void Configure(const TServerConfigPtr& config) override;
    void OnDynamicConfigChanged(const TServerDynamicConfigPtr& config) override;

    void Start() override;
    TFuture<void> Stop(bool graceful) override;

protected:
    const NLogging::TLogger Logger;

    std::atomic<bool> Started_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ServicesLock_);
    TServerConfigPtr StaticConfig_;
    TServerDynamicConfigPtr DynamicConfig_ = New<TServerDynamicConfig>();
    TServerConfigPtr AppliedConfig_;

    //! Service name to service.
    using TServiceMap = THashMap<TString, IServicePtr>;
    THashMap<TGuid, TServiceMap> RealmIdToServiceMap_;

    explicit TServerBase(NLogging::TLogger logger);

    void ApplyConfig();

    virtual void DoStart();
    virtual TFuture<void> DoStop(bool graceful);

    virtual void DoRegisterService(const IServicePtr& service);
    virtual void DoUnregisterService(const IServicePtr& service);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
