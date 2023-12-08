#pragma once

#include "yql_http_header.h"

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/retry/retry_policy.h>

#include <contrib/libs/curl/include/curl/curl.h>

#include <atomic>
#include <variant>
#include <functional>

namespace NYql {

class IHTTPGateway {
public:
    using TPtr = std::shared_ptr<IHTTPGateway>;
    using TWeakPtr = std::weak_ptr<IHTTPGateway>;
    using TRetryPolicy = IRetryPolicy<CURLcode, long>;

    virtual ~IHTTPGateway() = default;

    // Makes http gateways.
    // Throws on error.
    static TPtr Make(
        const THttpGatewayConfig* httpGatewaysCfg = nullptr,
        ::NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<::NMonitoring::TDynamicCounters>());

    class TContentBase : protected TString {
    protected:
        TContentBase(TString&& data);
        TContentBase(const TString& data);

        TContentBase(TContentBase&&) = default;
        TContentBase& operator=(TContentBase&&) = default;

        ~TContentBase();
        TString Extract();
    public:
        using TString::size;
        using TString::data;

        inline operator std::string_view() const { return { data(), size() }; }
    private:
        TContentBase(const TContentBase&) = delete;
        TContentBase& operator=(const TContentBase&) = delete;
    };

    class TContent : public TContentBase {
    friend class TEasyCurl;
    public:
        TContent(TString&& data, long httpResponseCode = 0LL, TString&& headers = {});
        TContent(const TString& data, long httpResponseCode = 0LL, const TString& headers = {});

        TContent(TContent&& src) = default;
        TContent& operator=(TContent&& src) = default;

        using TContentBase::Extract;
    public:
        TString Headers;
        long HttpResponseCode;
    };

    using THeaders = THttpHeader;
    struct TResult {
        TResult(TContent&& content) : Content(std::move(content)), CurlResponseCode(CURLE_OK) {}
        TResult(CURLcode curlResponseCode, const TIssues& issues) : Content(""), CurlResponseCode(curlResponseCode), Issues(issues) {}
        TContent Content;
        CURLcode CurlResponseCode;
        TIssues Issues;
    };
    using TOnResult = std::function<void(TResult&&)>;

    virtual void Upload(
        TString url,
        THeaders headers,
        TString body,
        TOnResult callback,
        bool put = false,
        TRetryPolicy::TPtr retryPolicy = TRetryPolicy::GetNoRetryPolicy()) = 0;

    virtual void Delete(
        TString url,
        THeaders headers,
        TOnResult callback,
        TRetryPolicy::TPtr retryPolicy = TRetryPolicy::GetNoRetryPolicy()) = 0;

    virtual void Download(
        TString url,
        THeaders headers,
        std::size_t offset,
        std::size_t sizeLimit,
        TOnResult callback,
        TString data = {},
        TRetryPolicy::TPtr retryPolicy = TRetryPolicy::GetNoRetryPolicy()) = 0;

    class TCountedContent : public TContentBase {
    public:
        TCountedContent(TString&& data, const std::shared_ptr<std::atomic_size_t>& counter, const ::NMonitoring::TDynamicCounters::TCounterPtr& inflightCounter);
        ~TCountedContent();

        TCountedContent(TCountedContent&&) = default;
        TCountedContent& operator=(TCountedContent&& src) = default;

        TString Extract();
    private:
        const std::shared_ptr<std::atomic_size_t> Counter;
        const ::NMonitoring::TDynamicCounters::TCounterPtr InflightCounter;
    };

    using TOnDownloadStart = std::function<void(CURLcode, long)>; // http code.
    using TOnNewDataPart = std::function<void(TCountedContent&&)>;
    using TOnDownloadFinish = std::function<void(CURLcode, TIssues)>;
    using TCancelHook = std::function<void(TIssue)>;

    virtual TCancelHook Download(
        TString url,
        THeaders headers,
        std::size_t offset,
        std::size_t sizeLimit,
        TOnDownloadStart onStart,
        TOnNewDataPart onNewData,
        TOnDownloadFinish onFinish,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& inflightCounter) = 0;
        
    virtual ui64 GetBuffersSizePerStream() = 0;

    static THeaders MakeYcHeaders(
        const TString& requestId,
        const TString& token = {},
        const TString& contentType = {},
        const TString& userPwd = {},
        const TString& awsSigV4 = {});
};

}
