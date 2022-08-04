#pragma once

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/retry/retry_policy.h>

#include <atomic>
#include <variant>
#include <functional>

namespace NYql {

class IHTTPGateway {
public:
    using TPtr = std::shared_ptr<IHTTPGateway>;
    using TWeakPtr = std::weak_ptr<IHTTPGateway>;

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
        TContent(TString&& data, long httpResponseCode = 0LL);
        TContent(const TString& data, long httpResponseCode = 0LL);

        TContent(TContent&& src) = default;
        TContent& operator=(TContent&& src) = default;

        using TContentBase::Extract;
    public:
        long HttpResponseCode;
    };

    using THeaders = TSmallVec<TString>;
    using TResult = std::variant<TContent, TIssues>;
    using TOnResult = std::function<void(TResult&&)>;

    virtual void Upload(
        TString url,
        THeaders headers,
        TString body,
        TOnResult callback,
        bool put = false,
        IRetryPolicy</*http response code*/long>::TPtr RetryPolicy = IRetryPolicy<long>::GetNoRetryPolicy()) = 0;

    virtual void Download(
        TString url,
        THeaders headers,
        std::size_t expectedSize,
        TOnResult callback,
        TString data = {},
        IRetryPolicy</*http response code*/long>::TPtr RetryPolicy = IRetryPolicy<long>::GetNoRetryPolicy()
    ) = 0;

    class TCountedContent : public TContentBase {
    public:
        TCountedContent(TString&& data, const std::shared_ptr<std::atomic_size_t>& counter);
        ~TCountedContent();

        TCountedContent(TCountedContent&&) = default;
        TCountedContent& operator=(TCountedContent&& src) = default;

        TString Extract();
    private:
        const std::shared_ptr<std::atomic_size_t> Counter;
    };

    using TOnNewDataPart = std::function<void(TCountedContent&&)>;
    using TOnDownloadFinish = std::function<void(std::variant<long, TIssues>)>; // http code or issues.
    using TCancelHook = std::function<void(TIssue)>;

    virtual TCancelHook Download(
        TString url,
        THeaders headers,
        std::size_t offset,
        TOnNewDataPart onNewData,
        TOnDownloadFinish onFinish) = 0;
};

}
