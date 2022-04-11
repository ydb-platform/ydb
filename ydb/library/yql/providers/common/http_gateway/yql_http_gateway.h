#pragma once

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/retry/retry_policy.h>
#include <library/cpp/threading/task_scheduler/task_scheduler.h>

#include <variant>
#include <functional>

namespace NYql {

class IHTTPGateway {
public:
    using TPtr = std::shared_ptr<IHTTPGateway>;
    using TWeakPtr = std::weak_ptr<IHTTPGateway>;

    virtual ~IHTTPGateway() = default;

    static TPtr Make(
        const THttpGatewayConfig* httpGatewaysCfg = nullptr,
        NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<NMonitoring::TDynamicCounters>());

    class TContent : private TString {
    friend class TEasyCurl;
    public:
        TContent(TString&& data, long httpResponseCode);
        TContent(const TString& data, long httpResponseCode);
        TContent(TString&& data);
        TContent(const TString& data);

        TString Extract();
        ~TContent();

        TContent(TContent&&) = default;
        TContent& operator=(TContent&&) = default;

        using TString::size;
        using TString::data;

        inline operator std::string_view() const { return { data(), size() }; }
    private:
        TContent(const TContent&) = delete;
        TContent& operator=(const TContent&) = delete;

    public:
        long HttpResponseCode;
    };

    using TResult = std::variant<TContent, TIssues>;
    using TOnResult = std::function<void(TResult&&)>;
    using THeaders = TSmallVec<TString>;

    virtual void Download(
        TString url,
        THeaders headers,
        std::size_t expectedSize,
        TOnResult callback,
        TString data = {},
        IRetryPolicy</*http response code*/long>::TPtr RetryPolicy = IRetryPolicy<long>::GetNoRetryPolicy()
    ) = 0;

    using TOnNewDataPart = std::function<void(TContent&&)>;
    using TOnDowloadFinsh = std::function<void(std::optional<TIssues>)>;

    virtual void Download(
        TString url,
        THeaders headers,
        std::size_t offset,
        std::size_t expectedSize,
        TOnNewDataPart onNewData,
        TOnDowloadFinsh onFinish) = 0;
};

}
