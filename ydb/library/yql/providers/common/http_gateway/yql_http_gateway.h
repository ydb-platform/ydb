#pragma once

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h> 
 
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/monlib/dynamic_counters/counters.h> 

#include <variant>
#include <functional>

namespace NYql {

class IHTTPGateway {
public:
    using TPtr = std::shared_ptr<IHTTPGateway>;
    using TWeakPtr = std::weak_ptr<IHTTPGateway>;

    virtual ~IHTTPGateway() = default;

    template<bool SingleThread = true>
    static TPtr Make( 
        const THttpGatewayConfig* httpGatewaysCfg = nullptr, 
        NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<NMonitoring::TDynamicCounters>()); 

    class TContent : private TString {
    friend class TEasyCurl;
    public:
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
    };

    using TResult = std::variant<TContent, TIssues>;
    using TOnResult = std::function<void(TResult&&)>;
    using THeaders = TSmallVec<TString>;

    virtual void Download(TString url, THeaders headers, std::size_t expectedSize, TOnResult callback, TString data = {}) = 0;
};

}
