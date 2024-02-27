#include "init.h"

#include <ydb/library/yql/utils/log/log.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/client/init.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/generic/singleton.h>
#include <util/system/compiler.h>
#include <util/system/env.h>

namespace NYql {

namespace {

class TInitYtApi {
public:
    TInitYtApi()
        : DeterministicMode_(GetEnv("YQL_DETERMINISTIC_MODE"))
    {
        YQL_CLOG(DEBUG, ProviderYt) << "Entering YT API init";
        if (NYT::NDetail::GetInitStatus() == NYT::NDetail::EInitStatus::NotInitialized) {
            YQL_CLOG(DEBUG, ProviderYt) << "Init jobless YT API";
            NYT::JoblessInitialize();
        }
        NYT::TConfig::Get()->RetryCount = 1 << 6;
        NYT::TConfig::Get()->StartOperationRetryCount = 1 << 10;
        if (GetEnv("YT_FORCE_IPV6").empty()) {
            NYT::TConfig::Get()->ForceIpV6 = true;
        }
        if (NYT::TConfig::Get()->Prefix.empty()) {
            NYT::TConfig::Get()->Prefix = "//";
        }
        NYT::TConfig::Get()->UseAsyncTxPinger = true;
        NYT::TConfig::Get()->AsyncHttpClientThreads = 2;
        NYT::TConfig::Get()->AsyncTxPingerPoolThreads = 2;
        YQL_CLOG(DEBUG, ProviderYt) << "Using YT global prefix: " << NYT::TConfig::Get()->Prefix;
    }

    void UpdateProps(const TMaybe<TString>& attrs) {
        if (!attrs) {
            return;
        }
        NYT::TNode node;
        try {
            node = NYT::NodeFromYsonString(*attrs);
            node.AsMap();
        } catch (...) {
            YQL_CLOG(ERROR, ProviderYt) << "Cannot parse yql operation attrs: " << CurrentExceptionMessage();
            return;
        }
        if (DeterministicMode_) {
            TString prefix;
            if (auto p = node.AsMap().FindPtr("test_prefix")) {
                prefix = p->AsString();
            }
            if (prefix) {
                if (!prefix.StartsWith("//")) {
                    prefix.prepend("//");
                }
                if (!prefix.EndsWith('/')) {
                    prefix.append('/');
                }
                if (NYT::TConfig::Get()->Prefix != prefix) {
                    NYT::TConfig::Get()->Prefix = prefix;
                    YQL_CLOG(DEBUG, ProviderYt) << "Using YT global prefix: " << prefix;
                }
            }
        }
        if (auto p = node.AsMap().FindPtr("poller_interval")) {
            try {
                if (auto d = TDuration::Parse(p->AsString()); d >= TDuration::MilliSeconds(200) && d <= TDuration::Seconds(10)) {
                    NYT::TConfig::Get()->WaitLockPollInterval = d;
                    YQL_CLOG(DEBUG, ProviderYt) << "Set poller_interval to " << d;
                } else {
                    YQL_CLOG(ERROR, ProviderYt) << "Invalid poller_interval value " << p->AsString();
                    throw yexception() << "Invalid poller_interval value " << p->AsString();
                }
            } catch (...) {
                YQL_CLOG(ERROR, ProviderYt) << "Cannot parse poller_interval: " << CurrentExceptionMessage();
                throw;
            }
        }
    }
private:
    const bool DeterministicMode_;
};

}

void InitYtApiOnce(const TMaybe<TString>& attrs) {
    Singleton<TInitYtApi>()->UpdateProps(attrs);
}

} // NYql
