#pragma once

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <util/generic/noncopyable.h>


namespace NKikimr::NKqp {

class IQueryReplayBackend : public TNonCopyable {
public:

    /// Collect details about query:
    /// Accepts query text
    virtual void Collect(const TString& queryData) = 0;

    virtual ~IQueryReplayBackend() {};

    //// Updates configuration onn running backend, if applicable.
    virtual void UpdateConfig(const NKikimrConfig::TTableServiceConfig& serviceConfig) = 0;
};


class TNullQueryReplayBackend : public IQueryReplayBackend {
public:
    void Collect(const TString&) {
    }

    virtual void UpdateConfig(const NKikimrConfig::TTableServiceConfig&) {
    }

    ~TNullQueryReplayBackend() {
    }
};

class IQueryReplayBackendFactory {
public:
    virtual ~IQueryReplayBackendFactory() {}
    virtual IQueryReplayBackend *Create(
        const NKikimrConfig::TTableServiceConfig& serviceConfig,
        TIntrusivePtr<TKqpCounters> counters) = 0;
};

inline IQueryReplayBackend* CreateQueryReplayBackend(
        const NKikimrConfig::TTableServiceConfig& serviceConfig,
        TIntrusivePtr<TKqpCounters> counters,
        std::shared_ptr<IQueryReplayBackendFactory> factory) {
    if (!factory) {
        return new TNullQueryReplayBackend();
    } else {
        return factory->Create(serviceConfig, std::move(counters));
    }
}

}
