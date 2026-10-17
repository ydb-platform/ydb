#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/control/lib/immediate_control_board_wrapper.h>
#include <ydb/core/kqp/common/kqp_executer_mutable_config.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/table_service_config.pb.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <memory>

namespace NKikimr::NKqp {

struct TKqpWorkerSettings {
    TString Cluster;
    TString Database;
    TMaybe<TString> ApplicationName;
    TMaybe<TString> UserName;
    bool LongSession = false;

    TIntrusivePtr<TExecuterMutableConfig> MutableExecuterConfig;

private:
    std::shared_ptr<const NKikimrConfig::TTableServiceConfig> TableServicePtr;
    std::shared_ptr<const NKikimrConfig::TQueryServiceConfig> QueryServicePtr;
    std::shared_ptr<const NKikimrConfig::TTliConfig> TliConfigPtr;

public:
    const NKikimrConfig::TTableServiceConfig& TableService;
    const NKikimrConfig::TQueryServiceConfig& QueryService;
    const NKikimrConfig::TTliConfig& TliConfig;

    TControlWrapper MkqlInitialMemoryLimit;
    TControlWrapper MkqlMaxMemoryLimit;

    TKqpDbCountersPtr DbCounters;

    explicit TKqpWorkerSettings(const TString& cluster, const TString& database,
            const TMaybe<TString>& applicationName, const TMaybe<TString>& userName,
            const TIntrusivePtr<TExecuterMutableConfig> mutableExecuterConfig,
            std::shared_ptr<const NKikimrConfig::TTableServiceConfig> tableServiceConfig,
            std::shared_ptr<const NKikimrConfig::TQueryServiceConfig> queryServiceConfig,
            std::shared_ptr<const NKikimrConfig::TTliConfig> tliConfig,
            TControlWrapper mkqlInitialMemoryLimit,
            TControlWrapper mkqlMaxMemoryLimit,
            TKqpDbCountersPtr dbCounters)
        : Cluster(cluster)
        , Database(database)
        , ApplicationName(applicationName)
        , UserName(userName)
        , MutableExecuterConfig(mutableExecuterConfig)
        , TableServicePtr(std::move(tableServiceConfig))
        , QueryServicePtr(std::move(queryServiceConfig))
        , TliConfigPtr(std::move(tliConfig))
        , TableService(*TableServicePtr)
        , QueryService(*QueryServicePtr)
        , TliConfig(*TliConfigPtr)
        , MkqlInitialMemoryLimit(mkqlInitialMemoryLimit)
        , MkqlMaxMemoryLimit(mkqlMaxMemoryLimit)
        , DbCounters(dbCounters)
    {}

    TKqpWorkerSettings(const TKqpWorkerSettings& other)
        : Cluster(other.Cluster)
        , Database(other.Database)
        , ApplicationName(other.ApplicationName)
        , UserName(other.UserName)
        , LongSession(other.LongSession)
        , MutableExecuterConfig(other.MutableExecuterConfig)
        , TableServicePtr(other.TableServicePtr)
        , QueryServicePtr(other.QueryServicePtr)
        , TliConfigPtr(other.TliConfigPtr)
        , TableService(*TableServicePtr)
        , QueryService(*QueryServicePtr)
        , TliConfig(*TliConfigPtr)
        , MkqlInitialMemoryLimit(other.MkqlInitialMemoryLimit)
        , MkqlMaxMemoryLimit(other.MkqlMaxMemoryLimit)
        , DbCounters(other.DbCounters)
    {}
};

} // namespace NKikimr::NKqp
