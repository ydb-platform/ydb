#pragma once

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NDbPool {

class TConfig;
class TDbPoolMap;
class TDbPool;
using TDbPoolPtr = std::shared_ptr<TDbPool>;

class TDbPoolHolder final : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TDbPoolHolder>;

    TDbPoolHolder(const NDbPool::TConfig& config, NYdb::TDriver driver, NKikimr::TYdbCredentialsProviderFactory credentialsProviderFactory, NMonitoring::TDynamicCounterPtr counters);

    ~TDbPoolHolder();

    TDbPoolPtr GetOrCreate(ui32 poolId);

private:
    NYdb::TDriver Driver;
    const TIntrusivePtr<TDbPoolMap> Pools;
};

NYdb::TAsyncStatus ExecDbRequest(TDbPoolPtr dbPool, std::function<NYdb::TAsyncStatus(NYdb::NTable::TSession&)> handler);

} // namespace NDbPool
