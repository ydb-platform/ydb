#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/services/metadata/request/config.h>
#include <ydb/core/protos/config.pb.h>
#include <util/datetime/base.h>

namespace NKikimr::NMetadataProvider {

class TConfig {
private:
    YDB_READONLY_DEF(NInternal::NRequest::TConfig, RequestConfig);
    YDB_READONLY(TDuration, RefreshPeriod, TDuration::Seconds(10));
    YDB_READONLY_FLAG(Enabled, true);
public:
    TConfig() = default;

    TDuration GetRetryPeriod(const ui32 retry) const;
    bool DeserializeFromProto(const NKikimrConfig::TMetadataProviderConfig& config);
};
}
