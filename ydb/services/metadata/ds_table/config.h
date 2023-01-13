#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/services/metadata/request/config.h>
#include <ydb/core/protos/config.pb.h>
#include <util/datetime/base.h>

namespace NKikimr::NMetadata::NProvider {

class TConfig {
private:
    YDB_READONLY_DEF(NRequest::TConfig, RequestConfig);
    YDB_READONLY(TDuration, RefreshPeriod, TDuration::Seconds(10));
    YDB_READONLY_FLAG(Enabled, true);
    YDB_READONLY(TString, Path, ".metadata");
public:
    TConfig() = default;

    bool DeserializeFromProto(const NKikimrConfig::TMetadataProviderConfig& config);
};
}
