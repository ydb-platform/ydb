#include "config.h"
#include <ydb/library/actors/core/log.h>
#include <util/generic/ylimits.h>

namespace NKikimr::NMetadata::NProvider {

bool TConfig::DeserializeFromProto(const NKikimrConfig::TMetadataProviderConfig& config) {
    if (config.HasPath()) {
        Path = config.GetPath();
    }
    if (!Path) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "incorrect path for metadata - empty";
        return false;
    }
    EnabledFlag = config.GetEnabled();
    if (!RequestConfig.DeserializeFromProto(config.GetRequestConfig())) {
        return false;
    }
    RefreshPeriod = TDuration::Seconds(config.GetRefreshPeriodSeconds());
    return true;
}

}
