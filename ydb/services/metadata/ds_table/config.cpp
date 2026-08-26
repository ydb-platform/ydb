#include "config.h"
#include <ydb/library/actors/core/log.h>
#include <util/generic/ylimits.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::METADATA_PROVIDER

namespace NKikimr::NMetadata::NProvider {

bool TConfig::DeserializeFromProto(const NKikimrConfig::TMetadataProviderConfig& config) {
    if (config.HasPath()) {
        Path = config.GetPath();
    }
    if (!Path) {
        YDB_LOG_ERROR("Incorrect path for metadata - empty");
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
