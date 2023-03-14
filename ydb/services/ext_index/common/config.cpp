#include "config.h"
#include <ydb/core/base/appdata.h>

namespace NKikimr::NCSIndex {

bool TConfig::DeserializeFromProto(const NKikimrConfig::TExternalIndexConfig& config) {
    EnabledFlag = config.GetEnabled();
    if (!RequestConfig.DeserializeFromProto(config.GetRequestConfig())) {
        return false;
    }
    if (config.HasInternalTablePath()) {
        InternalTablePath = config.GetInternalTablePath();
    }
    return true;
}

TString TConfig::GetTablePath() const {
    return AppData()->TenantName + "/" + InternalTablePath;
}

}
