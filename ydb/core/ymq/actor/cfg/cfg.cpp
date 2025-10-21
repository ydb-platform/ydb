#include "cfg.h"

#include <ydb/core/base/appdata.h>

namespace NKikimr::NSQS {

const NKikimrConfig::TSqsConfig& Cfg() {
    return AppData()->SqsConfig;
}

ui32 GetLeadersDescriberUpdateTimeMs() {
    const auto& config = AppData()->SqsConfig;
    if (config.HasMastersDescriberUpdateTimeMs()) {
        return config.GetMastersDescriberUpdateTimeMs();
    }
    return config.GetLeadersDescriberUpdateTimeMs();
}

} // namespace NKikimr::NSQS
