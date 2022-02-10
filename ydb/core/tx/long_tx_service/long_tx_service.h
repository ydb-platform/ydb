#pragma once
#include "defs.h"

namespace NKikimr {
namespace NLongTxService {

    struct TLongTxServiceSettings {
        // TODO: add settings for long tx service
    };

    IActor* CreateLongTxService(const TLongTxServiceSettings& settings = {});

} // namespace NLongTxService
} // namespace NKikimr
