#include "cli_factories.h"

#include <util/generic/singleton.h>

namespace NYdb::NConsoleClient {

TAppData* AppData() {
    return Singleton<TAppData>();
}

} // namespace NYdb::NConsoleClient
