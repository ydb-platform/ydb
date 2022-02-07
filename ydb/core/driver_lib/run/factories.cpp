#include "factories.h"

namespace NKikimr {

TModuleFactories::~TModuleFactories() {
    if (PqCmConnections) {
        PqCmConnections->Stop(true);
    }
}

} // namespace NKikimr
