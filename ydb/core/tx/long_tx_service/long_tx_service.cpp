#include "long_tx_service.h"
#include "long_tx_service_impl.h"

namespace NKikimr {
namespace NLongTxService {

    IActor* CreateLongTxService(const TLongTxServiceSettings& settings) {
        return new TLongTxServiceActor(settings);
    }

} // namespace NLongTxService
} // namespace NKikimr
