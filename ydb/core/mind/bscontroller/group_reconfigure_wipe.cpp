#include "impl.h"

namespace NKikimr::NBsController {

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerGroupReconfigureWipe::TPtr& ev) {
    SendInReply(*ev, std::make_unique<TEvBlobStorage::TEvControllerGroupReconfigureWipeResult>(NKikimrProto::ERROR,
        "unsupported operation"));
}

}
