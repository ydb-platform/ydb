#include "config.h"

namespace NKikimr::NBsController {

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDefineDDiskPool& /*cmd*/, TStatus& /*status*/) {
        throw TExError() << "not implemented";
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TReadDDiskPool& /*cmd*/, TStatus& /*status*/) {
        throw TExError() << "not implemented";
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDeleteDDiskPool& /*cmd*/, TStatus& /*status*/) {
        throw TExError() << "not implemented";
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TMoveDDisk& /*cmd*/, TStatus& /*status*/) {
        throw TExError() << "not implemented";
    }

} // NKikimr::NBsController
