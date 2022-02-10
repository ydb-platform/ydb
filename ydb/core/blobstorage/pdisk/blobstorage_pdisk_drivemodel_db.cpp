#include "blobstorage_pdisk_drivemodel_db.h"

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Drive Model DB
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TDriveModelDb::TDriveModelDb() {
};

void TDriveModelDb::Merge(const NKikimrBlobStorage::TDriveModelList &modelList) {
    for (size_t idx = 0; idx < modelList.DriveModelSize(); ++idx) {
        auto protoModel = modelList.GetDriveModel(idx);
        TList<NKikimrBlobStorage::TDriveModel> &models = ModelMap[protoModel.GetSourceModelNumber()];
        models.emplace_back();
        models.back().CopyFrom(protoModel);
    }
}

//TIntrusivePtr<TDriveModel> TDriveModelDb::MakeDriveModel(TString ModelNumber, bool isWriteCacheEnabled,
//        bool isSharedWithOs, bool isSolidState) {

} // NPDisk
} // NKikimr
