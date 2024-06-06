#pragma once

#include <ydb/core/protos/drivemodel.pb.h>

#include <util/generic/hash.h>
#include <util/generic/list.h>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Drive Model DB
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TDriveModelDb : public TThrRefBase {
public:
    TDriveModelDb();
    void Merge(const NKikimrBlobStorage::TDriveModelList &modelList);
//    TIntrusivePtr<TDriveModel> MakeDriveModel(TString ModelNumber, bool isWriteCacheEnabled, bool isSharedWithOs,
//            bool isSolidState);
public:
    using TModelMap = THashMap<TString, TList<NKikimrBlobStorage::TDriveModel>>;

    TModelMap ModelMap;
};

}
}
