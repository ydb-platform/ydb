#include "blobstorage_vdisk_guids.h"

#include <ydb/core/base/appdata.h>
#include <library/cpp/random_provider/random_provider.h>

namespace NKikimr {

    TVDiskIncarnationGuid TVDiskIncarnationGuid::Generate() {
        ui64 guid = TAppData::RandomProvider->GenRand64();
        return TVDiskIncarnationGuid(guid);
    }

    TVDiskEternalGuid TVDiskEternalGuid::Generate() {
        ui64 guid = TAppData::RandomProvider->GenRand64();
        return TVDiskEternalGuid(guid);
    }

} // NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::TVDiskIncarnationGuid, stream, value) {
    value.Output(stream);
}

Y_DECLARE_OUT_SPEC(, NKikimr::TVDiskEternalGuid, stream, value) {
    value.Output(stream);
}
