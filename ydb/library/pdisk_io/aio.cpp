#include "aio.h"
#include "file_params.h"

namespace NKikimr {
namespace NPDisk {

std::unique_ptr<IAsyncIoContext> TIoContextFactoryOSS::CreateAsyncIoContext(const TString &path, ui32 pDiskId,
            TDeviceMode::TFlags flags, TIntrusivePtr<TSectorMap> sectorMap) const {
    if (sectorMap) {
        return CreateAsyncIoContextMap(path, pDiskId, sectorMap);
    } else {
        return CreateAsyncIoContextReal(path, pDiskId, flags);
    }
};

ISpdkState *TIoContextFactoryOSS::CreateSpdkState() const {
    return Singleton<TSpdkStateOSS>();
}

void TIoContextFactoryOSS::DetectFileParameters(const TString &path, ui64 &outDiskSizeBytes, bool &outIsBlockDevice) const {
    ::NKikimr::DetectFileParameters(path, outDiskSizeBytes, outIsBlockDevice);
}


} // NPDisk
} // NKikimr
