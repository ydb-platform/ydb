#include "appdata.h"

namespace NKikimr {

TAppPrepare::TAppPrepare(std::shared_ptr<NDataShard::IExportFactory> ef)
    : TAppPrepare(TLightweightTag{}, std::move(ef))
{
    Mine->IoContext = std::make_shared<NPDisk::TIoContextFactoryOSS>();
    Mine->SchemeOperationFactory.reset(NSchemeShard::DefaultOperationFactory());
}

}
