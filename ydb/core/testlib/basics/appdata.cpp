#include "appdata.h"

#include <ydb/core/sys_view/factory.h>

namespace NKikimr {

TAppPrepare::TAppPrepare(std::shared_ptr<NDataShard::IExportFactory> ef)
    : TAppPrepare(TLightweightTag{}, std::move(ef))
{
    Mine->IoContext = std::make_shared<NPDisk::TIoContextFactoryOSS>();
    Mine->SchemeOperationFactory.reset(NSchemeShard::DefaultOperationFactory());
    Mine->SystemViewScanFactory.reset(NSysView::CreateSystemViewScanFactory());
}

}
