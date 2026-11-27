#pragma once

#include "scan_factory/scan_factory.h"

namespace NKikimr::NSysView {

ISystemViewScanFactory* CreateSystemViewScanFactory();

} // namespace NKikimr::NSysView
