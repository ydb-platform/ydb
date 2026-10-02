#pragma once

#include <yt/yql/providers/yt/lib/access_provider/yt_access_provider.h>

namespace NYql {

IYtAccessProvider::TPtr CreateYtDummyAccessProvider();

}; // namespace NYql
