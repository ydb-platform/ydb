#pragma once

#include "yql_yt_file_services.h"

#include <ydb/library/yql/providers/yt/provider/yql_yt_gateway.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/folder/dirut.h>
#include <util/system/mutex.h>

#include <vector>

namespace NKikimr {
namespace NMiniKQL {
class IComputationPatternCache;
}
}

namespace NYql {

IYtGateway::TPtr CreateYtFileGateway(const NFile::TYtFileServices::TPtr& services,
    bool* emulateOutputForMultirunPtr = nullptr);

}
