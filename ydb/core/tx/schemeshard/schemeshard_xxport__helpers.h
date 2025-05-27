#pragma once

#include <util/generic/string.h>

namespace Ydb::Operations {
    class OperationParams;
}

namespace NKikimr::NSchemeShard {

TString GetUid(const Ydb::Operations::OperationParams& operationParams);

}  // NKikimr::NSchemeShard
