#pragma once

#include <ydb/core/base/defs.h>

namespace NKikimr::NReplication::NService {

IActor* CreateLocalTableWriter(const TString& path);

}
