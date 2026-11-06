#pragma once

#include "status.h"

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NKikimr::NColumnShard::NLocalDbBackup {

TString RenderJson(const TStatus& status, TInstant now);

}   // namespace NKikimr::NColumnShard::NLocalDbBackup
