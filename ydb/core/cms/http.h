#pragma once

#include "defs.h"

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NCms {

IActor *CreateCmsHttp();

} // namespace NKikimr::NCms
