#pragma once

#include "defs.h"

#include <ydb/library/actors/core/mon.h>

namespace NKikimr::NCms {

class TApiMethodHandlerBase {
public:
    virtual ~TApiMethodHandlerBase() = default;

    virtual IActor *CreateHandlerActor(NMon::TEvHttpInfo::TPtr &event) = 0;
};

} // namespace NKikimr::NCms
