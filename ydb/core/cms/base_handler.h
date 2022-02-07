#pragma once

#include "defs.h"

#include <library/cpp/actors/core/mon.h>

namespace NKikimr {
namespace NCms {

class TApiMethodHandlerBase {
public:
    virtual ~TApiMethodHandlerBase() = default;

    virtual IActor *CreateHandlerActor(NMon::TEvHttpInfo::TPtr &event) = 0;
};

} // NCms
} // NKikimr
