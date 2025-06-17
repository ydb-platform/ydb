#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NLogin {
    class TLoginProvider;
}

namespace NKikimr::NSchemeShard {

THolder<NActors::IActor> CreateLoginHelper(const NLogin::TLoginProvider& loginProvider);

} // namespace NKikimr::NSchemeShard
