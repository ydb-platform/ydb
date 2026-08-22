#pragma once

#include <util/generic/ptr.h>

#include <memory>

namespace NActors {

////////////////////////////////////////////////////////////////////////////////

struct TActorContext;

class IActor;
using IActorPtr = std::unique_ptr<IActor>;

class IEventBase;
using IEventBasePtr = std::unique_ptr<IEventBase>;

class IEventHandle;
using IEventHandlePtr = std::unique_ptr<IEventHandle>;

}   // namespace NActors

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

struct IActorSystem;
using IActorSystemPtr = TIntrusivePtr<IActorSystem>;

}   // namespace NYdb::NBS
