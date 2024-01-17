#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/record.h>


namespace NKikimr {

// Log backend that allows us to create shared YDB driver early (before actor system starts),
// but log to actor system.
class TDeferredActorLogBackend : public TLogBackend {
public:
    using TAtomicActorSystemPtr = std::atomic<NActors::TActorSystem*>;
    using TSharedAtomicActorSystemPtr = std::shared_ptr<TAtomicActorSystemPtr>;

    TDeferredActorLogBackend(TSharedAtomicActorSystemPtr actorSystem, int logComponent);

    NActors::NLog::EPriority GetActorLogPriority(ELogPriority priority) const;

    void WriteData(const TLogRecord& rec) override;

    void ReopenLog() override {}

protected:
    TSharedAtomicActorSystemPtr ActorSystemPtr;
    const int LogComponent;
};

} // NKikimr

