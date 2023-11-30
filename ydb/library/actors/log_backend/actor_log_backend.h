#pragma once
#include <library/cpp/logger/backend.h>

namespace NActors {
class TActorSystem;
} // namespace NActors

class TActorLogBackend : public TLogBackend {
public:
    TActorLogBackend(NActors::TActorSystem* actorSystem, int logComponent);

    void WriteData(const TLogRecord& rec) override;

    void ReopenLog() override {
    }

private:
    NActors::TActorSystem* const ActorSystem;
    const int LogComponent;
};
