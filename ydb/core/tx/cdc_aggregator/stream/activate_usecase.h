#pragma once

#include "stream_repository.h"

namespace NKikimr::NCdcAggregator {

struct TActivateCommand {
    TPathId StreamPathId;
    ui64 ConfigVersion = 0;
};

struct TActivateResult {
    enum class EStatus {
        Success,
        NotReady,
        WrongStream,
        Conflict,
    };

    EStatus Status = EStatus::Success;
    TString Error;
    // The phase changed by this command, so publication may start once it is
    // committed.
    bool Activated = false;
};

// Opens publication of the barrier after SchemeShard has configured every
// participant. Reports do not wait for activation.
TActivateResult ActivateStream(TStream& stream, IStreamRepository& repository, const TActivateCommand& command);

} // namespace NKikimr::NCdcAggregator
