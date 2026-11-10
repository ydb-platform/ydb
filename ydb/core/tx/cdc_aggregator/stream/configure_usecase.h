#pragma once

#include "stream_repository.h"

namespace NKikimr::NCdcAggregator {

struct TConfigureCommand {
    TStreamConfig Config;
};

struct TConfigureResult {
    enum class EStatus {
        Success,
        BadRequest,
        Conflict,
    };

    EStatus Status = EStatus::Success;
    TString Error;
    // Version of the stored configuration; zero when there is none.
    ui64 ConfigVersion = 0;
};

// Sets the initial configuration. An identical repeat succeeds without
// touching the progress; a different configuration is never applied silently.
TConfigureResult ConfigureStream(TStream& stream, IStreamRepository& repository, TConfigureCommand command);

} // namespace NKikimr::NCdcAggregator
