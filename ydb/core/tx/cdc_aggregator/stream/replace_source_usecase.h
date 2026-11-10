#pragma once

#include "stream_repository.h"

namespace NKikimr::NCdcAggregator {

struct TReplaceSourceCommand {
    TPathId StreamPathId;
    TSourceReplacement Replacement;
};

struct TReplaceSourceResult {
    enum class EStatus {
        Success,
        NotReady,
        WrongStream,
        UnknownSource,
        Conflict,
        BadRequest,
    };

    EStatus Status = EStatus::Success;
    TString Error;
    // The barrier committed together with the replacement, if it advanced.
    std::optional<TBarrier> AdvancedReadyBarrier;
};

// Replaces a split/merge parent with its children. A repeat of the same
// replacement succeeds without resetting progress of the children.
TReplaceSourceResult ReplaceStreamSource(TStream& stream, IStreamRepository& repository, TReplaceSourceCommand command);

} // namespace NKikimr::NCdcAggregator
