#include "activate_usecase.h"

#include <util/string/builder.h>

namespace NKikimr::NCdcAggregator {

TActivateResult ActivateStream(TStream& stream, IStreamRepository& repository, const TActivateCommand& command) {
    TActivateResult result;
    if (!stream.IsConfigured()) {
        result.Status = TActivateResult::EStatus::NotReady;
        result.Error = "Stream is not configured";
        return result;
    }
    if (command.StreamPathId != stream.GetConfig().StreamPathId) {
        result.Status = TActivateResult::EStatus::WrongStream;
        result.Error = TStringBuilder() << "Aggregator serves stream " << stream.GetConfig().StreamPathId;
        return result;
    }

    TStreamChanges changes;
    switch (stream.Activate(command.ConfigVersion, changes)) {
        case EActivateOutcome::Activated:
            repository.Save(stream, changes);
            result.Activated = true;
            break;
        case EActivateOutcome::AlreadyActive:
            break;
        case EActivateOutcome::VersionMismatch:
            result.Status = TActivateResult::EStatus::Conflict;
            result.Error = TStringBuilder() << "Configuration version is " << stream.GetConfig().Version
                << ", requested " << command.ConfigVersion;
            break;
    }

    return result;
}

} // namespace NKikimr::NCdcAggregator
