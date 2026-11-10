#include "configure_usecase.h"

#include <util/string/builder.h>

namespace NKikimr::NCdcAggregator {

TConfigureResult ConfigureStream(TStream& stream, IStreamRepository& repository, TConfigureCommand command) {
    TConfigureResult result;
    if (stream.IsConfigured()) {
        result.ConfigVersion = stream.GetConfig().Version;
    }

    if (!NormalizeStreamConfig(command.Config, result.Error)) {
        result.Status = TConfigureResult::EStatus::BadRequest;
        return result;
    }

    TStreamChanges changes;
    switch (stream.Configure(command.Config, changes)) {
        case EConfigureOutcome::Configured:
            repository.Save(stream, changes);
            break;
        case EConfigureOutcome::AlreadyConfigured:
            break;
        case EConfigureOutcome::Conflict:
            result.Status = TConfigureResult::EStatus::Conflict;
            result.Error = TStringBuilder() << "Stream is already configured with version " << result.ConfigVersion
                << ", requested version " << command.Config.Version << " differs";
            return result;
    }

    result.ConfigVersion = stream.GetConfig().Version;
    return result;
}

} // namespace NKikimr::NCdcAggregator
