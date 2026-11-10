#include "replace_source_usecase.h"

#include <util/string/builder.h>

namespace NKikimr::NCdcAggregator {

TReplaceSourceResult ReplaceStreamSource(TStream& stream, IStreamRepository& repository, TReplaceSourceCommand command) {
    using EStatus = TReplaceSourceResult::EStatus;

    TReplaceSourceResult result;
    if (!stream.IsConfigured()) {
        result.Status = EStatus::NotReady;
        result.Error = "Stream is not configured";
        return result;
    }
    if (command.StreamPathId != stream.GetConfig().StreamPathId) {
        result.Status = EStatus::WrongStream;
        result.Error = TStringBuilder() << "Aggregator serves stream " << stream.GetConfig().StreamPathId;
        return result;
    }
    if (!NormalizeSourceReplacement(command.Replacement, result.Error)) {
        result.Status = EStatus::BadRequest;
        return result;
    }

    const auto& replacement = command.Replacement;
    TStreamChanges changes;
    switch (stream.ReplaceSource(replacement, changes)) {
        case EReplaceOutcome::Replaced:
            repository.Save(stream, changes);
            break;
        case EReplaceOutcome::AlreadyReplaced:
            break;
        case EReplaceOutcome::UnknownSource:
            result.Status = EStatus::UnknownSource;
            result.Error = TStringBuilder() << "Source " << replacement.Parent << " is not in the stream";
            return result;
        case EReplaceOutcome::Conflict:
            result.Status = EStatus::Conflict;
            result.Error = TStringBuilder() << "Source " << replacement.Parent
                << " is already replaced by other children, or a child is a retired source";
            return result;
    }

    if (changes.ReadyBarrier) {
        result.AdvancedReadyBarrier = stream.GetReadyBarrier();
    }
    return result;
}

} // namespace NKikimr::NCdcAggregator
