#include "blobstorage_relevance.h"

namespace NKikimr {

TMessageRelevance::TMessageRelevance(const TMessageRelevanceOwner& onwer,
        std::optional<TMessageRelevanceWatcher> external)
    : InternalWatcher(onwer)
    , ExternalWatcher(external)
{}

TMessageRelevance::EStatus TMessageRelevance::GetStatus() const {
    if (InternalWatcher.expired()) {
        return EStatus::CancelledInternally;
    }
    if (ExternalWatcher && ExternalWatcher->expired()) {
        return EStatus::CancelledExternally;
    }
    return EStatus::Relevant;
}

} // namespace NKikimr
