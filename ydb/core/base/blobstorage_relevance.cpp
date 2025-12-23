#include "blobstorage_relevance.h"

namespace NKikimr {

TMessageRelevance::TMessageRelevance(const TMessageRelevanceOwner& onwer,
        std::optional<TMessageRelevanceWatcher> external)
    : InternalWatcher(onwer)
    , ExternalWatcher(external)
{}

bool TMessageRelevance::IsRelevant() const {
    return !InternalWatcher.expired() && (!ExternalWatcher || !ExternalWatcher->expired());
}

} // namespace NKikimr
