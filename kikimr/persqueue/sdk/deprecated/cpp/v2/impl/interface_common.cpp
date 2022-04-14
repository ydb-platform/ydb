#include "interface_common.h"
#include "persqueue_p.h"

namespace NPersQueue {

const TString& GetCancelReason()
{
    static const TString reason = "Destroyed";
    return reason;
}

TSyncDestroyed::TSyncDestroyed(std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib)
    : DestroyEventRef(std::move(destroyEventRef))
    , PQLib(std::move(pqLib))
{
}

TSyncDestroyed::~TSyncDestroyed() {
    DestroyedPromise.SetValue();
}

void TSyncDestroyed::DestroyPQLibRef() {
    auto guard = Guard(DestroyLock);
    if (DestroyEventRef) {
        IsCanceling = true;
        PQLib = nullptr;
        DestroyEventRef = nullptr;
    }
}

} // namespace NPersQueue
