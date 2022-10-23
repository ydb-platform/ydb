#include "accessor_subscribe.h"
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NMetadataProvider {

bool TDSAccessorNotifier::Handle(TEvRequestResult<TDialogSelect>::TPtr& ev) {
    if (TBase::Handle(ev)) {
        auto snapshot = GetCurrentSnapshot();
        if (!snapshot) {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot construct snapshot";
            return false;
        }
        
        for (auto&& i : Subscribed) {
            Send(i, new TEvRefreshSubscriberData(snapshot));
        }
        return true;
    } else {
        return false;
    }
}

void TDSAccessorNotifier::Handle(TEvSubscribe::TPtr& ev) {
    Subscribed.emplace(ev->Get()->GetSubscriberId());
    if (TBase::IsReady()) {
        auto snapshot = GetCurrentSnapshot();
        if (!snapshot) {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot construct snapshot";
            return;
        }
        Send(ev->Get()->GetSubscriberId(), new TEvRefreshSubscriberData(snapshot));
    }
}

void TDSAccessorNotifier::Handle(TEvUnsubscribe::TPtr& ev) {
    Subscribed.erase(ev->Get()->GetSubscriberId());
}

}
