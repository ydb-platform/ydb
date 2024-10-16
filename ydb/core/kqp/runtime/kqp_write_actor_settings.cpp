#include "kqp_write_actor_settings.h"

#include <library/cpp/threading/hot_swap/hot_swap.h>
#include <util/generic/singleton.h>


namespace NKikimr {
namespace NKqp {

struct TWriteActorDefaultSettings {
    THotSwap<TWriteActorSettings> SettingsPtr;

    TWriteActorDefaultSettings() {
        SettingsPtr.AtomicStore(new TWriteActorSettings());
    }

};

TWriteActorSettings GetWriteActorSettings() {
    return *Singleton<TWriteActorDefaultSettings>()->SettingsPtr.AtomicLoad();
}

void SetWriteActorSettings(TIntrusivePtr<TWriteActorSettings> ptr) {
    Singleton<TWriteActorDefaultSettings>()->SettingsPtr.AtomicStore(ptr);
}

}
}
