#include "test_utils.h"

#include <ydb/core/base/tablet_resolver.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NTestUtils {

void WaitFor(TDuration timeout, const TString& description, std::function<bool(TString&)> predicate) {
    const TInstant start = TInstant::Now();
    TString errorString;
    while (TInstant::Now() - start <= timeout) {
        if (predicate(errorString)) {
            return;
        }

        Cerr << "Wait " << description << " " << TInstant::Now() - start << ": " << errorString << "\n";
        Sleep(TDuration::Seconds(1));
    }

    UNIT_FAIL("Waiting " << description << " timeout. Spent time " << TInstant::Now() - start << " exceeds limit " << timeout << ", last error: " << errorString);
}

void WaitFor(TDuration timeout, const TString& description, std::function<bool()> predicate) {
    WaitFor(timeout, description, [predicate](TString&) {
        return predicate();
    });
}

void RestartTablet(const NActors::TActorSystem& runtime, ui64 tabletId) {
    runtime.Send(NKikimr::MakeTabletResolverID(), new NKikimr::TEvTabletResolver::TEvForward(
        tabletId,
        new NActors::IEventHandle(NActors::TActorId(), NActors::TActorId(), new NActors::TEvents::TEvPoisonPill()),
        {},
        NKikimr::TEvTabletResolver::TEvForward::EActor::Tablet
    ));
    runtime.Send(NKikimr::MakeTabletResolverID(), new NKikimr::TEvTabletResolver::TEvTabletProblem(tabletId, NActors::TActorId()));
}

}  // namespace NTestUtils
