#include "dummy_lock.h"


#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/svnversion/svnversion.h>

#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/actors/events/events.h>

namespace NYql {

using namespace NActors;

class TDummyLock: public TRichActor<TDummyLock> {
public:
    static constexpr char ActorName[] = "DUMMY_LOCK";

    TDummyLock(
        const TString& lockName,
        const TString& lockAttributes)
        : TRichActor<TDummyLock>(&TDummyLock::Handler)
        , LockName(lockName)
        , Attributes(lockAttributes)
        , Revision(GetProgramCommitId())
    {
        Y_UNUSED(LockName);
        Y_UNUSED(Attributes);
        Y_UNUSED(Revision);
    }

private:
    STRICT_STFUNC(Handler, {
        cFunc(TEvents::TEvPoison::EventType, PassAway);
    });

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
        Y_UNUSED(self);
        return new IEventHandle(parentId, parentId, new TEvBecomeLeader(1, "TransactionId", Attributes), 0);
    }

    TString LockName;
    TString Attributes;
    TString Revision;
};

NActors::IActor* CreateDummyLock(
    const TString& lockName,
    const TString& lockAttributesYson)
{
    return new TDummyLock(lockName, lockAttributesYson);
}

} // namespace NYql

