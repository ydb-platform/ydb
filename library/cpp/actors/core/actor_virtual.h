#pragma once
#include "event.h"
#include "actor.h"

namespace NActors {

class IEventBehavioral: public IEventBase {
protected:
    virtual bool DoExecute(IActor* actor, TAutoPtr<IEventHandle>& eventPtr, const NActors::TActorContext& ctx) = 0;
public:
    bool Execute(IActor* actor, TAutoPtr<IEventHandle>& eventPtr, const NActors::TActorContext& ctx) {
        return DoExecute(actor, eventPtr, ctx);
    }
};

template <class TEvent, class TExpectedActor>
class IEventForActor: public IEventBehavioral {
protected:
    virtual bool DoExecute(IActor* actor, TAutoPtr<IEventHandle>& eventPtr, const NActors::TActorContext& ctx) override {
        Y_ASSERT(dynamic_cast<TExpectedActor*>(actor));
        Y_ASSERT(dynamic_cast<TEvent*>(eventPtr->GetBase()));
        auto* actorCorrect = static_cast<TExpectedActor*>(actor);
        TEvent* evPtrLocal(static_cast<TEvent*>(eventPtr->GetBase()));
        actorCorrect->ProcessEvent(evPtrLocal, eventPtr, ctx);
        return true;
    }
public:
};

template <class TEvent, class TActor>
class TEventLocalForActor: public IEventForActor<TEvent, TActor> {
private:
    using TBase = IEventForActor<TEvent, TActor>;
    static TString GetClassTitle() {
        return TStringBuilder() << typeid(TEvent).name() << "->" << typeid(TActor).name();
    }
    static i64 LocalClassId;
public:
    virtual ui32 Type() const override {
        return LocalClassId;
    }
    virtual TString ToStringHeader() const override {
        return GetClassTitle();
    }

    virtual bool SerializeToArcadiaStream(TChunkSerializer* /*serializer*/) const override {
        Y_FAIL("Serialization of local event %s->%s", typeid(TEvent).name(), typeid(TActor).name());
    }

    virtual bool IsSerializable() const override {
        return false;
    }

    static IEventBase* Load(TEventSerializedData*) {
        Y_FAIL("Loading of local event %s->%s", typeid(TEvent).name(), typeid(TActor).name());
    }
};

template <class TEvent, class TActor>
i64 TEventLocalForActor<TEvent, TActor>::LocalClassId = Singleton<TAtomicCounter>()->Inc();

}
