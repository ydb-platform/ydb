#pragma once
#include "event.h"
#include "actor.h"

namespace NActors {

template <class TEvent>
class TEventContext {
private:
    TEvent* Event;
    std::unique_ptr<IEventHandle> Handle;
public:
    const TEvent* operator->() const {
        return Event;
    }
    const IEventHandle& GetHandle() const {
        return *Handle;
    }
    TEventContext(std::unique_ptr<IEventHandle> handle)
        : Handle(std::move(handle))
    {
        Y_DEBUG_ABORT_UNLESS(dynamic_cast<TEvent*>(Handle->GetBase()));
        Event = static_cast<TEvent*>(Handle->GetBase());
        Y_ABORT_UNLESS(Event);
    }
};

template <class TEvent, class TExpectedActor>
class IEventForActor: public IEventBase {
protected:
    virtual bool DoExecute(IActor* actor, std::unique_ptr<IEventHandle> eventPtr) override {
        Y_DEBUG_ABORT_UNLESS(dynamic_cast<TExpectedActor*>(actor));
        auto* actorCorrect = static_cast<TExpectedActor*>(actor);
        TEventContext<TEvent> context(std::move(eventPtr));
        actorCorrect->ProcessEvent(context);
        return true;
    }
public:
};

template <class TBaseEvent, class TEvent, class TExpectedObject>
class IEventForAnything: public TBaseEvent {
protected:
    virtual bool DoExecute(IActor* actor, std::unique_ptr<IEventHandle> eventPtr) override {
        auto* objImpl = dynamic_cast<TExpectedObject*>(actor);
        if (!objImpl) {
            return false;
        }
        TEventContext<TEvent> context(std::move(eventPtr));
        objImpl->ProcessEvent(context);
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
        Y_ABORT("Serialization of local event %s->%s", typeid(TEvent).name(), typeid(TActor).name());
    }

    virtual bool IsSerializable() const override {
        return false;
    }

    static IEventBase* Load(TEventSerializedData*) {
        Y_ABORT("Loading of local event %s->%s", typeid(TEvent).name(), typeid(TActor).name());
    }
};

template <class TEvent, class TActor>
i64 TEventLocalForActor<TEvent, TActor>::LocalClassId = Singleton<TAtomicCounter>()->Inc();

}
