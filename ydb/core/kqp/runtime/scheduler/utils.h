#pragma once

#include <util/generic/intrlist.h>
#include <util/generic/noncopyable.h>
#include <util/generic/set.h>

namespace NKikimr::NKqp::NScheduler {

class IObservable : TNonCopyable, public TIntrusiveListItem<IObservable> {
public:
    virtual bool Update() = 0;

    void AddDependency(IObservable* dep) {
        Depth = Max<size_t>(Depth, dep->Depth + 1);
        Dependencies.insert(dep);
        dep->Dependents.insert(this);
    }

    bool HasDependents() {
        return !Dependents.empty();
    }

    virtual ~IObservable() {
        for (auto& dep : Dependencies) {
            dep->Dependents.erase(this);
        }
        for (auto& dep : Dependents) {
            dep->Dependencies.erase(this);
        }
    }

    size_t GetDepth() {
        return Depth;
    }

    template<typename T>
    void ForAllDependents(T&& f) {
        for (auto* dep : Dependents) {
            f(dep);
        }
    }

private:
    size_t Depth = 0;

    TSet<IObservable*> Dependencies;
    TSet<IObservable*> Dependents;
};

template<typename T>
class IObservableValue : public IObservable {
protected:
    virtual T DoUpdateValue() = 0;

public:
    bool Update() override {
        auto val = DoUpdateValue();
        if (val != Value) {
            Value = val;
            return true;
        } else {
            return false;
        }
    }

    T GetValue() {
        return Value;
    }

private:
    T Value;
};

template<typename T>
class TMultiThreadView {
public:
    TMultiThreadView(std::atomic<ui64>* usage, T* slot)
        : Usage(usage)
        , Slot(slot)
    {
        Usage->fetch_add(1);
    }
    const T* get() {
        return Slot;
    }

    ~TMultiThreadView() {
        Usage->fetch_sub(1);
    }

private:
    std::atomic<ui64>* Usage;
    T* Slot;
};

template<typename T>
class TMultithreadPublisher {
public:
    void Publish() {
        auto oldVal = CurrentT.load();
        auto newVal = 1 - oldVal;
        CurrentT.store(newVal);
        while (true) {
            if (Usage[oldVal].load() == 0) {
                Slots[oldVal] = Slots[newVal];
                return;
            }
        }
    }

    T* Next() {
        return &Slots[1 - CurrentT.load()];
    }

    TMultiThreadView<T> Current() {
        while (true) {
            auto val = CurrentT.load();
            TMultiThreadView<T> view(&Usage[val], &Slots[val]);
            if (CurrentT.load() == val) {
                return view;
            }
        }
    }

private:
    std::atomic<ui32> CurrentT = 0;
    std::atomic<ui64> Usage[2] = {0, 0};
    T Slots[2];
};

} // namespace NKikimr::NKqp::NScheduler
