#pragma once

#include "task.h"

namespace NKikimr::NOlap::NResourceBroker::NSubscribe {

template <typename T>
class TResourceContainer: private TMoveOnly {
private:
    std::optional<T> Value;
    std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard> ResourcesGuard;

    TResourceContainer(T&& value)
        : Value(std::move(value)) {
    }

public:
    const T& GetValue() const {
        AFL_VERIFY(Value);
        return *Value;
    }

    T ExtractValue() {
        AFL_VERIFY(Value);
        T value = std::move(*Value);
        Value.reset();
        return value;
    }

    std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard> ExtractResourcesGuard() {
        AFL_VERIFY(ResourcesGuard);
        return std::move(ResourcesGuard);
    }

    TResourceContainer(TResourceContainer<T>&& other)
        : Value(std::move(other.Value))
        , ResourcesGuard(std::move(other.ResourcesGuard)) {
    }
    TResourceContainer& operator=(TResourceContainer<T>&& other) {
        std::swap(Value, other.Value);
        std::swap(ResourcesGuard, other.ResourcesGuard);
        return *this;
    }

    TResourceContainer(T&& value, std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>&& guard)
        : Value(std::move(value))
        , ResourcesGuard(std::move(guard)) {
        AFL_VERIFY(ResourcesGuard);
    }

    ~TResourceContainer() {
        if (!Value) {
            AFL_VERIFY(!ResourcesGuard);
        }
    }

    static TResourceContainer BuildForTest(T&& value) {
        return TResourceContainer(std::move(value));
    }
};
}   // namespace NKikimr::NOlap::NResourceBroker::NSubscribe
