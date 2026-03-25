#pragma once

#include <cstddef>
#include <memory>
#include <type_traits>
#include <vector>

namespace NActors {
    class TActorSystem;

    class ISubSystem {
    public:
        virtual ~ISubSystem() = default;

        virtual void OnBeforeStart(TActorSystem&) {}
        virtual void OnAfterStart(TActorSystem&) {}
        virtual void OnBeforeStop(TActorSystem&) {}
        virtual void OnAfterStop(TActorSystem&) {}
    };

    class TSubSystemRegistry {
    private:
        static size_t NextIndex() noexcept;

    public:
        template<class T>
        struct TItem {
            static size_t Index() noexcept {
                static const size_t value = NextIndex();
                return value;
            }
        };
    };

    using TSubSystems = std::vector<std::unique_ptr<ISubSystem>>;

    template<class T>
    void RegisterSubSystem(TSubSystems& subsystems, std::unique_ptr<T>&& subsystem) {
        static_assert(std::is_base_of_v<ISubSystem, T>, "T must implement ISubSystem");
        const size_t index = TSubSystemRegistry::TItem<T>::Index();
        if (subsystems.size() <= index) {
            subsystems.resize(index + 1);
        }
        subsystems[index] = std::move(subsystem);
    }

    template<class T>
    T* GetSubSystem(TSubSystems& subsystems) {
        static_assert(std::is_base_of_v<ISubSystem, T>, "T must implement ISubSystem");
        const size_t index = TSubSystemRegistry::TItem<T>::Index();
        if (index >= subsystems.size()) {
            return nullptr;
        }
        return static_cast<T*>(subsystems[index].get());
    }

    template<class T>
    const T* GetSubSystem(const TSubSystems& subsystems) {
        static_assert(std::is_base_of_v<ISubSystem, T>, "T must implement ISubSystem");
        const size_t index = TSubSystemRegistry::TItem<T>::Index();
        if (index >= subsystems.size()) {
            return nullptr;
        }
        return static_cast<const T*>(subsystems[index].get());
    }

} // namespace NActors
