#pragma once

#include <cstddef>

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

} // namespace NActors
