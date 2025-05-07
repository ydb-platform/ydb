#include "test_runtime.h"

#include <functional>

namespace NActors {

    /**
     * Easy wait for a first event under the test actor runtime
     *
     */
    template<class TEvType>
    class TWaitForFirstEvent {
    public:
        TWaitForFirstEvent(TTestActorRuntime& runtime, std::function<bool(const typename TEvType::TPtr&)> condition = {})
            : Runtime(runtime)
            , Condition(std::move(condition))
            , Holder(Runtime.AddObserver<TEvType>(
                [this](typename TEvType::TPtr& ev) {
                    if (EventSeen)
                        return;
                    if (Condition && !Condition(ev))
                        return;
                    EventSeen = true;
                }))
        {}

        /**
         * Wait for a first event
         */
        void Wait(TDuration simTimeout = TDuration::Max()) {
            Runtime.WaitFor(TypeName<TEvType>(), [&]{ return EventSeen; }, simTimeout);
        }

        /**
         * Stops waiting and remove event observer
         */
        void Stop() {
            Holder.Remove();
        }

    private:
        TTestActorRuntime& Runtime;
        std::function<bool(const typename TEvType::TPtr&)> Condition;
        TTestActorRuntime::TEventObserverHolder Holder;
        bool EventSeen = false;
    };

} // namespace NActors
