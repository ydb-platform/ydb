#include "test_runtime.h"

#include <deque>
#include <functional>

namespace NActors {

    /**
     * Easy blocking for events under the test actor runtime
     *
     * Matching events are blocked just before they are processed and stashed
     * into a deque.
     */
    template<class TEvType>
    class TBlockEvents : public std::deque<typename TEvType::TPtr> {
    public:
        TBlockEvents(TTestActorRuntime& runtime, std::function<bool(typename TEvType::TPtr&)> condition = {})
            : Runtime(runtime)
            , Condition(std::move(condition))
            , Holder(Runtime.AddObserver<TEvType>(
                [this](typename TEvType::TPtr& ev) {
                    this->Process(ev);
                }))
        {}

        /**
         * Unblocks up to count events at the front of the deque, allowing them
         * to be handled by the destination actor.
         */
        TBlockEvents& Unblock(size_t count = Max<size_t>()) {
            while (!this->empty() && count > 0) {
                auto& ev = this->front();
                if (!Stopped) {
                    IEventHandle* ptr = ev.Get();
                    UnblockedOnce.insert(ptr);
                }
                ui32 nodeId = ev->GetRecipientRewrite().NodeId();
                ui32 nodeIdx = nodeId - Runtime.GetFirstNodeId();
                Cerr << "TBlockEvents::Unblock " << typeid(TEvType).name() << " from " << Runtime.FindActorName(ev->Sender) << " to " << Runtime.FindActorName(ev->GetRecipientRewrite()) << Endl;
                Runtime.Send(ev.Release(), nodeIdx, /* viaActorSystem */ true);
                this->pop_front();
                --count;
            }
            return *this;
        }

        /**
         * Stops blocking any new events. Events currently in the deque are
         * not unblocked, but may be unblocked at a later time if needed.
         */
        TBlockEvents& Stop() {
            UnblockedOnce.clear();
            Holder.Remove();
            Stopped = true;
            return *this;
        }

    private:
        void Process(typename TEvType::TPtr& ev) {
            IEventHandle* ptr = ev.Get();
            auto it = UnblockedOnce.find(ptr);
            if (it != UnblockedOnce.end()) {
                UnblockedOnce.erase(it);
                return;
            }

            if (Condition && !Condition(ev)) {
                return;
            }

            Cerr << "TBlockEvents::Block " << typeid(TEvType).name() << " from " << Runtime.FindActorName(ev->Sender) << " to " << Runtime.FindActorName(ev->GetRecipientRewrite()) << Endl;
            this->emplace_back(std::move(ev));
        }

    private:
        TTestActorRuntime& Runtime;
        std::function<bool(typename TEvType::TPtr&)> Condition;
        TTestActorRuntime::TEventObserverHolder Holder;
        THashSet<IEventHandle*> UnblockedOnce;
        bool Stopped = false;
    };


} // namespace NActors
