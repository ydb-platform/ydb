#pragma once

#include <library/cpp/messagebus/rain_check/core/spawn.h>
#include <library/cpp/messagebus/rain_check/core/task.h>

#include <library/cpp/messagebus/ybus.h>

#include <util/system/yassert.h>

namespace NRainCheck {
    class TBusTaskStarter: public NBus::IBusServerHandler {
    private:
        struct ITaskFactory {
            virtual void NewTask(NBus::TOnMessageContext&) = 0;
            virtual ~ITaskFactory() {
            }
        };

        THolder<ITaskFactory> TaskFactory;

        void OnMessage(NBus::TOnMessageContext&) override;

    public:
        TBusTaskStarter(TAutoPtr<ITaskFactory>);
        ~TBusTaskStarter() override;

    public:
        template <typename TTask, typename TEnv>
        static TAutoPtr<TBusTaskStarter> NewStarter(TEnv* env) {
            struct TTaskFactory: public ITaskFactory {
                TEnv* const Env;

                TTaskFactory(TEnv* env)
                    : Env(env)
                {
                }

                void NewTask(NBus::TOnMessageContext& context) override {
                    SpawnTask<TTask, TEnv, NBus::TOnMessageContext&>(Env, context);
                }
            };

            return new TBusTaskStarter(new TTaskFactory(env));
        }
    };
}
