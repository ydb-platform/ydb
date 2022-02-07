#pragma once

#include "sleep.h"
#include "spawn.h"

#include <library/cpp/messagebus/actor/executor.h>

#include <util/generic/ptr.h>

namespace NRainCheck {
    struct IEnv {
        virtual ::NActor::TExecutor* GetExecutor() = 0;
        virtual ~IEnv() {
        }
    };

    template <typename TSelf>
    struct TEnvTemplate: public IEnv {
        template <typename TTask, typename TParam>
        TIntrusivePtr<typename TTask::TTaskRunner> SpawnTask(TParam param) {
            return ::NRainCheck::SpawnTask<TTask, TSelf>((TSelf*)this, param);
        }
    };

    template <typename TSelf>
    struct TSimpleEnvTemplate: public TEnvTemplate<TSelf> {
        ::NActor::TExecutorPtr Executor;
        TSleepService SleepService;

        TSimpleEnvTemplate(unsigned threadCount = 0)
            : Executor(new ::NActor::TExecutor(threadCount != 0 ? threadCount : 4))
        {
        }

        ::NActor::TExecutor* GetExecutor() override {
            return Executor.Get();
        }
    };

    struct TSimpleEnv: public TSimpleEnvTemplate<TSimpleEnv> {
        TSimpleEnv(unsigned threadCount = 0)
            : TSimpleEnvTemplate<TSimpleEnv>(threadCount)
        {
        }
    };

}
