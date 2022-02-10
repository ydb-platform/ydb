#pragma once

#include <library/cpp/messagebus/rain_check/core/rain_check.h>

#include <array>

namespace NRainCheck {
    struct TNopSimpleTask: public ISimpleTask {
        TNopSimpleTask(IEnv*, const void*) {
        }

        TContinueFunc Start() override {
            return nullptr;
        }
    };

    struct TNopCoroTask: public ICoroTask {
        TNopCoroTask(IEnv*, const void*) {
        }

        void Run() override {
        }
    };

    struct TSpawnNopTasksCoroTask: public ICoroTask {
        IEnv* const Env;
        unsigned const Count;

        TSpawnNopTasksCoroTask(IEnv* env, unsigned count)
            : Env(env)
            , Count(count)
        {
        }

        std::array<TSubtaskCompletion, 2> Completion;

        void Run() override;
    };

    struct TSpawnNopTasksSimpleTask: public ISimpleTask {
        IEnv* const Env;
        unsigned const Count;

        TSpawnNopTasksSimpleTask(IEnv* env, unsigned count)
            : Env(env)
            , Count(count)
        {
        }

        std::array<TSubtaskCompletion, 2> Completion;

        TContinueFunc Start() override;

        TContinueFunc Join();
    };

}
