#include <library/cpp/messagebus/rain_check/test/helper/misc.h>

#include <library/cpp/messagebus/rain_check/core/rain_check.h>

#include <util/datetime/base.h>

#include <array>

using namespace NRainCheck;

static const unsigned SUBTASKS = 2;

struct TRainCheckPerftestEnv: public TSimpleEnvTemplate<TRainCheckPerftestEnv> {
    unsigned SubtasksPerTask;

    TRainCheckPerftestEnv()
        : TSimpleEnvTemplate<TRainCheckPerftestEnv>(4)
        , SubtasksPerTask(1000)
    {
    }
};

struct TCoroOuter: public ICoroTask {
    TRainCheckPerftestEnv* const Env;

    TCoroOuter(TRainCheckPerftestEnv* env)
        : Env(env)
    {
    }

    void Run() override {
        for (;;) {
            TInstant start = TInstant::Now();

            unsigned count = 0;

            unsigned current = 1000;

            do {
                for (unsigned i = 0; i < current; ++i) {
                    std::array<TSubtaskCompletion, SUBTASKS> completion;

                    for (unsigned j = 0; j < SUBTASKS; ++j) {
                        //SpawnSubtask<TNopSimpleTask>(Env, &completion[j]);
                        //SpawnSubtask<TSpawnNopTasksCoroTask>(Env, &completion[j], SUBTASKS);
                        SpawnSubtask<TSpawnNopTasksSimpleTask>(Env, &completion[j], SUBTASKS);
                    }

                    WaitForSubtasks();
                }

                count += current;
                current *= 2;
            } while (TInstant::Now() - start < TDuration::Seconds(1));

            TDuration d = TInstant::Now() - start;
            unsigned dns = d.NanoSeconds() / count;
            Cerr << dns << "ns per spawn/join\n";
        }
    }
};

struct TSimpleOuter: public ISimpleTask {
    TRainCheckPerftestEnv* const Env;

    TSimpleOuter(TRainCheckPerftestEnv* env, const void*)
        : Env(env)
    {
    }

    TInstant StartInstant;
    unsigned Count;
    unsigned Current;
    unsigned I;

    TContinueFunc Start() override {
        StartInstant = TInstant::Now();
        Count = 0;
        Current = 1000;
        I = 0;

        return &TSimpleOuter::Spawn;
    }

    std::array<TSubtaskCompletion, SUBTASKS> Completion;

    TContinueFunc Spawn() {
        for (unsigned j = 0; j < SUBTASKS; ++j) {
            //SpawnSubtask<TNopSimpleTask>(Env, &Completion[j]);
            //SpawnSubtask<TSpawnNopTasksCoroTask>(Env, &Completion[j], SUBTASKS);
            SpawnSubtask<TSpawnNopTasksSimpleTask>(Env, &Completion[j], SUBTASKS);
        }

        return &TSimpleOuter::Join;
    }

    TContinueFunc Join() {
        I += 1;
        if (I != Current) {
            return &TSimpleOuter::Spawn;
        }

        I = 0;
        Count += Current;
        Current *= 2;

        TDuration d = TInstant::Now() - StartInstant;
        if (d < TDuration::Seconds(1)) {
            return &TSimpleOuter::Spawn;
        }

        unsigned dns = d.NanoSeconds() / Count;
        Cerr << dns << "ns per spawn/join\n";

        return &TSimpleOuter::Start;
    }
};

struct TReproduceCrashTask: public ISimpleTask {
    TRainCheckPerftestEnv* const Env;

    TReproduceCrashTask(TRainCheckPerftestEnv* env)
        : Env(env)
    {
    }

    std::array<TSubtaskCompletion, SUBTASKS> Completion;

    TContinueFunc Start() override {
        for (unsigned j = 0; j < 2; ++j) {
            //SpawnSubtask<TNopSimpleTask>(Env, &Completion[j]);
            SpawnSubtask<TSpawnNopTasksSimpleTask>(Env, &Completion[j], SUBTASKS);
        }

        return &TReproduceCrashTask::Start;
    }
};

int main(int argc, char** argv) {
    Y_UNUSED(argc);
    Y_UNUSED(argv);

    TRainCheckPerftestEnv env;

    env.SpawnTask<TSimpleOuter>("");
    //env.SpawnTask<TCoroOuter>();
    //env.SpawnTask<TReproduceCrashTask>();

    for (;;) {
        Sleep(TDuration::Hours(1));
    }

    return 0;
}
