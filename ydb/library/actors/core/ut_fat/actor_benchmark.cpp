#include "actor_benchmark_helper.h"

#include <library/cpp/testing/unittest/registar.h>


using namespace NActors;
using namespace NActors::NTests;


struct THeavyActorBenchmarkSettings : TActorBenchmarkSettings {
    static constexpr ui32 TotalEventsAmountPerThread = 1'000'000;

    static constexpr auto MailboxTypes = {
        TMailboxType::HTSwap,
    };
};



Y_UNIT_TEST_SUITE(HeavyActorBenchmark) {

    using TActorBenchmark = ::NActors::NTests::TActorBenchmark<THeavyActorBenchmarkSettings>;
    using TSettings = TActorBenchmark::TSettings;


    Y_UNIT_TEST(SendActivateReceiveCSV) {
        const char* loadTypeEnv = std::getenv("LOAD_TYPE");
        TString loadTypeStr = loadTypeEnv ? loadTypeEnv : "RandomRead";
        TActorBenchmark::ELoadType loadType = TActorBenchmark::ELoadType::RandomRead;
        if (loadTypeStr == "RandomRead") {
            loadType = TActorBenchmark::ELoadType::RandomRead;
        } else if (loadTypeStr == "RandomWrite") {
            loadType = TActorBenchmark::ELoadType::RandomWrite;
        } else if (loadTypeStr == "SequentialRead") {
            loadType = TActorBenchmark::ELoadType::SequentialRead;
        } else if (loadTypeStr == "SequentialWrite") {
            loadType = TActorBenchmark::ELoadType::SequentialWrite;
        } else {
            Y_ABORT("Invalid load type: %s", loadTypeStr.c_str());
        }

        const char* busyCoresEnv = std::getenv("LOAD_CORES");
        std::vector<ui32> cores;
        std::vector<TString> rangeCores = StringSplitter(busyCoresEnv).Split(',');
        for (const TString& rangeCore : rangeCores) {
            if (rangeCore.find('-') != TString::npos) {
                std::vector<TString> range = StringSplitter(rangeCore).Split('-');
                for (ui32 i = std::stoi(range[0]), end = std::stoi(range[1]); i <= end; ++i) {
                    cores.push_back(i);
                }
            } else {
                cores.push_back(std::atoi(rangeCore.c_str()));
            }
        }

        std::vector<TActorBenchmark::TSendActivateReceiveCSVParams> paramsList;
        const char* threadsEnv = std::getenv("THREADS_COUNT");
        ui32 maxThreads = threadsEnv ? std::stoi(threadsEnv) : 28;
        const char *durationEnv = std::getenv("DURATION");
        ui32 duration = durationEnv ? std::stoi(durationEnv) : 1;
        TCpuMask cpuMask;
        ui32 idx = 0;
        for (ui32 core : cores) {
            cpuMask.Set(core);
        }
        for (ui32 threads = 1; threads <= maxThreads; threads++) {
            if (idx < cores.size()) {
                cpuMask.Reset(cores[idx]);
                idx++;
            }
            paramsList.push_back({
                .ThreadCount=threads,
                .ActorPairCount=32 * threads,
                .InFlight=1,
                .SubtestDuration=TDuration::Seconds(duration),
                .ExtraLoad=cpuMask,
                .LoadType=loadType});
        }
        TActorBenchmark::RunSendActivateReceiveCSV(paramsList);
    }

    Y_UNIT_TEST(StarSendActivateReceiveCSV) {
        std::vector<ui32> threadsList;
        const char* threadsEnv = std::getenv("THREADS_COUNT");
        ui32 maxThreads = threadsEnv ? std::stoi(threadsEnv) : 28;
        for (ui32 threads = 1; threads <= maxThreads; threads++) {
            threadsList.push_back(threads);
        }
        std::vector<ui32> actorPairsList = {512};
        std::vector<ui32> starsList = {10};
        TActorBenchmark::RunStarSendActivateReceiveCSV(threadsList, actorPairsList, starsList);
    }

    Y_UNIT_TEST(BusyRun) {
        const char* busyCoresEnv = std::getenv("LOAD_CORES");
        std::vector<ui32> cores;
        std::vector<TString> rangeCores = StringSplitter(busyCoresEnv).Split(',');
        for (const TString& rangeCore : rangeCores) {
            if (rangeCore.find('-') != TString::npos) {
                std::vector<TString> range = StringSplitter(rangeCore).Split('-');
                for (ui32 i = std::stoi(range[0]), end = std::stoi(range[1]); i <= end; ++i) {
                    cores.push_back(i);
                }
            } else {
                cores.push_back(std::atoi(rangeCore.c_str()));
            }
        }
        const char *durationEnv = std::getenv("DURATION");
        ui64 duration = durationEnv ? std::stoi(durationEnv) : 1;
        TCpuMask cpuMask;
        for (ui32 core : cores) {
            cpuMask.Set(core);
        }
        std::vector<std::unique_ptr<TActorBenchmark::TBusyThread>> busyThreads;

        for (ui32 i = 0; i < cores.size(); ++i) {
            busyThreads.emplace_back(new TActorBenchmark::TBusyThread());
            busyThreads.back()->LoadType = TActorBenchmark::ELoadType::RandomWrite;
            busyThreads.back()->Buffer.resize(1024*1024);
            TCpuMask coreMask(cores[i]);
            busyThreads.back()->Affinity = std::make_unique<TAffinity>(coreMask);
            busyThreads.back()->Start();
        }
        NanoSleep(duration * 1000000000);
        for (ui32 i = 0; i < cores.size(); ++i) {
            busyThreads[i]->StopFlag.store(true);
        }
        for (ui32 i = 0; i < cores.size(); ++i) {
            busyThreads[i]->Join();
        }
    }
}
