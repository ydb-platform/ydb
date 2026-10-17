#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <dlfcn.h>

#include <atomic>
#include <thread>
#include <vector>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TRunPerCpuSensorFallbackUpdates = bool (*)();

TEST(TPerCpuSensorDynamicTlsTest, PreExistingThreadsUpdateFallbackSensors)
{
    constexpr int ThreadCount = 8;

    std::atomic<int> readyCount = 0;
    std::atomic<bool> start = false;
    TRunPerCpuSensorFallbackUpdates runUpdates = nullptr;
    std::vector<int> updatesSucceeded(ThreadCount);
    std::vector<std::thread> workers;
    for (int index = 0; index < ThreadCount; ++index) {
        workers.emplace_back([&, index] {
            readyCount.fetch_add(1, std::memory_order::release);
            while (!start.load(std::memory_order::acquire)) {
                std::this_thread::yield();
            }
            if (runUpdates) {
                updatesSucceeded[index] = runUpdates();
            }
        });
    }

    while (readyCount.load(std::memory_order::acquire) != ThreadCount) {
        std::this_thread::yield();
    }

    auto libraryPath = BinaryPath(
        "yt/yt/library/profiling/unittests/dynamic_tls/shared/libprofiling_dynamic_tls.so");
    auto* library = dlopen(libraryPath.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (library) {
        runUpdates = reinterpret_cast<TRunPerCpuSensorFallbackUpdates>(
            dlsym(library, "RunPerCpuSensorFallbackUpdatesFromDynamicTlsLibrary"));
    }

    start.store(true, std::memory_order::release);
    for (auto& worker : workers) {
        worker.join();
    }

    ASSERT_NE(library, nullptr) << dlerror();
    ASSERT_NE(runUpdates, nullptr) << dlerror();
    for (int succeeded : updatesSucceeded) {
        EXPECT_TRUE(succeeded);
    }
    EXPECT_EQ(dlclose(library), 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
