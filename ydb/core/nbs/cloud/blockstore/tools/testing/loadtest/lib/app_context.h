#pragma once

#include <util/folder/tempdir.h>
#include <util/stream/str.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>

#include <atomic>

namespace NYdb::NBS::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

struct TTestContext
{
    TMutex WaitMutex;
    TCondVar WaitCondVar;
    std::atomic<bool> ShouldStop = false;
    std::atomic<bool> Finished = false;
    TStringStream Result;
};

////////////////////////////////////////////////////////////////////////////////

enum EExitCode
{
    EC_LOAD_TEST_FAILED = 1,
    EC_CONTROL_PLANE_ACTION_FAILED = 2,
    EC_VALIDATION_FAILED = 3,
    EC_COMPARE_DATA_ACTION_FAILED = 4,
    EC_TIMEOUT = 5,
    EC_FAILED_TO_LOAD_TESTS_CONFIGURATION = 6,
    EC_FAILED_TO_DESTROY_ALIASED_VOLUMES = 7,
    EC_WAIT_FRESH_DEVICES_ACTION_FAILED = 8,
};

////////////////////////////////////////////////////////////////////////////////

struct TAppContext
{
    std::atomic<int> ExitCode = 0;
    std::atomic<bool> ShouldStop = false;
    std::atomic<int> FailedTests = 0;

    TTempDir TempDir;
};

}   // namespace NYdb::NBS::NBlockStore::NLoadTest
