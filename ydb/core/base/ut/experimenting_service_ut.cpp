#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr {

#define TEST_REPEATS 1000000
#define TEST_THREADS_CNT 4
#define IS_VERBOSE 1

#if IS_VERBOSE
#   define VERBOSE_COUT(a)  \
        Cout << a;          \
        Cout << Endl
#endif

Y_UNIT_TEST_SUITE(ExperimentingServiceTests) {
    Y_UNIT_TEST(TestExpServiceRegLocal) {
        TIntrusivePtr<TExperimentingService> expService(new TExperimentingService);
        TControlWrapper control1(1, 1, 1);
        TControlWrapper control2(2, 2, 2);
        EXP_SERVICE_REG_LOCAL(*expService, DataShardControlsDisableByKeyFilter, control1);
        UNIT_ASSERT(expService->DataShardControlsDisableByKeyFilter.IsDefined());
    }

    Y_UNIT_TEST(TestExpServiceRegShared) {
        TIntrusivePtr<TExperimentingService> expService(new TExperimentingService);
        TControlWrapper control1(1, 1, 1);
        TControlWrapper control1_origin(control1);
        TControlWrapper control2(2, 2, 2);
        EXP_SERVICE_REG_SHARED(*expService, DataShardControlsDisableByKeyFilter, control1);
        UNIT_ASSERT(control1.IsTheSame(control1_origin));
        EXP_SERVICE_REG_SHARED(*expService, DataShardControlsDisableByKeyFilter, control2);
        UNIT_ASSERT(control2.IsTheSame(control1_origin));
    }

    Y_UNIT_TEST(TestParallelRegisterSharedControl) {
        void* (*parallelJob)(void*) = [](void *es) -> void *{
            TExperimentingService *expService = reinterpret_cast<TExperimentingService *>(es);
            TControlWrapper control1(2, 2, 2);
            TControlWrapper control1_origin(2, 2, 2);
            EXP_SERVICE_REG_SHARED(*expService, DataShardControlsDisableByKeyFilter, control1);
            UNIT_ASSERT(!control1.IsTheSame(control1_origin));
            UNIT_ASSERT(expService->DataShardControlsDisableByKeyFilter.IsDefined());
            return nullptr;
        };
        
        TIntrusivePtr<TExperimentingService> expService(new TExperimentingService);
        TControlWrapper initialControl(1, 1, 1);
        EXP_SERVICE_REG_SHARED(*expService, DataShardControlsDisableByKeyFilter, initialControl);

        TVector<THolder<TThread>> threads;
        threads.reserve(TEST_THREADS_CNT);
        for (ui64 i = 0; i < TEST_THREADS_CNT; ++i) {
            threads.emplace_back(new TThread(parallelJob, (void *)expService.Get()));
        }

        for (ui64 i = 0; i < TEST_THREADS_CNT; ++i) {
            threads[i]->Start();
        }
        for (ui64 i = 0; i < TEST_THREADS_CNT; ++i) {
            threads[i]->Join();
        }
    }
}

} // namespace NKikimr

