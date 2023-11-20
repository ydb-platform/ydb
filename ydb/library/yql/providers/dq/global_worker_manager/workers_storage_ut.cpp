#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/providers/dq/global_worker_manager/workers_storage.h>

using namespace NYql;
using namespace NYql::NDq;

Y_UNIT_TEST_SUITE(WorkersBenchmark) {
    Y_UNIT_TEST(Basic) {
        int workers = 1000;
        int i;
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();
        for (i = 0; i < workers; i++) {
            TGUID guid;
            Yql::DqsProto::RegisterNodeRequest request;
            request.SetCapacity(100);
            request.AddKnownNodes(1);
            CreateGuid(&guid);
            storage.CreateOrUpdate(i+100, guid, request);
        }

        UNIT_ASSERT_VALUES_EQUAL(workers*100, storage.FreeSlots());

        TVector<NDqs::TWorkerInfo::TPtr> all;
        TInstant now = TInstant::Now();
        while (1) {
            NYql::NDqProto::TAllocateWorkersRequest request;
            request.SetCount(10);
            IScheduler::TWaitInfo waitInfo(request, NActors::TActorId());
            auto result = storage.TryAllocate(waitInfo);
            if (result.empty()) {
                break;
            }
            all.insert(all.end(), result.begin(), result.end());
        }

        Cerr << (TInstant::Now() - now) << Endl;

        UNIT_ASSERT_VALUES_EQUAL(0, storage.FreeSlots());


        now = TInstant::Now();
        for (auto& node : all) {
            storage.FreeWorker(now, node);
        }
        Cerr << (TInstant::Now() - now) << Endl;

        UNIT_ASSERT_VALUES_EQUAL(workers*100, storage.FreeSlots());
    }

    Y_UNIT_TEST(AcquireAll) {
        int workers = 1;
        int i;
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();
        for (i = 0; i < workers; i++) {
            TGUID guid;
            Yql::DqsProto::RegisterNodeRequest request;
            request.SetCapacity(100);
            request.AddKnownNodes(1);
            CreateGuid(&guid);
            storage.CreateOrUpdate(i+100, guid, request);
        }

        UNIT_ASSERT_VALUES_EQUAL(workers*100, storage.FreeSlots());

        TVector<NDqs::TWorkerInfo::TPtr> all;
        while (1) {
            NYql::NDqProto::TAllocateWorkersRequest request;
            request.SetCount(10);
            IScheduler::TWaitInfo waitInfo(request, NActors::TActorId());
            auto result = storage.TryAllocate(waitInfo);
            if (result.empty()) {
                break;
            }
            all.insert(all.end(), result.begin(), result.end());
        }
        UNIT_ASSERT_VALUES_EQUAL(all.size(), 100);
        UNIT_ASSERT_VALUES_EQUAL(0, storage.FreeSlots());
    }
}
