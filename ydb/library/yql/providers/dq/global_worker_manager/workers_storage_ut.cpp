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

    Y_UNIT_TEST(ScheduleDownload) {
        int workers = 10;
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();
        for (int i = 0; i < workers; i++) {
            TGUID guid;
            Yql::DqsProto::RegisterNodeRequest request;
            request.SetCapacity(100);
            request.AddKnownNodes(1);
            CreateGuid(&guid);
            storage.CreateOrUpdate(100+i, guid, request);
        }

        {
            auto request = NDqProto::TAllocateWorkersRequest();
            request.SetCount(10);

            auto waitInfo1 = IScheduler::TWaitInfo(request, NActors::TActorId());
            auto result = storage.TryAllocate(waitInfo1);

            UNIT_ASSERT_VALUES_EQUAL(result.size(), 10);
        }

        {
            auto request = NDqProto::TAllocateWorkersRequest();
            auto workerFilter = Yql::DqsProto::TWorkerFilter();
            workerFilter.AddNodeId(102);

            request.SetCount(10);
            for (ui32 i = 0; i < request.GetCount(); i++) {
                *request.AddWorkerFilterPerTask() = workerFilter;
            }
            auto waitInfo2 = IScheduler::TWaitInfo(request, NActors::TActorId());
            auto result = storage.TryAllocate(waitInfo2);
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 10);
        }

        {
            auto request = NDqProto::TAllocateWorkersRequest();
            auto workerFilter = Yql::DqsProto::TWorkerFilter();
            workerFilter.AddNodeId(102);
            Yql::DqsProto::TFile file;
            file.SetObjectId("fileId");
            file.SetLocalPath("/tmp/test");
            *workerFilter.AddFile() = file;
            request.SetCount(10);
            for (ui32 i = 0; i < request.GetCount(); i++) {
                *request.AddWorkerFilterPerTask() = workerFilter;
            }

            auto waitInfo3 = IScheduler::TWaitInfo(request, NActors::TActorId());
            auto result = storage.TryAllocate(waitInfo3);
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 0);

            storage.Visit([](const NDqs::TWorkerInfo::TPtr& workerInfo) {
                UNIT_ASSERT(workerInfo->GetDownloadList().size() == 0 || workerInfo->NodeId == 102);
            });
        }
    }
}
