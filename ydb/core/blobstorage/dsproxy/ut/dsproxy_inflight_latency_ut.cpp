#include "defs.h"

#include "dsproxy_env_mock_ut.h"
#include "dsproxy_test_state_ut.h"

namespace NKikimr {

    namespace {

        void SetLogPriorities(TTestBasicRuntime& runtime) {
            runtime.SetLogPriority(NKikimrServices::BS_PROXY, NLog::PRI_CRIT);
            runtime.SetLogPriority(NKikimrServices::BS_QUEUE, NLog::PRI_CRIT);
        }

        void SimulateSleep(TTestBasicRuntime& runtime, TDuration duration) {
            runtime.AdvanceCurrentTime(duration);
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }

        void SimulateSeconds(TTestBasicRuntime& runtime, ui32 seconds) {
            for (ui32 i = 0; i < seconds; ++i) {
                SimulateSleep(runtime, TDuration::Seconds(1));
            }
        }

        void AssertInFlightLatencyPublished(TRequestMonItem& requestMonItem, const TString& message) {
            UNIT_ASSERT_VALUES_EQUAL_C(requestMonItem.ResponseTimeCompletedCount->Val(), 0, message);
            UNIT_ASSERT_VALUES_EQUAL_C(requestMonItem.InFlightCount->Val(), 1, message);
            UNIT_ASSERT_C(requestMonItem.InFlightResponseTimeUsSum->Val() >= static_cast<i64>(TDuration::Seconds(1).MicroSeconds()),
                          message << " InFlightResponseTimeUsSum# " << requestMonItem.InFlightResponseTimeUsSum->Val());
            UNIT_ASSERT_VALUES_EQUAL_C(requestMonItem.InFlightResponseTimeUsSum->Val(),
                                       requestMonItem.InFlightResponseTimeUsMax->Val(),
                                       message);
        }

        void CheckGetInFlightLatencyCounters(
            NKikimrBlobStorage::EGetHandleClass getHandleClass,
            TStoragePoolCounters::EHandleClass storagePoolHandleClass)
        {
            NKikimr::TBlobStorageGroupType erasure = TErasureType::Erasure4Plus2Block;
            TTestBasicRuntime runtime(1, false);
            SetLogPriorities(runtime);
            SetupRuntime(runtime);
            TDSProxyEnv env;
            env.Configure(runtime, erasure, 1, 0, TBlobStorageGroupInfo::EEM_ENC_V1, true);
            TTestState testState(runtime, erasure, env.Info);

            TLogoBlobID blobId = TLogoBlobID(72075186224047637, 1, 863, 1, 254, 24576);
            TString buffer = TString::Uninitialized(blobId.BlobSize());
            for (char& ch : buffer) {
                ch = 'a';
            }
            testState.PutBlobsToGroupMock(TVector<TBlobTestSet::TBlob>{
                TBlobTestSet::TBlob(blobId, buffer),
            });

            const ui32 requestBytes = blobId.BlobSize();
            TRequestMonItem& requestMonItem = env.ProxyStoragePoolCounters->GetItem(storagePoolHandleClass, requestBytes);

            runtime.Send(new IEventHandle(
                env.RealProxyActorId,
                testState.EdgeActor,
                new TEvBlobStorage::TEvGet(
                    blobId,
                    0,
                    blobId.BlobSize(),
                    TInstant::Max(),
                    getHandleClass)));
            runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));

            SimulateSeconds(runtime, 2);
            const TString message = TStringBuilder()
                                    << "getHandleClass# " << NKikimrBlobStorage::EGetHandleClass_Name(getHandleClass);
            AssertInFlightLatencyPublished(requestMonItem, message);
        }

        void CheckPutInFlightLatencyCounters(
            NKikimrBlobStorage::EPutHandleClass putHandleClass,
            TStoragePoolCounters::EHandleClass storagePoolHandleClass)
        {
            NKikimr::TBlobStorageGroupType erasure = TErasureType::Erasure4Plus2Block;
            TTestBasicRuntime runtime(1, false);
            SetLogPriorities(runtime);
            SetupRuntime(runtime);
            TDSProxyEnv env;
            env.Configure(runtime, erasure, 1, 0, TBlobStorageGroupInfo::EEM_ENC_V1, true);
            TTestState testState(runtime, erasure, env.Info);

            TLogoBlobID blobId = TLogoBlobID(72075186224047637, 1, 863, 1, 512 * 1024, 24576);
            TString buffer = TString::Uninitialized(blobId.BlobSize());
            for (char& ch : buffer) {
                ch = 'a';
            }

            const ui32 requestBytes = blobId.BlobSize();
            TRequestMonItem& requestMonItem = env.ProxyStoragePoolCounters->GetItem(storagePoolHandleClass, requestBytes);

            runtime.Send(new IEventHandle(
                env.RealProxyActorId,
                testState.EdgeActor,
                new TEvBlobStorage::TEvPut(
                    blobId,
                    buffer,
                    TInstant::Max(),
                    putHandleClass,
                    TEvBlobStorage::TEvPut::TacticDefault)));
            runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));

            SimulateSeconds(runtime, 2);
            const TString message = TStringBuilder()
                                    << "putHandleClass# " << NKikimrBlobStorage::EPutHandleClass_Name(putHandleClass);
            AssertInFlightLatencyPublished(requestMonItem, message);
        }

    } // namespace

    Y_UNIT_TEST_SUITE(DSProxyInFlightLatencyCounters) {
        Y_UNIT_TEST(InFlightLatencyCountersCoverStoragePoolHandleClasses) {
            CheckGetInFlightLatencyCounters(NKikimrBlobStorage::FastRead, TStoragePoolCounters::EHandleClass::HcGetFast);
            CheckGetInFlightLatencyCounters(NKikimrBlobStorage::AsyncRead, TStoragePoolCounters::EHandleClass::HcGetAsync);
            CheckGetInFlightLatencyCounters(NKikimrBlobStorage::Discover, TStoragePoolCounters::EHandleClass::HcGetDiscover);
            CheckGetInFlightLatencyCounters(NKikimrBlobStorage::LowRead, TStoragePoolCounters::EHandleClass::HcGetLow);

            CheckPutInFlightLatencyCounters(NKikimrBlobStorage::TabletLog, TStoragePoolCounters::EHandleClass::HcPutTabletLog);
            CheckPutInFlightLatencyCounters(NKikimrBlobStorage::UserData, TStoragePoolCounters::EHandleClass::HcPutUserData);
            CheckPutInFlightLatencyCounters(NKikimrBlobStorage::AsyncBlob, TStoragePoolCounters::EHandleClass::HcPutAsync);
        }
    } // Y_UNIT_TEST_SUITE(DSProxyInFlightLatencyCounters)

} // namespace NKikimr
