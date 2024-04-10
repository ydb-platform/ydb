#include "dummy.h"
#include <ydb/core/base/blobstorage_grouptype.h>
#include <ydb/core/protos/alloc.pb.h>

TAutoPtr<NKikimrConfig::TActorSystemConfig> DummyActorSystemConfig() {
    TAutoPtr<NKikimrConfig::TActorSystemConfig> ret(new NKikimrConfig::TActorSystemConfig());

    NKikimrConfig::TActorSystemConfig::TScheduler *sched = ret->MutableScheduler();
    sched->SetResolution(512);
    sched->SetSpinThreshold(0);
    sched->SetProgressThreshold(10000);

    NKikimrConfig::TActorSystemConfig::TExecutor *exec1 = ret->AddExecutor();
    NKikimrConfig::TActorSystemConfig::TExecutor *exec2 = ret->AddExecutor();
    NKikimrConfig::TActorSystemConfig::TExecutor *exec3 = ret->AddExecutor();

    exec1->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
    exec1->SetThreads(1);
    exec1->SetSpinThreshold(50);

    exec2->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
    exec2->SetThreads(2);
    exec2->SetSpinThreshold(50);

    exec3->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::IO);
    exec3->SetThreads(10);

    ret->SetSysExecutor(0);
    ret->SetUserExecutor(1);
    ret->SetBatchExecutor(1);
    ret->SetIoExecutor(2);

    return ret;
}

TAutoPtr<NKikimrConfig::TChannelProfileConfig> DummyChannelProfileConfig() {
    TAutoPtr<NKikimrConfig::TChannelProfileConfig> ret(new NKikimrConfig::TChannelProfileConfig());

    auto *profile = ret->AddProfile();
    profile->SetProfileId(0);

    auto *channel0 = profile->AddChannel();
    auto *channel1 = profile->AddChannel();
    auto *channel2 = profile->AddChannel();

    channel0->SetErasureSpecies(NKikimr::TBlobStorageGroupType::ErasureName[NKikimr::TBlobStorageGroupType::ErasureMirror3]);
    channel0->SetPDiskCategory(0);

    channel1->SetErasureSpecies(NKikimr::TBlobStorageGroupType::ErasureName[NKikimr::TBlobStorageGroupType::ErasureMirror3]);
    channel1->SetPDiskCategory(0);

    channel2->SetErasureSpecies(NKikimr::TBlobStorageGroupType::ErasureName[NKikimr::TBlobStorageGroupType::ErasureMirror3]);
    channel2->SetPDiskCategory(0);

    return ret;
}

TAutoPtr<NKikimrConfig::TAllocatorConfig> DummyAllocatorConfig() {
    TAutoPtr<NKikimrConfig::TAllocatorConfig> ret(new NKikimrConfig::TAllocatorConfig());

    ret->MutableParam()->insert({"EnableDefrag", "false"});

    return ret;
}
