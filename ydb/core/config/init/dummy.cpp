#include "dummy.h"
#include <ydb/core/base/blobstorage_grouptype.h>
#include <ydb/core/protos/alloc.pb.h>


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
