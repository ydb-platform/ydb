#pragma once

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_state.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <ydb/core/protos/blobstorage_config.pb.h>

namespace NKikimr::NBsController {

    NKikimrBlobStorage::TUpdateSettings FromBscConfig(const NKikimrBlobStorage::TBscConfig &config);

    bool CheckGroupReassignmentFailModel(const TMirror3dcGroupConfiguration& config,
        const TBlobStorageGroupInfo::TGroupVDisks& failed);

    TBlobStorageGroupInfo::TGroupVDisks GetExcludedVDisks(const TMirror3dcGroupConfiguration& config,
        const TBlobStorageGroupInfo::TTopology& topology);

}  // NKikimr::NBsController
