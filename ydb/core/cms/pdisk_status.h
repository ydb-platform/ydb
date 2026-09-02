#pragma once

<<<<<<< HEAD
=======
#include <ydb/core/protos/blobstorage_base3.pb.h>
>>>>>>> 36e85567e32 (Fix maintenance with EvictVDisks when there are faulty disks in storage group (#19450))
#include <ydb/core/protos/blobstorage_config.pb.h>

namespace NKikimr::NCms {

using EPDiskStatus = NKikimrBlobStorage::EDriveStatus;
using EMaintenanceStatus = NKikimrBlobStorage::TMaintenanceStatus;

} // namespace NKikimr::NCms
