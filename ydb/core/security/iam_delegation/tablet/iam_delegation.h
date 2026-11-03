#pragma once

#include <ydb/core/base/blobstorage.h>

namespace NKikimr::NIamDelegation {

IActor* CreateIamDelegationTablet(const TActorId& tablet, TTabletStorageInfo* info);

} // namespace NKikimr::NIamDelegation
