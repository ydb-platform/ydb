#include "keyvalue_flat_impl.h"

namespace NKikimr {

IActor* CreateKeyValueFlat(const TActorId &tablet, TTabletStorageInfo *info) {
    return new NKeyValue::TKeyValueFlat(tablet, info);
}

} // NKikimr
