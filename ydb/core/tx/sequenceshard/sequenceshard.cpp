#include "sequenceshard.h"
#include "sequenceshard_impl.h"

namespace NKikimr {
namespace NSequenceShard {

    IActor* CreateSequenceShard(const TActorId& tablet, TTabletStorageInfo* info) {
        return new TSequenceShard(tablet, info);
    }

} // namespace NSequenceShard
} // namespace NKikimr
