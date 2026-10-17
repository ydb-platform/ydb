#include "tablet_pipecache.h"

namespace NKikimr {

TActorId MakePipePerNodeCacheID(EPipePerNodeCache kind) {
    char x[12] = "PipeCache";
    switch (kind) {
        case EPipePerNodeCache::Leader:
            x[9] = 'A';
            break;
        case EPipePerNodeCache::Follower:
            x[9] = 'F';
            break;
        case EPipePerNodeCache::Persistent:
            x[9] = 'P';
            break;
    }
    return TActorId(0, TStringBuf(x, 12));
}

TActorId MakePipePerNodeCacheID(bool allowFollower) {
    return MakePipePerNodeCacheID(allowFollower ? EPipePerNodeCache::Follower : EPipePerNodeCache::Leader);
}

} // namespace NKikimr
