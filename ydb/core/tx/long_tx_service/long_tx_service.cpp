#include "long_tx_service.h"
#include "long_tx_service_impl.h"

namespace NKikimr {
namespace NLongTxService {

    TLongTxServiceCounters::TLongTxServiceCounters(const TGroupPtr& group)
        : AcquireReadSnapshotInRequests(group->GetCounter("AcquireReadSnapshotInRequests", true))
        , AcquireReadSnapshotOutRequests(group->GetCounter("AcquireReadSnapshotOutRequests", true))
        , AcquireReadSnapshotInInFlight(group->GetCounter("AcquireReadSnapshotInInFlight"))
        , AcquireReadSnapshotOutInFlight(group->GetCounter("AcquireReadSnapshotOutInFlight"))
        , TimeSinceLastRemoteSnapshotsUpdateMs(group->GetCounter("TimeSinceLastRemoteSnapshotsUpdateMs"))
        , RemoteSnapshotsInRegistry(group->GetCounter("RemoteSnapshotsInRegistry"))
        , SnapshotsCollectionTimeMs(group->GetCounter("SnapshotsCollectionTimeMs"))
        , SnapshotsPropagationTimeMs(group->GetCounter("SnapshotsPropagationTimeMs"))
    {}

    IActor* CreateLongTxService(const TLongTxServiceSettings& settings) {
        return new TLongTxServiceActor(settings);
    }

} // namespace NLongTxService
} // namespace NKikimr
