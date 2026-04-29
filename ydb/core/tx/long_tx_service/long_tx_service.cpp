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
        , RemoteLockSubscriptions(group->GetCounter("RemoteLockSubscriptions"))
        , WaitGraphEdges(group->GetCounter("WaitGraphEdges"))
        , LocalWaitGraphEdges(group->GetCounter("LocalWaitGraphEdges"))
        , WaitGraphEdgesSent(group->GetCounter("WaitGraphEdgesSent", true))
        , WaitGraphEdgesReceived(group->GetCounter("WaitGraphEdgesReceived", true))
        , WaitGraphEdgesBroken(group->GetCounter("WaitGraphEdgesBroken", true))
    {}

    IActor* CreateLongTxService(const TLongTxServiceSettings& settings) {
        return new TLongTxServiceActor(settings);
    }

} // namespace NLongTxService
} // namespace NKikimr
