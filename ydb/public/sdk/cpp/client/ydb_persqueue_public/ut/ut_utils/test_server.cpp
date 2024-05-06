#include "test_server.h"

namespace NPersQueue {

const TVector<NKikimrServices::EServiceKikimr> TTestServer::LOGGED_SERVICES = {
    NKikimrServices::PQ_READ_PROXY,
    NKikimrServices::PQ_WRITE_PROXY,
    NKikimrServices::PQ_PARTITION_CHOOSER,
    NKikimrServices::PERSQUEUE,
    NKikimrServices::PERSQUEUE_READ_BALANCER
};

} // namespace NPersQueue
