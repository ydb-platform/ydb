#include "agent.h"
#include "agent_impl.h"
#include "blocks.h"
#include "blob_mapping_cache.h"

namespace NKikimr::NBlobDepot {

    TBlobDepotAgent::TBlobDepotAgent(ui32 virtualGroupId, TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId proxyId)
        : TRequestSender(*this)
        , VirtualGroupId(virtualGroupId)
        , ProxyId(proxyId)
        , AgentInstanceId(RandomNumber<ui64>())
        , BlocksManagerPtr(new TBlocksManager(*this))
        , BlocksManager(*BlocksManagerPtr)
        , BlobMappingCachePtr(new TBlobMappingCache(*this))
        , BlobMappingCache(*BlobMappingCachePtr)
    {
        if (info) {
            Y_ABORT_UNLESS(info->BlobDepotId);
            TabletId = *info->BlobDepotId;
            LogId = TStringBuilder() << '{' << TabletId << '@' << virtualGroupId << '}';
        } else {
            LogId = TStringBuilder() << '{' << '?' << '@' << virtualGroupId << "}";
        }
    }

    TBlobDepotAgent::~TBlobDepotAgent() {
        TRequestSender::ClearRequestsInFlight();
    }

    void TBlobDepotAgent::Bootstrap() {
        Become(&TThis::StateFunc);

        SetupCounters();

        if (TabletId && TabletId != Max<ui64>()) {
            ConnectToBlobDepot();
        }

        HandleQueryWatchdog();
        HandlePendingEventQueueWatchdog();
        HandlePushMetrics();
    }

    void TBlobDepotAgent::SetupCounters() {
        AgentCounters = GetServiceCounters(AppData()->Counters, "blob_depot_agent")
            ->GetSubgroup("group", ::ToString(VirtualGroupId));

        auto connectivity = AgentCounters->GetSubgroup("subsystem", "connectivity");

        ModeConnectPending = connectivity->GetCounter("Mode/ConnectPending", false);
        ModeRegistering = connectivity->GetCounter("Mode/Registering", false);
        ModeConnected = connectivity->GetCounter("Mode/Connected", false);

        auto pendingEventQueue = AgentCounters->GetSubgroup("subsystem", "pendingEventQueue");

        PendingEventQueueItems = pendingEventQueue->GetCounter("Items", false);
        PendingEventQueueBytes = pendingEventQueue->GetCounter("Bytes", false);

        auto requests = AgentCounters->GetSubgroup("subsystem", "requests");

        auto makeHist = [&] {
            return NMonitoring::ExplicitHistogram({
                0.25, 0.5,
                1, 2, 4, 8, 16, 32, 64, 128, 256, 512,
                1024, 2048, 4096, 8192, 16384, 32768,
                65536
            });
        };

#define XX(ITEM) \
        do { \
            auto subgroup = requests->GetSubgroup("request", #ITEM); \
            RequestsReceived[TEvBlobStorage::ITEM] = subgroup->GetCounter("Received", true); \
            SuccessResponseTime[TEvBlobStorage::ITEM] = subgroup->GetNamedHistogram("sensor", "SuccessResponseTime_us", makeHist()); \
            ErrorResponseTime[TEvBlobStorage::ITEM] = subgroup->GetNamedHistogram("sensor", "ErrorResponseTime_us", makeHist()); \
        } while (false);

        ENUMERATE_INCOMING_EVENTS(XX)
#undef XX

        auto s3 = AgentCounters->GetSubgroup("subsystem", "s3");

        S3GetBytesOk = s3->GetCounter("GetBytesOk", true);
        S3GetsOk = s3->GetCounter("GetsOk", true);
        S3GetsError = s3->GetCounter("GetsError", true);

        S3PutBytesOk = s3->GetCounter("PutBytesOk", true);
        S3PutsOk = s3->GetCounter("PutsOk", true);
        S3PutsError = s3->GetCounter("PutsError", true);
    }

    void TBlobDepotAgent::SwitchMode(EMode mode) {
        auto getCounter = [&](EMode mode) -> NMonitoring::TCounterForPtr* {
            switch (mode) {
                case EMode::None:           return nullptr;
                case EMode::ConnectPending: return ModeConnectPending.Get();
                case EMode::Registering:    return ModeRegistering.Get();
                case EMode::Connected:      return ModeConnected.Get();
            }
        };

        if (Mode != mode) {
            if (auto *p = getCounter(Mode)) {
                --*p;
            }
            if (auto *p = getCounter(mode)) {
                ++*p;
            }
            Mode = mode;
        }
    }

    IActor *CreateBlobDepotAgent(ui32 virtualGroupId, TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId proxyId) {
        return new TBlobDepotAgent(virtualGroupId, std::move(info), proxyId);
    }

} // NKikimr::NBlobDepot
