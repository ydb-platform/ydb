#include "offload_actor.h"

#include <ydb/core/backup/impl/local_partition_reader.h>
#include <ydb/core/backup/impl/table_writer.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/write_meta.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/replication/service/table_writer.h>
#include <ydb/core/tx/replication/service/worker.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/core/tx/scheme_cache/helpers.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#define LOG_T(stream) LOG_TRACE_S (*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, GetLogPrefix() << stream)
#define LOG_D(stream) LOG_DEBUG_S (*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, GetLogPrefix() << stream)
#define LOG_I(stream) LOG_INFO_S  (*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, GetLogPrefix() << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, GetLogPrefix() << stream)
#define LOG_W(stream) LOG_WARN_S  (*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, GetLogPrefix() << stream)
#define LOG_E(stream) LOG_ERROR_S (*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, GetLogPrefix() << stream)
#define LOG_C(stream) LOG_CRIT_S  (*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, GetLogPrefix() << stream)

using namespace NKikimr::NReplication::NService;
using namespace NKikimr::NReplication;

namespace NKikimr::NPQ {

class TOffloadActor
    : public TActorBootstrapped<TOffloadActor>
    , private NSchemeCache::TSchemeCacheHelpers
{
private:
    const TActorId ParentTablet;
    const ui64 TabletID;
    const ui32 Partition;
    const NKikimrPQ::TOffloadConfig Config;

    mutable TMaybe<TString> LogPrefix;
    TActorId Worker;
    TActorId SchemeShardPipe;

    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[OffloadActor]"
                << "[" << ParentTablet << "]"
                << "[" << TabletID << "]"
                << "[" << Partition << "]"
                << SelfId() << " ";
        }

        return LogPrefix.GetRef();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BACKUP_PQ_OFFLOAD_ACTOR;
    }

    TOffloadActor(TActorId parentTablet, ui64 tabletId, ui32 partition, const NKikimrPQ::TOffloadConfig& config)
        : ParentTablet(parentTablet)
        , TabletID(tabletId)
        , Partition(partition)
        , Config(config)
    {}

    auto CreateReaderFactory() {
        return [=, this]() -> IActor* {
            return NBackup::NImpl::CreateLocalPartitionReader(ParentTablet, Partition);
        };
    }

    auto CreateWriterFactory() {
        return [=, this]() -> IActor* {
            if (Config.HasIncrementalBackup()) {
                return NBackup::NImpl::CreateLocalTableWriter(TPathId::FromProto(Config.GetIncrementalBackup().GetDstPathId()));
            } else {
                return NBackup::NImpl::CreateLocalTableWriter(
                    TPathId::FromProto(Config.GetIncrementalRestore().GetDstPathId()),
                    NBackup::NImpl::EWriterType::Restore);
            }
        };
    }

    void Bootstrap() {
        auto* workerActor = CreateWorker(
            SelfId(),
            CreateReaderFactory(),
            CreateWriterFactory());

        Worker = TActivationContext::Register(workerActor);

        Become(&TOffloadActor::StateWork);
    }

    void Handle(TEvWorker::TEvGone::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        if (ev->Get()->Status == TEvWorker::TEvGone::DONE) {
            NotifySchemeShard();
        }
    }

    void NotifySchemeShard() {
        if (!SchemeShardPipe && Config.GetIncrementalBackup().HasDstPathId()) {
            ui64 schemeShardId = Config.GetIncrementalBackup().GetDstPathId().GetOwnerId();
            NTabletPipe::TClientConfig clientConfig;
            clientConfig.RetryPolicy = {.RetryLimitCount = 3};
            SchemeShardPipe = Register(NTabletPipe::CreateClient(SelfId(), schemeShardId));

            auto request = std::make_unique<TEvPersQueue::TEvOffloadStatus>();
            request->Record.SetStatus(NKikimrPQ::TEvOffloadStatus::DONE);
            request->Record.SetTabletId(TabletID);
            request->Record.SetPartitionId(Partition);
            request->Record.SetTxId(Config.GetIncrementalBackup().GetTxId());

            NTabletPipe::SendData(SelfId(), SchemeShardPipe, request.release());
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        if (SchemeShardPipe == ev->Get()->ClientId) {
            OnPipeDestroyed();
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        if (SchemeShardPipe == ev->Get()->ClientId && ev->Get()->Status != NKikimrProto::OK) {
            NTabletPipe::CloseClient(SelfId(), SchemeShardPipe);
            OnPipeDestroyed();
        }
    }

    void OnPipeDestroyed() {
        SchemeShardPipe = TActorId();
        NotifySchemeShard();
    }

    void PassAway() override {
        if (SchemeShardPipe) {
            NTabletPipe::CloseClient(SelfId(), SchemeShardPipe);
        }
        TActor::PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWorker::TEvGone, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        default:
            LOG_W("Unhandled event type: " << ev->GetTypeRewrite() << " event: " << ev->ToString());
        }
    }
};

IActor* CreateOffloadActor(TActorId parentTablet, ui64 tabletId, TPartitionId partition, const NKikimrPQ::TOffloadConfig& config) {
    return new TOffloadActor(parentTablet, tabletId, partition.OriginalPartitionId, config);
}

} // namespace NKikimr::NPQ
