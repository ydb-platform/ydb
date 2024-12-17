#include "offload_actor.h"

#include <ydb/core/backup/impl/local_partition_reader.h>
#include <ydb/core/backup/impl/table_writer.h>
#include <ydb/core/persqueue/write_meta.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/replication/service/table_writer.h>
#include <ydb/core/tx/replication/service/worker.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

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
{
private:
    const TActorId ParentTablet;
    const ui32 Partition;
    const NKikimrPQ::TOffloadConfig Config;

    mutable TMaybe<TString> LogPrefix;
    TActorId Worker;

    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[OffloadActor]"
                << "[" << ParentTablet << "]"
                << "[" << Partition << "]"
                << SelfId() << " ";
        }

        return LogPrefix.GetRef();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BACKUP_PQ_OFFLOAD_ACTOR;
    }

    TOffloadActor(TActorId parentTablet, ui32 partition, const NKikimrPQ::TOffloadConfig& config)
        : ParentTablet(parentTablet)
        , Partition(partition)
        , Config(config)
    {}

    auto CreateReaderFactory() {
        return [=]() -> IActor* {
            return NBackup::NImpl::CreateLocalPartitionReader(ParentTablet, Partition);
        };
    }

    auto CreateWriterFactory() {
        return [=]() -> IActor* {
            if (Config.HasIncrementalBackup()) {
                return NBackup::NImpl::CreateLocalTableWriter(PathIdFromPathId(Config.GetIncrementalBackup().GetDstPathId()));
            } else {
                return NBackup::NImpl::CreateLocalTableWriter(
                    PathIdFromPathId(Config.GetIncrementalRestore().GetDstPathId()),
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

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
        default:
            LOG_W("Unhandled event type: " << ev->GetTypeRewrite() << " event: " << ev->ToString());
        }
    }
};

IActor* CreateOffloadActor(TActorId parentTablet, TPartitionId partition, const NKikimrPQ::TOffloadConfig& config) {
    return new TOffloadActor(parentTablet, partition.OriginalPartitionId, config);
}

} // namespace NKikimr::NPQ
