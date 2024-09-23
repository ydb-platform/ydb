#include "sequenceproxy_impl.h"

#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/sequenceshard/public/events.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>

#include <ydb/library/actors/core/log.h>
#include <util/string/builder.h>

#define TXLOG_LOG(priority, stream) \
    LOG_LOG_S(*TlsActivationContext, priority, NKikimrServices::SEQUENCEPROXY, LogPrefix << stream)
#define TXLOG_DEBUG(stream) TXLOG_LOG(NActors::NLog::PRI_DEBUG, stream)
#define TXLOG_NOTICE(stream) TXLOG_LOG(NActors::NLog::PRI_NOTICE, stream)
#define TXLOG_ERROR(stream) TXLOG_LOG(NActors::NLog::PRI_ERROR, stream)

namespace NKikimr {
namespace NSequenceProxy {

    using namespace NKikimr::NSequenceShard;

    class TSequenceProxy::TAllocateActor : public TActorBootstrapped<TAllocateActor> {
    public:
        TAllocateActor(const TActorId& owner, ui64 cookie, ui64 tabletId, const TPathId& pathId, ui64 cache)
            : Owner(owner)
            , Cookie(cookie)
            , PipeCache(MakePipePerNodeCacheID(true))
            , TabletId(tabletId)
            , PathId(pathId)
            , Cache(cache)
        { }

        void Bootstrap() {
            Send(PipeCache,
                new TEvPipeCache::TEvForward(
                    new TEvSequenceShard::TEvAllocateSequence(PathId, Cache),
                    TabletId,
                    true),
                IEventHandle::FlagTrackDelivery);
            Become(&TThis::StateAlloc);
        }

        void PassAway() override {
            Send(PipeCache, new TEvPipeCache::TEvUnlink(0));
            TActorBootstrapped::PassAway();
        }

        void ReplyAndDie(Ydb::StatusIds::StatusCode status, NKikimrIssues::TIssuesIds::EIssueCode code, const TString& message) {
            NYql::TIssueManager issueManager;
            issueManager.RaiseIssue(MakeIssue(code, message));
            auto res = MakeHolder<TEvPrivate::TEvAllocateResult>();
            res->Status = status;
            res->Issues = issueManager.GetIssues();
            Send(Owner, res.Release(), 0, Cookie);
            PassAway();
        }

    private:
        STFUNC(StateAlloc) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvSequenceShard::TEvAllocateSequenceResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvents::TEvUndelivered, Handle);
            }
        }

        void Handle(TEvSequenceShard::TEvAllocateSequenceResult::TPtr& ev) {
            auto* msg = ev->Get();
            switch (msg->Record.GetStatus()) {
                case NKikimrTxSequenceShard::TEvAllocateSequenceResult::SUCCESS:
                    break;

                case NKikimrTxSequenceShard::TEvAllocateSequenceResult::SEQUENCE_NOT_FOUND:
                    return ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR,
                        NKikimrIssues::TIssuesIds::PATH_NOT_EXIST,
                        "The specified sequence no longer exists");

                case NKikimrTxSequenceShard::TEvAllocateSequenceResult::SEQUENCE_FROZEN:
                    return ReplyAndDie(Ydb::StatusIds::UNAVAILABLE,
                        NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE,
                        TStringBuilder()
                            << "sequence shard " << TabletId
                            << " has sequence " << PathId
                            << " temporarily frozen");

                case NKikimrTxSequenceShard::TEvAllocateSequenceResult::SEQUENCE_MOVED:
                    // Switch to a different tablet and retry transparently
                    TabletId = msg->Record.GetMovedTo();
                    return Bootstrap();

                case NKikimrTxSequenceShard::TEvAllocateSequenceResult::SEQUENCE_OVERFLOW:
                    return ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR,
                        NKikimrIssues::TIssuesIds::ENGINE_ERROR,
                        TStringBuilder()
                            << "sequence " << PathId
                            << " doesn't have any more values available");

                default:
                    return ReplyAndDie(Ydb::StatusIds::INTERNAL_ERROR,
                        NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR,
                        TStringBuilder()
                            << "Unexpected error from sequence shard " << TabletId);
            }

            auto res = MakeHolder<TEvPrivate::TEvAllocateResult>();
            res->Status = Ydb::StatusIds::SUCCESS;
            res->TabletId = TabletId;
            res->PathId = PathId;
            res->Start = msg->Record.GetAllocationStart();
            res->Increment = msg->Record.GetAllocationIncrement();
            res->Count = msg->Record.GetAllocationCount();
            Send(Owner, res.Release(), 0, Cookie);
            PassAway();
        }

        void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
            ui64 tabletId = ev->Get()->TabletId;
            if (tabletId == TabletId) {
                const TString error = TStringBuilder()
                    << "sequence shard " << tabletId << " is unavailable";
                return ReplyAndDie(Ydb::StatusIds::UNAVAILABLE, NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, error);
            }
        }

        void Handle(TEvents::TEvUndelivered::TPtr&) {
            const TString error = "Pipe cache may not be configured correctly";
            return ReplyAndDie(Ydb::StatusIds::UNAVAILABLE, NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR, error);
        }

    private:
        const TActorId Owner;
        const ui64 Cookie;
        const TActorId PipeCache;
        ui64 TabletId;
        TPathId PathId;
        ui64 Cache;
        NYql::TIssueManager IssueManager;
    };

    ui64 TSequenceProxy::StartAllocate(ui64 tabletId, const TString& database, const TPathId& pathId, ui64 cache) {
        ui64 cookie = ++LastCookie;
        auto& info = AllocateInFlight[cookie];
        info.Database = database;
        info.PathId = pathId;
        Counters->SequenceShardAllocateCount->Collect(cache);
        Register(new TAllocateActor(SelfId(), cookie, tabletId, pathId, cache));
        return cookie;
    }

} // namespace NSequenceProxy
} // namespace NKikimr
