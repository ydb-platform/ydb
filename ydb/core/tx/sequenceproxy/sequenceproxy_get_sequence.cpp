#include "sequenceproxy_impl.h"

#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/sequenceshard/public/events.h>
#include <yql/essentials/public/issue/yql_issue_manager.h>

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

    class TSequenceProxy::TGetSequenceActor : public TActorBootstrapped<TGetSequenceActor> {
    public:
        TGetSequenceActor(const TActorId& owner, ui64 cookie, ui64 tabletId, const TPathId& pathId)
            : Owner(owner)
            , Cookie(cookie)
            , PipeCache(MakePipePerNodeCacheID(true))
            , TabletId(tabletId)
            , PathId(pathId)
        { }

        void Bootstrap() {
            Send(PipeCache,
                new TEvPipeCache::TEvForward(
                    new TEvSequenceShard::TEvGetSequence(PathId),
                    TabletId,
                    true),
                IEventHandle::FlagTrackDelivery);
            Become(&TThis::StateGetSequence);
        }

        void PassAway() override {
            Send(PipeCache, new TEvPipeCache::TEvUnlink(0));
            TActorBootstrapped::PassAway();
        }

        void ReplyAndDie(Ydb::StatusIds::StatusCode status, NKikimrIssues::TIssuesIds::EIssueCode code, const TString& message) {
            NYql::TIssueManager issueManager;
            issueManager.RaiseIssue(MakeIssue(code, message));
            auto res = MakeHolder<TEvSequenceProxy::TEvGetSequenceResult>(status, issueManager.GetIssues());
            Send(Owner, res.Release(), 0, Cookie);
            PassAway();
        }

    private:
        STFUNC(StateGetSequence) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvSequenceShard::TEvGetSequenceResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvents::TEvUndelivered, Handle);
            }
        }

        void Handle(TEvSequenceShard::TEvGetSequenceResult::TPtr& ev) {
            auto* msg = ev->Get();
            switch (msg->Record.GetStatus()) {
                case NKikimrTxSequenceShard::TEvGetSequenceResult::SUCCESS:
                    break;

                case NKikimrTxSequenceShard::TEvGetSequenceResult::SEQUENCE_NOT_FOUND:
                    return ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR,
                        NKikimrIssues::TIssuesIds::PATH_NOT_EXIST,
                        "The specified sequence no longer exists");

                case NKikimrTxSequenceShard::TEvGetSequenceResult::SEQUENCE_MOVED:
                    // Switch to a different tablet and retry transparently
                    TabletId = msg->Record.GetMovedTo();
                    return Bootstrap();

                default:
                    return ReplyAndDie(Ydb::StatusIds::INTERNAL_ERROR,
                        NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR,
                        TStringBuilder()
                            << "Unexpected error from sequence shard " << TabletId);
            }

            auto res = MakeHolder<TEvSequenceProxy::TEvGetSequenceResult>(PathId);
            res->MinValue = msg->Record.GetMinValue();
            res->MaxValue = msg->Record.GetMaxValue();
            res->StartValue = msg->Record.GetStartValue();
            res->NextValue = msg->Record.GetNextValue();
            res->NextUsed = msg->Record.GetNextUsed();
            res->Cache = msg->Record.GetCache();
            res->Increment = msg->Record.GetIncrement();
            res->Cycle = msg->Record.GetCycle();
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
        NYql::TIssueManager IssueManager;
    };

    ui64 TSequenceProxy::StartGetSequence(ui64 tabletId, const TString& database, const TPathId& pathId) {
        ui64 cookie = ++LastCookie;
        auto& info = GetSequenceInFlight[cookie];
        info.Database = database;
        info.PathId = pathId;
        Register(new TGetSequenceActor(SelfId(), cookie, tabletId, pathId));
        return cookie;
    }

} // namespace NSequenceProxy
} // namespace NKikimr
