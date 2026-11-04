#include "sequenceproxy_impl.h"

#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/sequenceshard/public/events.h>
#include <yql/essentials/public/issue/yql_issue_manager.h>

#include <ydb/library/actors/core/log.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NSequenceProxy {

    using namespace NKikimr::NSequenceShard;

    class TSequenceProxy::TAdvanceSequenceActor : public TActorBootstrapped<TAdvanceSequenceActor> {
    public:
        TAdvanceSequenceActor(const TActorId& owner, ui64 cookie, ui64 tabletId, const TPathId& pathId,
                i64 observedMin, i64 observedMax)
            : Owner(owner)
            , Cookie(cookie)
            , PipeCache(MakePipePerNodeCacheID(true))
            , TabletId(tabletId)
            , PathId(pathId)
            , ObservedMin(observedMin)
            , ObservedMax(observedMax)
        { }

        void Bootstrap() {
            auto request = MakeHolder<TEvSequenceShard::TEvAdvanceSequence>(PathId);
            request->Record.SetObservedMin(ObservedMin);
            request->Record.SetObservedMax(ObservedMax);
            Send(PipeCache,
                new TEvPipeCache::TEvForward(request.Release(), TabletId, true),
                IEventHandle::FlagTrackDelivery);
            Become(&TThis::StateAdvance);
        }

        void PassAway() override {
            Send(PipeCache, new TEvPipeCache::TEvUnlink(0));
            TActorBootstrapped::PassAway();
        }

        void ReplyAndDie(Ydb::StatusIds::StatusCode status, NKikimrIssues::TIssuesIds::EIssueCode code, const TString& message) {
            NYql::TIssueManager issueManager;
            issueManager.RaiseIssue(MakeIssue(code, message));
            auto res = MakeHolder<TEvPrivate::TEvAdvanceResult>();
            res->Status = status;
            res->Issues = issueManager.GetIssues();
            res->Moved = Moved;
            Send(Owner, res.Release(), 0, Cookie);
            PassAway();
        }

    private:
        STFUNC(StateAdvance) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvSequenceShard::TEvAdvanceSequenceResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvents::TEvUndelivered, Handle);
            }
        }

        void Handle(TEvSequenceShard::TEvAdvanceSequenceResult::TPtr& ev) {
            auto* msg = ev->Get();
            switch (msg->Record.GetStatus()) {
                case NKikimrTxSequenceShard::TEvAdvanceSequenceResult::SUCCESS:
                    break;

                case NKikimrTxSequenceShard::TEvAdvanceSequenceResult::PIPE_OUTDATED:
                    return ReplyAndDie(Ydb::StatusIds::UNAVAILABLE,
                        NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE,
                        TStringBuilder() << "pipe to sequence shard " << TabletId << " is outdated");

                case NKikimrTxSequenceShard::TEvAdvanceSequenceResult::SEQUENCE_NOT_FOUND:
                    return ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR,
                        NKikimrIssues::TIssuesIds::PATH_NOT_EXIST,
                        "The specified sequence no longer exists");

                case NKikimrTxSequenceShard::TEvAdvanceSequenceResult::SEQUENCE_FROZEN:
                    return ReplyAndDie(Ydb::StatusIds::UNAVAILABLE,
                        NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE,
                        TStringBuilder()
                            << "sequence shard " << TabletId
                            << " has sequence " << PathId
                            << " temporarily frozen");

                case NKikimrTxSequenceShard::TEvAdvanceSequenceResult::SEQUENCE_MOVED:
                    // Switch to a different tablet and retry transparently
                    TabletId = msg->Record.GetMovedTo();
                    Moved = true;
                    return Bootstrap();

                default:
                    return ReplyAndDie(Ydb::StatusIds::INTERNAL_ERROR,
                        NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR,
                        TStringBuilder()
                            << "Unexpected error from sequence shard " << TabletId);
            }

            auto res = MakeHolder<TEvPrivate::TEvAdvanceResult>();
            res->Status = Ydb::StatusIds::SUCCESS;
            res->PathId = PathId;
            res->NextValue = msg->Record.GetNextValue();
            res->NextUsed = msg->Record.GetNextUsed();
            res->Moved = Moved;
            Send(Owner, res.Release(), 0, Cookie);
            PassAway();
        }

        void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
            if (ev->Get()->TabletId == TabletId) {
                return ReplyAndDie(Ydb::StatusIds::UNAVAILABLE,
                    NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE,
                    TStringBuilder() << "sequence shard " << TabletId << " is unavailable");
            }
        }

        void Handle(TEvents::TEvUndelivered::TPtr&) {
            return ReplyAndDie(Ydb::StatusIds::UNAVAILABLE,
                NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR,
                "Pipe cache may not be configured correctly");
        }

    private:
        const TActorId Owner;
        const ui64 Cookie;
        const TActorId PipeCache;
        ui64 TabletId;
        const TPathId PathId;
        const i64 ObservedMin;
        const i64 ObservedMax;
        bool Moved = false;
    };

    void TSequenceProxy::StartAdvance(ui64 tabletId, const TString& database, const TPathId& pathId, const TAdvanceRequestInfo& request) {
        ui64 cookie = ++LastCookie;
        auto& info = AdvanceInFlight[cookie];
        info.Database = database;
        info.PathId = pathId;
        info.Request = request;
        Register(new TAdvanceSequenceActor(SelfId(), cookie, tabletId, pathId,
            request.ObservedMin, request.ObservedMax));
    }

} // namespace NSequenceProxy
} // namespace NKikimr
