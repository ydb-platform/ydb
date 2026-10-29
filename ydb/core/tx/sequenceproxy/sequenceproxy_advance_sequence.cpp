#include "sequenceproxy_impl.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/sequenceshard/public/events.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <yql/essentials/public/issue/yql_issue_manager.h>

#include <ydb/library/actors/core/log.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NSequenceProxy {

    using namespace NKikimr::NSequenceShard;

    class TSequenceProxy::TAdvanceSequenceActor : public TActorBootstrapped<TAdvanceSequenceActor> {
    public:
        TAdvanceSequenceActor(const TActorId& owner, ui64 cookie, const TString& database,
                const std::variant<TString, TPathId>& path, i64 observedMin, i64 observedMax)
            : Owner(owner)
            , Cookie(cookie)
            , PipeCache(MakePipePerNodeCacheID(true))
            , Database(database)
            , Path(path)
            , ObservedMin(observedMin)
            , ObservedMax(observedMax)
        { }

        void Bootstrap() {
            Resolve();
        }

        void PassAway() override {
            Send(PipeCache, new TEvPipeCache::TEvUnlink(0));
            TActorBootstrapped::PassAway();
        }

    private:
        void Resolve() {
            auto req = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
            req->DatabaseName = Database;

            auto& entry = req->ResultSet.emplace_back();
            std::visit(
                [&entry](const auto& path) {
                    using TPath = std::decay_t<decltype(path)>;
                    if constexpr (std::is_same_v<TPath, TString>) {
                        entry.Path = SplitPath(path);
                        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
                    } else {
                        entry.TableId.PathId = path;
                        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
                    }
                },
                Path);
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            entry.ShowPrivatePath = true;

            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(req.Release()), 0, ++ResolveCookie);
            Become(&TThis::StateResolve);
        }

        void SendResult(THolder<TEvSequenceProxy::TEvAdvanceSequenceResult> result) {
            Send(Owner, result.Release(), 0, Cookie);
        }

        void ReplyAndDie(Ydb::StatusIds::StatusCode status, NKikimrIssues::TIssuesIds::EIssueCode code, const TString& message) {
            NYql::TIssueManager issueManager;
            issueManager.RaiseIssue(MakeIssue(code, message));
            SendResult(MakeHolder<TEvSequenceProxy::TEvAdvanceSequenceResult>(status, issueManager.GetIssues()));
            PassAway();
        }

        void SendToShard() {
            auto request = MakeHolder<TEvSequenceShard::TEvAdvanceSequence>(PathId);
            request->Record.SetObservedMin(ObservedMin);
            request->Record.SetObservedMax(ObservedMax);
            Send(PipeCache,
                new TEvPipeCache::TEvForward(request.Release(), TabletId, true),
                IEventHandle::FlagTrackDelivery);
            Become(&TThis::StateAdvance);
        }

        TString DescribePath() const {
            return std::visit(
                [](const auto& path) {
                    using TPath = std::decay_t<decltype(path)>;
                    if constexpr (std::is_same_v<TPath, TString>) {
                        return path;
                    } else {
                        return TString(TStringBuilder() << path);
                    }
                },
                Path);
        }

    private:
        STFUNC(StateResolve) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleResolve);
            }
        }

        STFUNC(StateAdvance) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvSequenceShard::TEvAdvanceSequenceResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvents::TEvUndelivered, Handle);
            }
        }

        void HandleResolve(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            if (ev->Cookie != ResolveCookie) {
                // A stale response from a previous resolve attempt.
                return;
            }

            auto req = std::move(ev->Get()->Request);
            if (req->ErrorCount > 0 || req->ResultSet.size() != 1) {
                return ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR,
                    NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR,
                    TStringBuilder() << "Failed to resolve sequence " << DescribePath());
            }

            auto& entry = req->ResultSet.at(0);
            if (!entry.SequenceInfo || entry.SequenceInfo->Description.GetSequenceShard() == 0) {
                return ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR,
                    NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR,
                    TStringBuilder() << "Failed to resolve sequence " << DescribePath());
            }

            PathId = entry.TableId.PathId;
            TabletId = entry.SequenceInfo->Description.GetSequenceShard();
            SendToShard();
        }

        void Handle(TEvSequenceShard::TEvAdvanceSequenceResult::TPtr& ev) {
            auto* msg = ev->Get();
            switch (msg->Record.GetStatus()) {
                case NKikimrTxSequenceShard::TEvAdvanceSequenceResult::SUCCESS:
                    break;

                case NKikimrTxSequenceShard::TEvAdvanceSequenceResult::SEQUENCE_NOT_FOUND:
                    return ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR,
                        NKikimrIssues::TIssuesIds::PATH_NOT_EXIST,
                        "The specified sequence no longer exists");

                case NKikimrTxSequenceShard::TEvAdvanceSequenceResult::SEQUENCE_MOVED:
                    // Switch to a different tablet and retry transparently
                    TabletId = msg->Record.GetMovedTo();
                    return SendToShard();

                default:
                    return ReplyAndDie(Ydb::StatusIds::INTERNAL_ERROR,
                        NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR,
                        TStringBuilder()
                            << "Unexpected error from sequence shard " << TabletId);
            }

            SendResult(MakeHolder<TEvSequenceProxy::TEvAdvanceSequenceResult>(
                PathId, msg->Record.GetNextValue(), msg->Record.GetNextUsed()));
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
        const TString Database;
        const std::variant<TString, TPathId> Path;
        const i64 ObservedMin;
        const i64 ObservedMax;

        ui64 ResolveCookie = 0;
        ui64 TabletId = 0;
        TPathId PathId;
    };

    void TSequenceProxy::Handle(TEvSequenceProxy::TEvAdvanceSequence::TPtr& ev) {
        auto* msg = ev->Get();
        Register(new TAdvanceSequenceActor(ev->Sender, ev->Cookie, msg->Database, msg->Path, msg->ObservedMin, msg->ObservedMax));
    }

} // namespace NSequenceProxy
} // namespace NKikimr
