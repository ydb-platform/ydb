#include "keep_check.h"

#include <ydb/library/actors/async/sleep.h>
#include <ydb/library/actors/async/timeout.h>
#include <ydb/library/actors/async/wait_for_event.h>

namespace NKikimr::NBridge {
namespace {

    class TKeepCheckActor : public TActor<TKeepCheckActor> {
        const TActorId Parent;
        const TVector<TKeepCheckPile> Sources;
        const TKeepCheckPile Target;
        TVector<TLogoBlobID> Kept;
        const bool WaitForCheck;
        static constexpr ui32 MaxAttempts = 3;
        static constexpr TDuration Timeout = TDuration::Seconds(5);

        template<class TResult, class TRequest>
        async<TAutoPtr<TResult>> Query(TKeepCheckPile pile, std::unique_ptr<TRequest> request) {
            request->ForceGroupGeneration = pile.Generation;
            const ui64 cookie = AllocateWaitCookie();
            SendToBSProxy(SelfId(), pile.GroupId, request.release(), cookie);
            auto reply = co_await WithTimeout(Timeout, [cookie]() -> async<typename TResult::TPtr> {
                co_return co_await ActorWaitForEvent<TResult>(cookie);
            });
            co_return reply ? (*reply)->Release() : TAutoPtr<TResult>();
        }

        async<TVector<TLogoBlobID>> ReleasedOn(TKeepCheckPile source, TVector<TLogoBlobID> pending) {
            TVector<TLogoBlobID> released;
            for (ui32 i = 0; i < MaxAttempts && !pending.empty(); ++i) {
                if (i) {
                    co_await AsyncSleepFor(TDuration::MilliSeconds(100 * i));
                }
                TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queries(new TEvBlobStorage::TEvGet::TQuery[pending.size()]);
                for (size_t j = 0; j < pending.size(); ++j) {
                    queries[j].Set(pending[j]);
                }
                auto get = std::make_unique<TEvBlobStorage::TEvGet>(queries, pending.size(), TActivationContext::Now() + Timeout,
                    NKikimrBlobStorage::FastRead, false, true);
                get->VerifyKeepFlags = true;
                auto reply = co_await Query<TEvBlobStorage::TEvGetResult>(source, std::move(get));
                if (!reply || reply->Status != NKikimrProto::OK) {
                    continue;
                }
                Y_ABORT_UNLESS(reply->ResponseSz == pending.size());
                pending.clear();
                for (ui32 j = 0; j < reply->ResponseSz; ++j) {
                    const auto& item = reply->Responses[j];
                    if (item.Status == NKikimrProto::NODATA || (item.Status == NKikimrProto::OK && item.DoNotKeep)) {
                        released.push_back(item.Id);
                    } else if (item.Status != NKikimrProto::OK || !item.Keep) {
                        pending.push_back(item.Id);
                    }
                }
            }
            co_return released;
        }

        void Check(TAutoPtr<IEventHandle> reply) {
            Become(&TThis::StateChecking);
            auto *result = dynamic_cast<TEvBlobStorage::TEvResultCommon*>(reply->GetBase());
            if (!result || result->Status != NKikimrProto::OK) {
                TActivationContext::Send(reply->Forward(Parent));
                PassAway();
                co_return;
            }
            if (!WaitForCheck) {
                TActivationContext::Send(reply->Forward(Parent));
                result = nullptr;
            }
            for (const auto& source : Sources) {
                Kept = co_await ReleasedOn(source, std::move(Kept));
                if (Kept.empty()) {
                    break;
                }
            }

            // Syncer flag batches belong to one tablet but can span channels.
            THashMap<ui32, TVector<TLogoBlobID>> byChannel;
            for (const auto& id : Kept) {
                byChannel[id.Channel()].push_back(id);
            }
            for (const auto& [channel, ids] : byChannel) {
                for (ui32 i = 0; i < MaxAttempts; ++i) {
                    auto gc = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(ids.front().TabletID(), Max<ui32>(), 0,
                        channel, false, 0, 0, nullptr, new TVector<TLogoBlobID>(ids), TActivationContext::Now() + Timeout,
                        true, TWriteSource::BridgeProxyRestorePut, false, true);
                    auto ack = co_await Query<TEvBlobStorage::TEvCollectGarbageResult>(Target, std::move(gc));
                    if (ack && ack->Status == NKikimrProto::OK) {
                        break;
                    } else if (result && i + 1 == MaxAttempts) {
                        result->Status = NKikimrProto::ERROR;
                        result->ErrorReason = "could not release repaired Keep";
                    }
                }
            }
            if (result) {
                TActivationContext::Send(reply->Forward(Parent));
            }
            PassAway();
        }

        STATEFN(StateWrite) {
            Check(std::move(ev));
        }

        STATEFN(StateChecking) {
            Y_UNUSED(ev); // late replies to timed-out attempts
        }

    public:
        TKeepCheckActor(TActorId parent, TVector<TKeepCheckPile> sources, TKeepCheckPile target, TVector<TLogoBlobID> kept,
                bool waitForCheck)
            : TActor(&TThis::StateWrite)
            , Parent(parent)
            , Sources(std::move(sources))
            , Target(target)
            , Kept(std::move(kept))
            , WaitForCheck(waitForCheck)
        {}
    };

} // namespace

    IActor* CreateKeepCheckActor(TActorId parent, TVector<TKeepCheckPile> sources, TKeepCheckPile target, TVector<TLogoBlobID> kept,
            bool waitForCheck) {
        return new TKeepCheckActor(parent, std::move(sources), target, std::move(kept), waitForCheck);
    }

} // NKikimr::NBridge
