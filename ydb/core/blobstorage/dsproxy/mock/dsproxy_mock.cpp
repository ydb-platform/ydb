#include "dsproxy_mock.h"
#include "model.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/util/stlog.h>
#include <util/random/fast.h>

namespace NKikimr {

    namespace {

        class TBlobStorageGroupProxyMockActor
            : public TActor<TBlobStorageGroupProxyMockActor>
        {
            TIntrusivePtr<NFake::TProxyDS> Model;

            void Handle(TEvBlobStorage::TEvPut::TPtr& ev) {
                STLOG(PRI_DEBUG, BS_PROXY, BSPM01, "TEvPut", (Msg, ev->Get()->ToString()));
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvGet::TPtr& ev) {
                STLOG(PRI_DEBUG, BS_PROXY, BSPM02, "TEvGet", (Msg, ev->Get()->ToString()));
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvBlock::TPtr& ev) {
                STLOG(PRI_DEBUG, BS_PROXY, BSPM03, "TEvBlock", (Msg, ev->Get()->ToString()));
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvDiscover::TPtr& ev) {
                STLOG(PRI_DEBUG, BS_PROXY, BSPM04, "TEvDiscover", (Msg, ev->Get()->ToString()));
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvRange::TPtr& ev) {
                STLOG(PRI_DEBUG, BS_PROXY, BSPM05, "TEvRange", (Msg, ev->Get()->ToString()));
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvCollectGarbage::TPtr& ev) {
                STLOG(PRI_DEBUG, BS_PROXY, BSPM06, "TEvCollectGarbage", (Msg, ev->Get()->ToString()));
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvStatus::TPtr& ev) {
                STLOG(PRI_DEBUG, BS_PROXY, BSPM07, "TEvStatus", (Msg, ev->Get()->ToString()));
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), new TEvBlobStorage::TEvStatusResult(NKikimrProto::OK,
                    Model->GetStorageStatusFlags())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvAssimilate::TPtr& ev) {
                STLOG(PRI_DEBUG, BS_PROXY, BSPM09, "TEvAssimilate", (Msg, ev->Get()->ToString()));
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), new TEvBlobStorage::TEvAssimilateResult(NKikimrProto::ERROR,
                    "not implemented")), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvPatch::TPtr& ev) {
                STLOG(PRI_DEBUG, BS_PROXY, BSPM10, "TEvPatch", (Msg, ev->Get()->ToString()));
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvGetBlock::TPtr& ev) {
                STLOG(PRI_DEBUG, BS_PROXY, BSPM11, "TEvGetBlock", (Msg, ev->Get()->ToString()));
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvCheckIntegrity::TPtr& ev) {
                STLOG(PRI_DEBUG, BS_PROXY, BSPM12, "TEvCheckIntegrity", (Msg, ev->Get()->ToString()));
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            template<typename TOut, typename TIn>
            TOut *CopyExecutionRelay(TIn *in, TOut *out) {
                out->ExecutionRelay = std::move(in->ExecutionRelay);
                return out;
            }

            void HandlePoison(TEvents::TEvPoisonPill::TPtr& ev) {
                STLOG(PRI_DEBUG, BS_PROXY, BSPM08, "TEvPoisonPill");
                Send(ev->Sender, new TEvents::TEvPoisonTaken);
                PassAway();
            }

            void Handle(TEvBlobStorage::TEvConfigureProxy::TPtr&/* ev*/) {
                //  do nothing, Model has neither monitoring counters nor Topology
            }

            STATEFN(StateFunc) {
                switch (const ui32 type = ev->GetTypeRewrite()) {
                    hFunc(TEvBlobStorage::TEvPut, Handle);
                    hFunc(TEvBlobStorage::TEvGet, Handle);
                    hFunc(TEvBlobStorage::TEvBlock, Handle);
                    hFunc(TEvBlobStorage::TEvGetBlock, Handle);
                    hFunc(TEvBlobStorage::TEvDiscover, Handle);
                    hFunc(TEvBlobStorage::TEvRange, Handle);
                    hFunc(TEvBlobStorage::TEvCollectGarbage, Handle);
                    hFunc(TEvBlobStorage::TEvStatus, Handle);
                    hFunc(TEvBlobStorage::TEvPatch, Handle);
                    hFunc(TEvBlobStorage::TEvCheckIntegrity, Handle);

                    hFunc(TEvents::TEvPoisonPill, HandlePoison);
                    hFunc(TEvBlobStorage::TEvConfigureProxy, Handle);

                    default:
                        Y_ABORT("unexpected event 0x%08" PRIx32, type);
                }
            }

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_PROXY_ACTOR;
            }

            TBlobStorageGroupProxyMockActor(TIntrusivePtr<NFake::TProxyDS> model)
                : TActor(&TBlobStorageGroupProxyMockActor::StateFunc)
                , Model(model ? std::move(model) : MakeIntrusive<NFake::TProxyDS>())
            {}


            TBlobStorageGroupProxyMockActor(TGroupId groupId)
                : TActor(&TBlobStorageGroupProxyMockActor::StateFunc)
                , Model(MakeIntrusive<NFake::TProxyDS>(groupId))
            {}
        };

        ////////////////////////////////////////////////////////////////////////////////

        class TBlobStorageFailureInjectingActor final
            : public TActor<TBlobStorageFailureInjectingActor>
        {
        private:
            const TActorId RealProxy;
            const TGroupId GroupId;
            const double FailureProbability;
            const ui64 RandomFailureSeed;
            const NKikimrProto::EReplyStatus ErrorReplyStatus;
            TFastRng64 Rng;
            const TString FailureErrorReason;

        public:
            TBlobStorageFailureInjectingActor(
                    TActorId realProxy,
                    TGroupId groupId,
                    TBSFailureInjectionConfig config)
                : TActor(&TThis::StateWork)
                , RealProxy(realProxy)
                , GroupId(groupId)
                , FailureProbability(std::clamp(config.GetFailureProbability(), 0.0, 1.0))
                , RandomFailureSeed(config.HasRandomSeed() ? config.GetRandomSeed() : RandomNumber<ui64>())
                , ErrorReplyStatus(config.GetErrorReplyStatus())
                , Rng(RandomFailureSeed)
                , FailureErrorReason(TStringBuilder()
                    << "injected by BSProxyInterceptor"
                    << " group " << GroupId
                    << " seed " << RandomFailureSeed)
            {
            }

        private:
            bool ShouldInjectFailure()
            {
                return Rng.GenRandReal4() < FailureProbability;
            }

            template <typename TRequest>
            bool MaybeInjectFailure(
                TAutoPtr<IEventHandle>& ev,
                TRequest& request,
                const TString& eventName)
            {
                if (!ShouldInjectFailure()) {
                    return false;
                }

                auto response = request.MakeErrorResponse(ErrorReplyStatus, FailureErrorReason, GroupId);
                response->ExecutionRelay = std::move(request.ExecutionRelay);

                LOG_WARN_S(*TlsActivationContext, NKikimrServices::BS_PROXY,
                    "[BSProxyInterceptor] group " << GroupId
                    << " injecting " << eventName
                    << " failure; not forwarding to " << RealProxy.ToString()
                    << " sender " << ev->Sender.ToString()
                    << " cookie " << ev->Cookie
                    << " probability " << FailureProbability
                    << " seed " << RandomFailureSeed);

                Send(ev->Sender, response.release(), 0, ev->Cookie);
                return true;
            }

            STFUNC(StateWork)
            {
                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_PROXY,
                    "[BSProxyInterceptor] group " << GroupId << " " << ev->GetTypeName() << " " << ev->ToString());

                #define HANDLE_EVENT(evType)                                        \
                    case evType::EventType: {                                       \
                        auto* msg = ev->Get<evType>();                              \
                        if (MaybeInjectFailure(ev, *msg, ev->GetTypeName())) {      \
                            return;                                                 \
                        }                                                           \
                        break;                                                      \
                    }

                switch (ev->GetTypeRewrite()) {
                    DSPROXY_ENUM_EVENTS(HANDLE_EVENT)
                    case TEvents::TEvPoison::EventType: {
                        TActor::PassAway();
                        [[fallthrough]];
                    }
                    default: {
                        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_PROXY,
                            "[BSProxyInterceptor] group " << GroupId
                            << " skipping event type " << ev->GetTypeName());
                        break;
                    }
                }

                TActivationContext::Forward(ev, RealProxy);
                #undef HANDLE_EVENT
            }
        };

    } // anon

    IActor *CreateBlobStorageGroupProxyMockActor(TIntrusivePtr<NFake::TProxyDS> model) {
        return new TBlobStorageGroupProxyMockActor(std::move(model));
    }

    IActor *CreateBlobStorageGroupProxyMockActor(TGroupId groupId) {
        return new TBlobStorageGroupProxyMockActor(groupId);
    }

    IActor *CreateBlobStorageGroupFailureInjectingActor(TActorId actorId, TGroupId groupId, const TBSFailureInjectionConfig& config) {
        return new TBlobStorageFailureInjectingActor(actorId, groupId, config);
    }

} // NKikimr
