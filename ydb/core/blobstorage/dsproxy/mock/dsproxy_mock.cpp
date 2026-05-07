#include "dsproxy_mock.h"
#include "model.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/util/stlog.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

namespace NKikimr {

    namespace {

        class TBlobStorageGroupProxyMockActor
            : public TActor<TBlobStorageGroupProxyMockActor>
        {
            TIntrusivePtr<NFake::TProxyDS> Model;

            void Handle(TEvBlobStorage::TEvPut::TPtr& ev) {
                YDBLOG_COMP_DEBUG(BS_PROXY, "TEvPut", {"Marker", "BSPM01"},
                    {"Msg", ev->Get()->ToString()});
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvGet::TPtr& ev) {
                YDBLOG_COMP_DEBUG(BS_PROXY, "TEvGet", {"Marker", "BSPM02"},
                    {"Msg", ev->Get()->ToString()});
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvBlock::TPtr& ev) {
                YDBLOG_COMP_DEBUG(BS_PROXY, "TEvBlock", {"Marker", "BSPM03"},
                    {"Msg", ev->Get()->ToString()});
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvDiscover::TPtr& ev) {
                YDBLOG_COMP_DEBUG(BS_PROXY, "TEvDiscover", {"Marker", "BSPM04"},
                    {"Msg", ev->Get()->ToString()});
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvRange::TPtr& ev) {
                YDBLOG_COMP_DEBUG(BS_PROXY, "TEvRange", {"Marker", "BSPM05"},
                    {"Msg", ev->Get()->ToString()});
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvCollectGarbage::TPtr& ev) {
                YDBLOG_COMP_DEBUG(BS_PROXY, "TEvCollectGarbage", {"Marker", "BSPM06"},
                    {"Msg", ev->Get()->ToString()});
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvStatus::TPtr& ev) {
                YDBLOG_COMP_DEBUG(BS_PROXY, "TEvStatus", {"Marker", "BSPM07"},
                    {"Msg", ev->Get()->ToString()});
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), new TEvBlobStorage::TEvStatusResult(NKikimrProto::OK,
                    Model->GetStorageStatusFlags())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvAssimilate::TPtr& ev) {
                YDBLOG_COMP_DEBUG(BS_PROXY, "TEvAssimilate", {"Marker", "BSPM09"},
                    {"Msg", ev->Get()->ToString()});
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), new TEvBlobStorage::TEvAssimilateResult(NKikimrProto::ERROR,
                    "not implemented")), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvPatch::TPtr& ev) {
                YDBLOG_COMP_DEBUG(BS_PROXY, "TEvPatch", {"Marker", "BSPM10"},
                    {"Msg", ev->Get()->ToString()});
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvGetBlock::TPtr& ev) {
                YDBLOG_COMP_DEBUG(BS_PROXY, "TEvGetBlock", {"Marker", "BSPM11"},
                    {"Msg", ev->Get()->ToString()});
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            void Handle(TEvBlobStorage::TEvCheckIntegrity::TPtr& ev) {
                YDBLOG_COMP_DEBUG(BS_PROXY, "TEvCheckIntegrity", {"Marker", "BSPM12"},
                    {"Msg", ev->Get()->ToString()});
                Send(ev->Sender, CopyExecutionRelay(ev->Get(), Model->Handle(ev->Get())), 0, ev->Cookie);
            }

            template<typename TOut, typename TIn>
            TOut *CopyExecutionRelay(TIn *in, TOut *out) {
                out->ExecutionRelay = std::move(in->ExecutionRelay);
                return out;
            }

            void HandlePoison(TEvents::TEvPoisonPill::TPtr& ev) {
                YDBLOG_COMP_DEBUG(BS_PROXY, "TEvPoisonPill", {"Marker", "BSPM08"});
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
    } // anon

    IActor *CreateBlobStorageGroupProxyMockActor(TIntrusivePtr<NFake::TProxyDS> model) {
        return new TBlobStorageGroupProxyMockActor(std::move(model));
    }

    IActor *CreateBlobStorageGroupProxyMockActor(TGroupId groupId) {
        return new TBlobStorageGroupProxyMockActor(groupId);
    }

} // NKikimr
