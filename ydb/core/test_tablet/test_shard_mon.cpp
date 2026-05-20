#include "test_shard_impl.h"

namespace NKikimr::NTestShard {

    class TTestShard::TMonQueryActor : public TActorBootstrapped<TMonQueryActor> {
        TActorId Sender;
        const ui64 Cookie;
        const TString Query;
        const TCgiParameters Params;

        const TActorId ActivityActorId;
        TString ActivityActorHtml;
        ui32 RepliesPending = 0;

        ui64 Trash = 0;

        enum ECookie : ui64 {
            LOAD_ACTOR = 1,
        };

    public:
        TMonQueryActor(TTestShard *self, NMon::TEvRemoteHttpInfo::TPtr ev)
            : Sender(ev->Sender)
            , Cookie(ev->Cookie)
            , Query(ev->Get()->Query)
            , Params(ev->Get()->Cgi())
            , ActivityActorId(self->ActivityActorId)
        {
            Trash = self->State.GetTrashTotalBytes();
        }

        void Bootstrap() {
            TCgiParameters params(Params);
            params.InsertUnescaped("trashvol", ToString(Trash));

            RepliesPending += Send(ActivityActorId, new NMon::TEvRemoteHttpInfo('?' + params()), 0, ECookie::LOAD_ACTOR);
            if (!RepliesPending) {
                PassAway();
            }
            Become(&TThis::StateFunc);
        }

        void Handle(NMon::TEvRemoteHttpInfoRes::TPtr ev) {
            switch (ev->Cookie) {
                case ECookie::LOAD_ACTOR:
                    ActivityActorHtml = ev->Get()->Html;
                    break;

                default:
                    Y_ABORT("unexpected Cookie");
            }
            if (!--RepliesPending) {
                PassAway();
            }
        }

        void Handle(NMon::TEvRemoteJsonInfoRes::TPtr ev) {
            Y_ABORT_UNLESS(ev->Cookie == ECookie::LOAD_ACTOR);
            Y_ABORT_UNLESS(RepliesPending == 1);
            --RepliesPending;
            Send(Sender, ev->Release().Release(), 0, Cookie);
            Sender = {};
            PassAway();
        }

        void PassAway() override {
            if (Sender) {
                RenderHtml();
            }
            TActorBootstrapped::PassAway();
        }

        void RenderHtml() {
            TStringStream str;

            HTML(str) {
                DIV() {
                    str << "<a href='/tablets" << Query << "&page=keyvalue'>KeyValue state</a>";
                }
                DIV() {
                    str << ActivityActorHtml;
                }
            }

            Send(Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()), 0, Cookie);
        }

        STRICT_STFUNC(StateFunc,
            hFunc(NMon::TEvRemoteHttpInfoRes, Handle);
            hFunc(NMon::TEvRemoteJsonInfoRes, Handle);
        )
    };

    IActor *TTestShard::CreateMonQueryActor(NMon::TEvRemoteHttpInfo::TPtr ev) {
        return new TMonQueryActor(this, ev);
    }

} // NKikimr::NTestShard
