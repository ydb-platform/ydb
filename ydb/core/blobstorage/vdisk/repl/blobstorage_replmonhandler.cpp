#include "blobstorage_repl.h"

namespace NKikimr {

    class TReplMonRequestHandlerActor : public TActorBootstrapped<TReplMonRequestHandlerActor> {
        const TActorId SkeletonId;
        const TVDiskIdShort VDiskId;
        const std::shared_ptr<TBlobStorageGroupInfo::TTopology> Topology;
        NMon::TEvHttpInfo::TPtr Ev;
        TActorId ParentId;

    public:
        TReplMonRequestHandlerActor(TActorId skeletonId, TVDiskIdShort vdiskId,
                std::shared_ptr<TBlobStorageGroupInfo::TTopology> topology, NMon::TEvHttpInfo::TPtr ev)
            : SkeletonId(skeletonId)
            , VDiskId(vdiskId)
            , Topology(std::move(topology))
            , Ev(ev)
        {}

        void Bootstrap(TActorId parentId) {
            ParentId = parentId;
            Send(SkeletonId, new TEvReplInvoke(std::bind(&TThis::ProcessBlobs, std::placeholders::_1, std::placeholders::_2,
                Ev->Sender, Ev->Cookie, Ev->Get()->SubRequestId, SelfId(), Ev->Get()->Request.GetParams(), VDiskId,
                Topology)));
            Become(&TThis::StateFunc);
        }

        static void ProcessBlobs(const TUnreplicatedBlobRecords& ubr, TString error, TActorId sender, ui64 cookie,
                int subRequestId, TActorIdentity selfId, const TCgiParameters& cgi, TVDiskIdShort vdiskId,
                std::shared_ptr<TBlobStorageGroupInfo::TTopology> topology) {
            // NOTE: this function is called from other actor context!
            if (error) {
                TStringStream str;
                str << "Can't obtain unreplicated blobs: <strong>" << error << "</strong>";
                Reply(sender, cookie, subRequestId, selfId, str.Str());
            } else {
                ui32 maxRows;
                if (!TryFromString(cgi.Get("maxRows"), maxRows)) {
                    maxRows = 1000;
                }

                TStringStream str;
                str << NMonitoring::HTTPOKTEXT;
                const auto& type = topology->GType;
                const ui32 subgroupSize = type.BlobSubgroupSize();
                const ui32 totalPartCount = type.TotalPartCount();
                for (const auto& [id, item] : ubr) {
                    if (!maxRows--) {
                        str << "--- truncated ---" << Endl;
                        break;
                    }

                    str << id << ' ' << item.Ingress.ToString(topology.get(), vdiskId, id);
                    str << " Parts# [";
                    for (ui32 i = 0; i < totalPartCount; ++i) {
                        str << (item.PartsMask >> totalPartCount - i - 1 & 1);
                    }
                    str << "] RepliesInSubgroup# [";
                    for (ui32 i = 0; i < subgroupSize; ++i) {
                        const ui32 mask = 1 << i;
                        if (i) {
                            str << ' ';
                        }
                        str << i << ':';
                        str << (item.DisksRepliedOK & mask ? "OK" :
                                item.DisksRepliedNODATA & mask ? "NODATA" :
                                item.DisksRepliedNOT_YET & mask ? "NOT_YET" :
                                item.DisksRepliedOther & mask ? "other" : "-");
                    }
                    str << ']';
                    if (item.LooksLikePhantom) {
                        str << " (ph)";
                    }
                    str << Endl;
                }
                Reply(sender, cookie, subRequestId, selfId, str.Str(), NMon::TEvHttpInfoRes::Custom);
            }
        }

        static void Reply(TActorId sender, ui64 cookie, int subRequestId, TActorIdentity selfId, const TString& answer,
                NMon::TEvHttpInfoRes::EContentType contentType = NMon::TEvHttpInfoRes::Html) {
            selfId.Send(sender, new NMon::TEvHttpInfoRes(answer, subRequestId, contentType), 0, cookie);
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, selfId, {}, nullptr, 0));
        }

        void PassAway() override {
            Send(ParentId, new TEvents::TEvGone);
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(StateFunc,
            cFunc(TEvents::TSystem::Poison, PassAway);
        )
    };

    IActor *CreateReplMonRequestHandler(TActorId skeletonId, TVDiskIdShort vdiskId,
            std::shared_ptr<TBlobStorageGroupInfo::TTopology> topology, NMon::TEvHttpInfo::TPtr ev) {
        return new TReplMonRequestHandlerActor(skeletonId, vdiskId, std::move(topology), ev);
    }

} // NKikimr
