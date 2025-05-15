#include "query_stattablet.h"
#include "query_statalgo.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

using namespace NKikimrServices;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TTabletStatActor
    // Output info about given TabletId
    ////////////////////////////////////////////////////////////////////////////
    class TTabletStatActor : public TActorBootstrapped<TTabletStatActor> {
        friend class TActorBootstrapped<TThis>;

        void Bootstrap(const TActorContext &ctx) {
            TStringStream str;
            const ui64 tabletId = Ev->Get()->Record.GetTabletId();
            const bool prettyPrint = Ev->Get()->Record.GetPrettyPrint();

            ProcessBarriers(str, tabletId, prettyPrint);
            ProcessBlocks(str, tabletId, prettyPrint);
            ProcessLogoBlobs(str, tabletId, prettyPrint);

            Result->SetResult(str.Str());
            SendVDiskResponse(ctx, Ev->Sender, Result.release(), 0, HullCtx->VCtx, {});
            ctx.Send(ParentId, new TEvents::TEvActorDied);
            TThis::Die(ctx);
        }

        void ProcessBarriers(IOutputStream &str, ui64 tabletId, bool pretty);
        void ProcessBlocks(IOutputStream &str, ui64 tabletId, bool pretty);
        void ProcessLogoBlobs(IOutputStream &str, ui64 tabletId, bool pretty);

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_LEVEL_INDEX_STAT_QUERY;
        }

        TTabletStatActor(TIntrusivePtr<THullCtx> hullCtx,
                         const TActorId &parentId,
                         THullDsSnap &&fullSnap,
                         TEvBlobStorage::TEvVDbStat::TPtr &ev,
                         std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result)
            : TActorBootstrapped<TTabletStatActor>()
            , HullCtx(std::move(hullCtx))
            , ParentId(parentId)
            , FullSnap(std::move(fullSnap))
            , Ev(ev)
            , Result(std::move(result))
        {}

    private:
        // class aggregates info about LogoBlobs
        class TAggr;

        TIntrusivePtr<THullCtx> HullCtx;
        const TActorId ParentId;
        THullDsSnap FullSnap;
        TEvBlobStorage::TEvVDbStat::TPtr Ev;
        std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> Result;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TTabletStatActor::TAggr
    ////////////////////////////////////////////////////////////////////////////
    class TTabletStatActor::TAggr {
    public:
        using TLevelSegment = ::NKikimr::TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
        using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;

    private:
        using TChannelLevel = std::tuple<ui32, TString>; // channel + level_name
        struct TValue {
            // total records we have seen
            ui64 Records = 0;
            // number of records that have data
            ui64 RecordsWithData = 0;

            // keep/don't keep flags
            ui64 RecordsWithDoNotKeepFlags = 0;
            ui64 RecordsWithKeepFlags = 0;
            ui64 RecordsWithDefFlags = 0;

            // data size
            ui64 DataSizeMem = 0;
            ui64 DataSizeInPlace = 0;
            ui64 DataSizeHuge = 0;

            TValue &operator += (const TValue &v) {
                Records += v.Records;
                RecordsWithData += v.RecordsWithData;
                RecordsWithDoNotKeepFlags += v.RecordsWithDoNotKeepFlags;
                RecordsWithKeepFlags += v.RecordsWithKeepFlags;
                RecordsWithDefFlags += v.RecordsWithDefFlags;
                DataSizeMem += v.DataSizeMem;
                DataSizeInPlace += v.DataSizeInPlace;
                DataSizeHuge += v.DataSizeHuge;
                return *this;
            }
        };
        using TMapType = TMap<TChannelLevel, TValue>;

        IOutputStream &Str;
        ui64 TabletId;
        bool Pretty;
        TMapType Map;
        const TIngress::EMode IngressMode;

    public:
        TAggr(IOutputStream &str, ui64 tabletId, bool pretty, TIngress::EMode ingressMode)
            : Str(str)
            , TabletId(tabletId)
            , Pretty(pretty)
            , IngressMode(ingressMode)
        {}

        void UpdateFresh(const char *segName,
                         const TKeyLogoBlob &key,
                         const TMemRecLogoBlob &memRec) {
            Update(segName, key, memRec);
        }

        void UpdateLevel(const TLevelSstPtr &sstPtr,
                         const TKeyLogoBlob &key,
                         const TMemRecLogoBlob &memRec) {
            const TString levelName = Sprintf("Level %4u", unsigned(sstPtr.Level));
            Update(levelName, key, memRec);
        }

        void Update(const TString &levelName,
                    const TKeyLogoBlob &key,
                    const TMemRecLogoBlob &memRec) {
            const TLogoBlobID id = key.LogoBlobID();
            if (id.TabletID() == TabletId) {
                TChannelLevel key(id.Channel(), levelName);
                TValue &val = Map[key];
                val.Records++;
                if (memRec.DataSize() > 0) {
                    val.RecordsWithData++;
                }

                // have some non default flags
                int flags = memRec.GetIngress().GetCollectMode(IngressMode);
                if (flags & ECollectMode::CollectModeDoNotKeep) {
                    val.RecordsWithDoNotKeepFlags++;
                } else if (flags & ECollectMode::CollectModeKeep) {
                    val.RecordsWithKeepFlags++;
                } else {
                    val.RecordsWithDefFlags++;
                }

                // data size
                switch (memRec.GetType()) {
                    case TBlobType::DiskBlob:
                        val.DataSizeInPlace += memRec.DataSize();
                        break;
                    case TBlobType::HugeBlob:
                    case TBlobType::ManyHugeBlobs:
                        val.DataSizeHuge += memRec.DataSize();
                        break;
                    case TBlobType::MemBlob:
                        val.DataSizeMem += memRec.DataSize();
                        break;
                    default:
                        Y_ABORT("Unexpected case");
                }
            }
        }

        void RenderRow(const TString &level, const TValue &val) const {
            Y_UNUSED(val);

            auto outputNum = [this] (ui64 val, ESizeFormat format) {
                Str << "<td data-text='" << val << "'><small>";
                if (Pretty) {
                    Str << HumanReadableSize(val, format);
                } else {
                    Str << val;
                }
                Str << "</small></td>";
            };

            HTML(Str) {
                TABLER() {
                    TABLED() {Str << level;}
                    outputNum(val.Records, SF_QUANTITY);
                    outputNum(val.RecordsWithData, SF_QUANTITY);
                    outputNum(val.RecordsWithDoNotKeepFlags, SF_QUANTITY);
                    outputNum(val.RecordsWithKeepFlags, SF_QUANTITY);
                    outputNum(val.RecordsWithDefFlags, SF_QUANTITY);
                    outputNum(val.DataSizeMem, SF_BYTES);
                    outputNum(val.DataSizeInPlace, SF_BYTES);
                    outputNum(val.DataSizeHuge, SF_BYTES);
                }
            }
        }

        void RenderTableForChannel(TMapType::const_iterator begin, TMapType::const_iterator end) const {
            // render output
            HTML(Str) {
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        ui32 channel = std::get<0>(begin->first);
                        Str << "LogoBlobs for TabletId=" << TabletId
                            << " Channel=" << channel;
                        // dump db button
                        Str << "<a class=\"btn btn-primary btn-xs navbar-right\""
                            << " href=\"?type=dump&dbname=LogoBlobs&tabletid=" << TabletId
                            << "&channel=" << channel << "\">Dump Records</a>";
                    }
                    DIV_CLASS("panel-body") {
                        TABLE_SORTABLE_CLASS ("table table-condensed") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() {Str << "Level";}
                                    TABLEH() {Str << "Records";}
                                    TABLEH() {Str << "Records With Data";}
                                    TABLEH() {Str << "Records With 'DoNotKeep' Flags";}
                                    TABLEH() {Str << "Records With 'Keep' Flags";}
                                    TABLEH() {Str << "Records With 'Def' Flags";}
                                    TABLEH() {Str << "Data Size Mem";}
                                    TABLEH() {Str << "Data Size In Place";}
                                    TABLEH() {Str << "Data Size Huge";}
                                }
                            }
                            TABLEBODY() {
                                TValue total;
                                for (TMapType::const_iterator it = begin; it != end; ++it) {
                                    const TString levelName = std::get<1>(it->first);
                                    total += it->second;
                                    RenderRow(levelName, it->second);
                                }
                                RenderRow("Total", total);
                            }
                        }
                    }
                }
            }
        }

        void Finish() {
            TMapType::const_iterator end = Map.end();
            TMapType::const_iterator first = end;
            for (TMapType::const_iterator it = Map.begin(); it != end; ++it) {
                if (first == end) {
                    first = it;
                } else {
                    ui32 firstChannel = std::get<0>(first->first);
                    ui32 curChannel = std::get<0>(it->first);
                    if (firstChannel != curChannel) {
                        // process segment
                        RenderTableForChannel(first, it);
                        first = it;
                    }
                }
            }
            if (first != end) {
                // process segment
                RenderTableForChannel(first, end);
            }
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TTabletStatActor::ProcessBarriers
    ////////////////////////////////////////////////////////////////////////////
    void TTabletStatActor::ProcessBarriers(IOutputStream &str, ui64 tabletId, bool pretty) {
        Y_UNUSED(pretty);

        // build barriers
        auto brs = FullSnap.BarriersSnap.CreateEssence(HullCtx, tabletId, tabletId, 0);
        // barrier printer
        auto outputBarrier = [&str] (const TMaybe<NBarriers::TCurrentBarrier> &b) {
            if (b)
                b->Output(str);
            else
                str << "empty";
        };

        // render output
        HTML(str) {
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Barriers for TabletId=" << tabletId;
                }
                DIV_CLASS("panel-body") {
                    TABLE_SORTABLE_CLASS ("table table-condensed") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {str << "Channel";}
                                TABLEH() {str << "Soft Barrier";}
                                TABLEH() {str << "Hard Barrier";}
                            }
                        }
                        TABLEBODY() {
                            const ui32 MaxChannel = 255;
                            TMaybe<NBarriers::TCurrentBarrier> soft;
                            TMaybe<NBarriers::TCurrentBarrier> hard;
                            for (ui32 channel = 0; channel < MaxChannel; ++channel) {
                                brs->FindBarrier(tabletId, channel, soft, hard);
                                if (soft || hard) {
                                    TABLER() {
                                        TABLED() {str << channel;}
                                        TABLED() {outputBarrier(soft);}
                                        TABLED() {outputBarrier(hard);}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // TTabletStatActor::ProcessBlocks
    ////////////////////////////////////////////////////////////////////////////
    void TTabletStatActor::ProcessBlocks(IOutputStream &str, ui64 tabletId, bool pretty) {
        Y_UNUSED(pretty);
        using TIndexForwardIterator = TBlocksSnapshot::TIndexForwardIterator;

        // find out blocked generation for this tablet
        TIndexForwardIterator it(HullCtx, &FullSnap.BlocksSnap);
        it.Seek(tabletId);
        TMaybe<ui32> blockedGen;
        if (it.Valid()) {
            auto key = it.GetCurKey();
            if (key.TabletId == tabletId) {
                blockedGen = it.GetMemRec().BlockedGeneration;
            }
        }

        // render output
        HTML(str) {
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Blocks for TabletId=" << tabletId;
                }
                DIV_CLASS("panel-body") {
                    if (blockedGen) {
                        str << "BlockedGeneration=" << *blockedGen;
                    } else {
                        str << "No block for this tablet at this VDisk database";
                    }
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // TTabletStatActor::ProcessLogoBlobs
    ////////////////////////////////////////////////////////////////////////////
    void TTabletStatActor::ProcessLogoBlobs(IOutputStream &str, ui64 tabletId, bool pretty) {
        TAggr aggr(str, tabletId, pretty, TIngress::IngressMode(HullCtx->VCtx->Top->GType));
        TraverseDbWithoutMerge(HullCtx, &aggr, FullSnap.LogoBlobsSnap);
    }

    ////////////////////////////////////////////////////////////////////////////
    // CreateTabletStatActor
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateTabletStatActor(TIntrusivePtr<THullCtx> hullCtx,
                                  const TActorId &parentId,
                                  THullDsSnap &&fullSnap,
                                  TEvBlobStorage::TEvVDbStat::TPtr &ev,
                                  std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result) {
        return new TTabletStatActor(std::move(hullCtx), parentId, std::move(fullSnap), ev, std::move(result));
    }

} // NKikimr
