#include "query_statdb.h"
#include "query_statalgo.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/util/format.h>

using namespace NKikimrServices;

namespace NKikimr {

    namespace {

        const char *ByteSuffix[] = {"B", "KiB", "MiB", "GiB", nullptr};
        const char *ItemSuffix[] = {"", "K", "M", "G", nullptr};

        ///////////////////////////////////////////////////////////////////////////////
        // TChannelInfo
        ///////////////////////////////////////////////////////////////////////////////
        class TChannelInfo {
        private:
            ui64 Num;
            ui64 DataSize;
            TLogoBlobID MinId;
            TLogoBlobID MaxId;

        public:
            TChannelInfo()
                : Num(0)
                , DataSize(0)
                , MinId(TLogoBlobID(ui64(-1), ui32(-1), ui32(-1), ui8(-1), 0, 0,
                                    TLogoBlobID::MaxPartId))
                , MaxId()
            {}

            bool Empty() const {
                return Num == 0;
            }

            void Update(const TLogoBlobID &id, const TMemRecLogoBlob &m) {
                ++Num;
                DataSize += m.DataSize();
                if (id < MinId)
                    MinId = id;
                if (id > MaxId)
                    MaxId = id;
            }

            void Finish(IOutputStream &str, bool pretty) {

                HTML(str) {
                    if (pretty) {
                        TABLED_ATTRS({{"data-text", Sprintf("%" PRIu64, Num)}, {"align", "right"}}) { SMALL() {
                            FormatHumanReadable(str, Num, 1000, 2, ItemSuffix);
                        }}
                        TABLED_ATTRS({{"data-text", Sprintf("%" PRIu64, DataSize)}, {"align", "right"}}) { SMALL() {
                            FormatHumanReadable(str, DataSize, 1024, 2, ByteSuffix);
                        }}
                    } else {
                        TABLED() {SMALL() {str << Num;}}
                        TABLED() {SMALL() {str << DataSize;}}
                    }
                    TABLED() {SMALL() {str << MinId.ToString();}}
                    TABLED() {SMALL() {str << MaxId.ToString();}}
                }
            }

            void Finish(NKikimrVDisk::ChannelInfo *channelInfo) {
                channelInfo->set_count(Num);
                channelInfo->set_data_size(DataSize);
                channelInfo->set_min_id(MinId.ToString());
                channelInfo->set_max_id(MaxId.ToString());
            }
        };

        ///////////////////////////////////////////////////////////////////////////////
        // TAllChannels
        ///////////////////////////////////////////////////////////////////////////////
        class TAllChannels {
        public:
            TAllChannels()
                : Channels()
            {}

            void Update(const TLogoBlobID &id, const TMemRecLogoBlob &m) {
                ui8 c = id.Channel();
                if (c >= Channels.size())
                    Channels.resize(c + 1);
                Channels[c].Update(id, m);
            }

            void Finish(IOutputStream &str, ui64 tabletID, bool pretty) {
                auto tabletIDOutputer = [tabletID] (IOutputStream &str) {
                    HTML(str) {
                        TABLED() {
                            SMALL() {
                                // tabletId and hyperlink to per tablet stat
                                str << "<a href=\"?type=tabletstat&tabletid=" << tabletID
                                    << "\">" << tabletID << "</a>";
                            }
                        }
                    }
                };
                Finish(str, tabletIDOutputer, pretty);
            }

            void Finish(IOutputStream &str, bool pretty) {
                auto nothing = [] (IOutputStream &) {};
                Finish(str, nothing, pretty);
            }

            void Finish(::google::protobuf::RepeatedPtrField<NKikimrVDisk::ChannelInfo> *channelsOutput) {
                for (auto &c : Channels) {
                    c.Finish(channelsOutput->Add());
                }
            }

        private:
            TVector<TChannelInfo> Channels;

            void Finish(IOutputStream &str,
                        std::function<void (IOutputStream &)> t,
                        bool pretty)
            {
                HTML(str) {
                    for (auto &c : Channels) {
                        if (!c.Empty()) {
                            TABLER() {
                                t(str);
                                TABLED() {SMALL() {str << (&c - &Channels.front());}}
                                c.Finish(str, pretty);
                            }
                        }
                    }
                }
            }
        };


        ///////////////////////////////////////////////////////////////////////////////
        // TTabletInfo
        ///////////////////////////////////////////////////////////////////////////////
        class TTabletInfo : public TThrRefBase {
        public:
            TTabletInfo(ui64 tabletID)
                : TabletID(tabletID)
                , AllChannels()
            {}

            void Update(const TLogoBlobID &id, const TMemRecLogoBlob &m) {
                AllChannels.Update(id, m);
            }

            void Finish(IOutputStream &str, bool pretty) {
                AllChannels.Finish(str, TabletID, pretty);
            }

            void Finish(NKikimrVDisk::TabletInfo *result) {
                AllChannels.Finish(result->mutable_channels());
                result->set_tablet_id(TabletID);
            }

        private:
            ui64 TabletID;
            TAllChannels AllChannels;
        };

        using TTabletInfoPtr = TIntrusivePtr<TTabletInfo>;

        ///////////////////////////////////////////////////////////s////////////////////
        // TAllTablets
        ///////////////////////////////////////////////////////////////////////////////
        class TAllTablets {
        private:
            using THash = THashMap<ui64, TTabletInfoPtr>; // tabletID -> TTabletInfoPtr
            THash Hash;
            TAllChannels AllChannels;

        public:
            void Update(const TLogoBlobID &id, const TMemRecLogoBlob &m) {
                ui64 tabletID = id.TabletID();

                auto it = Hash.find(tabletID);
                if (it == Hash.end()) {
                    it = Hash.insert(THash::value_type(tabletID,
                                                       new TTabletInfo(tabletID))).first;
                }

                it->second->Update(id, m);
                AllChannels.Update(id, m);
            }

            void Finish(IOutputStream &str, bool pretty) {
                HTML(str) {
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            str << "Per (Tablet, Channel) LogoBlobs DB Statistics "
                                << "(raw data w/o garbage collection)";
                        }
                        DIV_CLASS("panel-body") {
                            TABLE_SORTABLE_CLASS ("table table-condensed") {
                                TABLEHEAD() {
                                    TABLER() {
                                        TABLEH() {str << "TabletID";}
                                        TABLEH() {str << "Channel";}
                                        TABLEH() {str << "Blobs";}
                                        TABLEH() {str << "DataSize";}
                                        TABLEH() {str << "MinId";}
                                        TABLEH() {str << "MaxId";}
                                    }
                                }
                                TABLEBODY() {
                                    for (const auto &x : Hash)
                                        x.second->Finish(str, pretty);
                                }
                            }
                        }
                    }
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            str << "Per Channel (all tablets) LogoBlobs DB Statistics";
                        }
                        DIV_CLASS("panel-body") {
                            TABLE_SORTABLE_CLASS ("table table-condensed") {
                                TABLEHEAD() {
                                    TABLER() {
                                        TABLEH() {str << "Channel";}
                                        TABLEH() {str << "Blobs";}
                                        TABLEH() {str << "DataSize";}
                                        TABLEH() {str << "MinId";}
                                        TABLEH() {str << "MaxId";}
                                    }
                                }
                                TABLEBODY() {
                                    AllChannels.Finish(str, pretty);
                                }
                            }
                        }
                    }
                }
            }

            void Finish(::google::protobuf::RepeatedPtrField<NKikimrVDisk::TabletInfo> *tabletsOutput,
                    ::google::protobuf::RepeatedPtrField<NKikimrVDisk::ChannelInfo> *channelsOutput)
            {
                for (const auto &x : Hash) {
                    x.second->Finish(tabletsOutput->Add());
                }
                AllChannels.Finish(channelsOutput);
            }
        };
    }

    template <>
    void TLevelIndexStatActor<TKeyLogoBlob, TMemRecLogoBlob>::CalculateStat(IOutputStream &str,
                                                                            bool pretty) {
        // aggregation class
        struct TAggr {
            using TLevelSegment = ::NKikimr::TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
            using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;

            TAggr(IOutputStream &str, bool pretty)
                : Str(str)
                , Pretty(pretty)
            {}

            void UpdateFresh(const char *segName,
                             const TKeyLogoBlob &key,
                             const TMemRecLogoBlob &memRec) {
                Y_UNUSED(segName);
                Tablets.Update(key.LogoBlobID(), memRec);
            }

            void UpdateLevel(const TLevelSstPtr &sstPtr,
                             const TKeyLogoBlob &key,
                             const TMemRecLogoBlob &memRec) {
                Y_UNUSED(sstPtr);
                Tablets.Update(key.LogoBlobID(), memRec);
            }

            void Finish() {
                Tablets.Finish(Str, Pretty);
            }

            TAllTablets Tablets;
            IOutputStream &Str;
            bool Pretty;
        };

        // run aggregation
        TAggr aggr(str, pretty);
        TraverseDbWithoutMerge(HullCtx, &aggr, Snapshot);
    }

    template <>
    void TLevelIndexStatActor<TKeyLogoBlob, TMemRecLogoBlob,
            TEvGetLogoBlobIndexStatRequest, TEvGetLogoBlobIndexStatResponse
    >::CalculateStat(std::unique_ptr<TEvGetLogoBlobIndexStatResponse> &result) {
        // aggregation class
        struct TAggr {
            using TLevelSegment = ::NKikimr::TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
            using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;

            TAggr(std::unique_ptr<TEvGetLogoBlobIndexStatResponse> &result)
                : Result(result)
            {}

            void UpdateFresh(const char *segName,
                             const TKeyLogoBlob &key,
                             const TMemRecLogoBlob &memRec) {
                Y_UNUSED(segName);
                Tablets.Update(key.LogoBlobID(), memRec);
            }

            void UpdateLevel(const TLevelSstPtr &sstPtr,
                             const TKeyLogoBlob &key,
                             const TMemRecLogoBlob &memRec) {
                Y_UNUSED(sstPtr);
                Tablets.Update(key.LogoBlobID(), memRec);
            }

            void Finish() {
                auto stat = Result->Record.mutable_stat();
                Tablets.Finish(stat->mutable_tablets(), stat->mutable_channels());
            }

            TAllTablets Tablets;
            std::unique_ptr<TEvGetLogoBlobIndexStatResponse> &Result;
        };

        // run aggregation
        TAggr aggr(result);
        TraverseDbWithoutMerge(HullCtx, &aggr, Snapshot);
    }

    template <>
    void TLevelIndexStatActor<TKeyBlock, TMemRecBlock>::CalculateStat(IOutputStream &str,
                                                                      bool pretty) {
        // aggregation class
        struct TAggr {
            using TLevelSegment = ::NKikimr::TLevelSegment<TKeyBlock, TMemRecBlock>;
            using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;

            TAggr(IOutputStream &str, bool pretty)
                : Str(str)
                , Pretty(pretty)
            {}

            void Update(const TKeyBlock &key,
                        const TMemRecBlock &memRec) {
                TValue &v = Map[key.TabletId];
                v.Number++;
                v.BlockedGeneration = Max(v.BlockedGeneration, memRec.BlockedGeneration);
            }

            void UpdateFresh(const char *segName,
                             const TKeyBlock &key,
                             const TMemRecBlock &memRec) {
                Y_UNUSED(segName);
                Update(key, memRec);
            }

            void UpdateLevel(const TLevelSstPtr &sstPtr,
                             const TKeyBlock &key,
                             const TMemRecBlock &memRec) {
                Y_UNUSED(sstPtr);
                Update(key, memRec);
            }

            void Finish() {
                // render output
                HTML(Str) {
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            Str << "Per Tablet Blocks Statistics";
                        }
                        DIV_CLASS("panel-body") {
                            TABLE_SORTABLE_CLASS ("table table-condensed") {
                                TABLEHEAD() {
                                    TABLER() {
                                        TABLEH() {Str << "TabletId";}
                                        TABLEH() {Str << "Records";}
                                        TABLEH() {Str << "BlockedGeneration";}
                                    }
                                }
                                TABLEBODY() {
                                    for (const auto &x : Map) {
                                        TABLER() {
                                            const ui64 tabletId = x.first;
                                            const TValue &val = x.second;
                                            TABLED() {Str << tabletId;}
                                            TABLED() {Str << val.Number;}
                                            TABLED() {Str << val.BlockedGeneration;}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            //BlockedGeneration
            struct TValue {
                ui64 Number = 0;
                ui32 BlockedGeneration = 0;
            };

            using TMapType = TMap<ui64, TValue>; // TabletId -> TValue

            TMapType Map;
            IOutputStream &Str;
            bool Pretty;
        };


        // run aggregation
        TAggr aggr(str, pretty);
        TraverseDbWithoutMerge(HullCtx, &aggr, Snapshot);
    }

    template <>
    void TLevelIndexStatActor<TKeyBarrier, TMemRecBarrier>::CalculateStat(IOutputStream &str,
                                                                          bool pretty) {
        // aggregation class
        struct TAggr {
            using TLevelSegment = ::NKikimr::TLevelSegment<TKeyBarrier, TMemRecBarrier>;
            using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;

            TAggr(IOutputStream &str, bool pretty)
                : Str(str)
                , Pretty(pretty)
            {}

            void Update(const TKeyBarrier &key,
                        const TMemRecBarrier &memRec) {
                auto mapKey = TKey(key.TabletId, key.Channel, bool(key.Hard));
                TValue &v = Map[mapKey];
                v.Number++;
                std::tuple<ui32, ui32> newVal(memRec.CollectGen, memRec.CollectStep);
                std::tuple<ui32, ui32> curVal(v.CollectGen, v.CollectStep);
                if (newVal > curVal) {
                    v.CollectGen = memRec.CollectGen;
                    v.CollectStep = memRec.CollectStep;
                }
            }

            void UpdateFresh(const char *segName,
                             const TKeyBarrier &key,
                             const TMemRecBarrier &memRec) {
                Y_UNUSED(segName);
                Update(key, memRec);
            }


            void UpdateLevel(const TLevelSstPtr &sstPtr,
                             const TKeyBarrier &key,
                             const TMemRecBarrier &memRec) {
                Y_UNUSED(sstPtr);
                Update(key, memRec);
            }

            void Finish() {
                // render output
                HTML(Str) {
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            Str << "Per Tablet Blocks Statistics";
                        }
                        DIV_CLASS("panel-body") {
                            TABLE_SORTABLE_CLASS ("table table-condensed") {
                                TABLEHEAD() {
                                    TABLER() {
                                        TABLEH() {Str << "TabletId";}
                                        TABLEH() {Str << "Channel";}
                                        TABLEH() {Str << "Hard";}
                                        TABLEH() {Str << "Records";}
                                        TABLEH() {Str << "Last Seen Value (w/o quorum)";}
                                    }
                                }
                                TABLEBODY() {
                                    for (const auto &x : Map) {
                                        TABLER() {
                                            const TKey &mapKey = x.first;
                                            const TValue &val = x.second;
                                            TABLED() {Str << std::get<0>(mapKey);}
                                            TABLED() {Str << std::get<1>(mapKey);}
                                            TABLED() {Str << int(std::get<2>(mapKey));}
                                            TABLED() {Str << val.Number;}
                                            TABLED() {Str << val.CollectGen << ":"
                                                          << val.CollectStep;}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            //BlockedGeneration
            struct TValue {
                ui64 Number = 0;
                ui32 CollectGen = 0;
                ui32 CollectStep = 0;
            };

            using TBarrierKind = bool; // hard=true or soft=false
            using TKey = std::tuple<ui64, ui32, TBarrierKind>;
            using TMapType = TMap<TKey, TValue>; // TKey -> TValue

            TMapType Map;
            IOutputStream &Str;
            bool Pretty;
        };


        // run aggregation
        TAggr aggr(str, pretty);
        TraverseDbWithoutMerge(HullCtx, &aggr, Snapshot);
    }

} // NKikimr
