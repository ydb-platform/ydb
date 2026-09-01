#include "query_statdb.h"
#include "query_statalgo.h"
#include "query_statdb_stream.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap_events.h>
#include <ydb/core/util/format.h>

#include <algorithm>
#include <concepts>

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

    template <class TKey, class TMemRec>
    concept CStatDb =
        (std::same_as<TKey, TKeyLogoBlob> && std::same_as<TMemRec, TMemRecLogoBlob>) ||
        (std::same_as<TKey, TKeyBlock> && std::same_as<TMemRec, TMemRecBlock>) ||
        (std::same_as<TKey, TKeyBarrier> && std::same_as<TMemRec, TMemRecBarrier>);

    template <class TKey, class TMemRec>
    void EmplaceSnapshot(
            std::optional<TLevelIndexSnapshot<TKey, TMemRec>>& snapshot,
            THullDsSnap&& fullSnapshot);

    template <>
    void EmplaceSnapshot<TKeyLogoBlob, TMemRecLogoBlob>(
            std::optional<TLevelIndexSnapshot<TKeyLogoBlob, TMemRecLogoBlob>>& snapshot,
            THullDsSnap&& fullSnapshot)
    {
        snapshot.emplace(std::move(fullSnapshot.LogoBlobsSnap));
    }

    template <>
    void EmplaceSnapshot<TKeyBlock, TMemRecBlock>(
            std::optional<TLevelIndexSnapshot<TKeyBlock, TMemRecBlock>>& snapshot,
            THullDsSnap&& fullSnapshot)
    {
        snapshot.emplace(std::move(fullSnapshot.BlocksSnap));
    }

    template <>
    void EmplaceSnapshot<TKeyBarrier, TMemRecBarrier>(
            std::optional<TLevelIndexSnapshot<TKeyBarrier, TMemRecBarrier>>& snapshot,
            THullDsSnap&& fullSnapshot)
    {
        snapshot.emplace(std::move(fullSnapshot.BarriersSnap));
    }

    template <
        class TKey,
        class TMemRec,
        class TRequest = TEvBlobStorage::TEvVDbStat,
        class TResponse = TEvBlobStorage::TEvVDbStatResult>
        requires CStatDb<TKey, TMemRec>
    class TLevelIndexStatActor
        : public TActorBootstrapped<TLevelIndexStatActor<TKey, TMemRec, TRequest, TResponse>>
    {
        using TThis = TLevelIndexStatActor<TKey, TMemRec, TRequest, TResponse>;
        using TBase = TActorBootstrapped<TThis>;
        using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
        using TYieldedState = TDbStatYieldedState<TKey, TMemRec>;
        using TTraversal = std::function<std::optional<TYieldedState>(
            const TLevelIndexSnapshot&, std::optional<TYieldedState>)>;

        friend class TActorBootstrapped<TThis>;

        void Bootstrap() {
            if constexpr (std::same_as<TRequest, TEvBlobStorage::TEvVDbStat>) {
                PrepareStat(Output, Ev->Get()->Record.GetPrettyPrint());
            } else {
                PrepareStat(Result);
            }
            TThis::Become(&TThis::StateFunc);
            ContinueTraversal();
        }

        void PrepareStat(IOutputStream& str, bool pretty);

        void PrepareStat(std::unique_ptr<TResponse>& result);

        template <class TAggr>
        void SetAggregator(std::shared_ptr<TAggr> aggr) {
            Traversal = [this, aggr = std::move(aggr)](
                    const TLevelIndexSnapshot& snapshot,
                    std::optional<TYieldedState> yieldedState) {
                return TraverseDbWithoutMerge(
                    HullCtx,
                    aggr.get(),
                    snapshot,
                    std::move(yieldedState),
                    YieldPolicy);
            };
        }

        void ContinueTraversal() {
            Y_ABORT_UNLESS(Snapshot);
            YieldedState = Traversal(*Snapshot, std::move(YieldedState));
            ReleaseSnapshot();

            if (YieldedState) {
                TThis::Schedule(YieldPolicy.DelayBetweenQuanta, new TEvents::TEvWakeup);
            } else {
                ReplyAndDie();
            }
        }

        void ReleaseSnapshot() {
            if (Snapshot) {
                Snapshot->Destroy();
                Snapshot.reset();
            }
        }

        void HandleWakeup() {
            TThis::Send(ParentId, new TEvTakeHullSnapshot(true));
        }

        void Handle(TEvTakeHullSnapshotResult::TPtr& ev) {
            EmplaceSnapshot<TKey, TMemRec>(Snapshot, std::move(ev->Get()->Snap));
            ContinueTraversal();
        }

        void ReplyAndDie() {
            if constexpr (std::same_as<TRequest, TEvBlobStorage::TEvVDbStat>) {
                Result->SetResult(Output.Str());
            }
            SendVDiskResponse(TActivationContext::AsActorContext(), Ev->Sender, Result.release(),
                    Ev->Cookie, HullCtx->VCtx, {});
            TThis::PassAway();
        }

        void PassAway() override {
            ReleaseSnapshot();
            TThis::Send(ParentId, new TEvents::TEvGone);
            TBase::PassAway();
        }

        STRICT_STFUNC(StateFunc, {
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvTakeHullSnapshotResult, Handle);
        })

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_LEVEL_INDEX_STAT_QUERY;
        }

        TLevelIndexStatActor(
                const TIntrusivePtr<THullCtx>& hullCtx,
                const TActorId& parentId,
                TLevelIndexSnapshot&& snapshot,
                typename TRequest::TPtr& ev,
                std::unique_ptr<TResponse> result)
            : HullCtx(hullCtx)
            , ParentId(parentId)
            , Snapshot(std::in_place, std::move(snapshot))
            , Ev(ev)
            , Result(std::move(result))
        {}

    private:
        TIntrusivePtr<THullCtx> HullCtx;
        const TActorId ParentId;
        std::optional<TLevelIndexSnapshot> Snapshot;
        typename TRequest::TPtr Ev;
        std::unique_ptr<TResponse> Result;
        const TDbStatYieldPolicy YieldPolicy;
        TStringStream Output;
        TTraversal Traversal;
        std::optional<TYieldedState> YieldedState;
    };

    class TLogoBlobIndexStatStreamActor
        : public TActorBootstrapped<TLogoBlobIndexStatStreamActor>
    {
        using TThis = TLogoBlobIndexStatStreamActor;
        using TBase = TActorBootstrapped<TThis>;
        using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;
        using TYieldedState = TDbStatYieldedState<TKeyLogoBlob, TMemRecLogoBlob>;
        using TResponse = TEvGetLogoBlobIndexStatResponse;
        using TAck = TEvGetLogoBlobIndexStatResponseAck;

        enum EEv {
            EvAckTimeout = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvEnd,
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE));

        struct TEvAckTimeout : TEventLocal<TEvAckTimeout, EvAckTimeout> {};

        static constexpr ui64 DefaultBatchBytes = 1 << 20;
        static constexpr ui64 MinBatchBytes = 64 << 10;
        static constexpr TDuration AckTimeout = TDuration::Seconds(30);

        friend class TActorBootstrapped<TThis>;

        static ui64 CalculateBatchBytes(
                const NKikimrVDisk::GetLogoBlobIndexStatRequest& request,
                const TIntrusivePtr<THullCtx>& hullCtx)
        {
            const ui64 configuredMax = hullCtx->VCfg
                ? Max<ui64>(hullCtx->VCfg->MaxResponseSize, 1)
                : DefaultBatchBytes;
            const ui64 configuredMin = Min(MinBatchBytes, configuredMax);
            const ui64 requested = request.max_batch_bytes()
                ? request.max_batch_bytes()
                : Min(DefaultBatchBytes, configuredMax);
            return std::clamp(requested, configuredMin, configuredMax);
        }

        void Bootstrap() {
            TThis::Become(&TThis::StateTraverse);
            TThis::Schedule(AckTimeout, new TEvAckTimeout);
            ContinueTraversal();
        }

        void ContinueTraversal() {
            Y_ABORT_UNLESS(Snapshot);
            YieldedState = TraverseDbWithoutMergeUntil(
                HullCtx,
                &Accumulator,
                *Snapshot,
                std::move(YieldedState),
                YieldPolicy,
                [this] { return Accumulator.IsBatchReady(); });
            ReleaseSnapshot();

            if (!YieldedState) {
                ReplyAndDie();
            } else if (Accumulator.IsBatchReady()) {
                ReplyAndWaitForAck();
            } else {
                TThis::Schedule(YieldPolicy.DelayBetweenQuanta, new TEvents::TEvWakeup);
            }
        }

        void ReleaseSnapshot() {
            if (Snapshot) {
                Snapshot->Destroy();
                Snapshot.reset();
            }
        }

        void RequestSnapshot() {
            TThis::Send(ParentId, new TEvTakeHullSnapshot(true));
        }

        void HandleWakeup() {
            RequestSnapshot();
        }

        void Handle(TEvTakeHullSnapshotResult::TPtr& ev) {
            EmplaceSnapshot<TKeyLogoBlob, TMemRecLogoBlob>(Snapshot, std::move(ev->Get()->Snap));
            ContinueTraversal();
        }

        std::unique_ptr<TResponse> MakeResponse() {
            if (InitialResult) {
                return std::move(InitialResult);
            }
            return std::make_unique<TResponse>(
                NKikimrProto::OK,
                TVDiskID(),
                TActivationContext::Now(),
                nullptr,
                nullptr);
        }

        void SendBatch(bool hasMore, ui64 sequenceId) {
            auto response = MakeResponse();
            Accumulator.ExtractBatch(response->Record.mutable_stat());
            response->Record.set_has_more(hasMore);
            response->Record.set_sequence_id(sequenceId);
            SendVDiskResponse(
                TActivationContext::AsActorContext(),
                Recipient,
                response.release(),
                Cookie,
                HullCtx->VCtx,
                {});
        }

        void ReplyAndWaitForAck() {
            OutstandingSequence = ++LastSentSequence;
            AckDeadline = TActivationContext::Monotonic() + AckTimeout;
            TThis::Become(&TThis::StateWaitAck);
            SendBatch(true, OutstandingSequence);
        }

        void ReplyAndDie() {
            SendBatch(false, ++LastSentSequence);
            return TThis::PassAway();
        }

        bool ValidateControlMessage(const TAck::TPtr& ev) const {
            return ev->Sender == Recipient && ev->Cookie == Cookie;
        }

        void HandleAckWhileTraversing(TAck::TPtr& ev) {
            if (!ValidateControlMessage(ev)) {
                return;
            }

            const auto& record = ev->Get()->Record;
            if (record.has_cancel() && record.cancel()) {
                return TThis::PassAway();
            } else if (record.has_sequence_id() && record.sequence_id() > LastSentSequence) {
                return TThis::PassAway();
            }
        }

        void HandleAckWhileWaiting(TAck::TPtr& ev) {
            if (!ValidateControlMessage(ev)) {
                return;
            }

            const auto& record = ev->Get()->Record;
            if (record.has_cancel() && record.cancel()) {
                return TThis::PassAway();
            }
            if (!record.has_sequence_id()) {
                return;
            }

            const ui64 sequenceId = record.sequence_id();
            if (sequenceId < OutstandingSequence) {
                return;
            }
            if (sequenceId > OutstandingSequence) {
                return TThis::PassAway();
            }

            OutstandingSequence = 0;
            AckDeadline = TMonotonic::Max();
            TThis::Become(&TThis::StateTraverse);
            RequestSnapshot();
        }

        void HandleAckTimeout(TEvAckTimeout::TPtr& ev) {
            const TMonotonic now = TActivationContext::Monotonic();
            if (OutstandingSequence && AckDeadline <= now) {
                return TThis::PassAway();
            }

            TThis::Schedule(
                OutstandingSequence ? AckDeadline : now + AckTimeout,
                ev->Release().Release());
        }

        STRICT_STFUNC(StateTraverse, {
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvTakeHullSnapshotResult, Handle);
            hFunc(TAck, HandleAckWhileTraversing);
            hFunc(TEvAckTimeout, HandleAckTimeout);
        })

        STRICT_STFUNC(StateWaitAck, {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TAck, HandleAckWhileWaiting);
            hFunc(TEvAckTimeout, HandleAckTimeout);
        })

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_LEVEL_INDEX_STAT_STREAM_QUERY;
        }

        TLogoBlobIndexStatStreamActor(
                const TIntrusivePtr<THullCtx>& hullCtx,
                const TActorId& parentId,
                TLogoBlobsSnapshot&& snapshot,
                TEvGetLogoBlobIndexStatRequest::TPtr& ev,
                std::unique_ptr<TResponse> result)
            : HullCtx(hullCtx)
            , ParentId(parentId)
            , Snapshot(std::in_place, std::move(snapshot))
            , Recipient(ev->Sender)
            , Cookie(ev->Cookie)
            , InitialResult(std::move(result))
            , Accumulator(CalculateBatchBytes(ev->Get()->Record, hullCtx))
        {}

        void PassAway() override {
            ReleaseSnapshot();
            TThis::Send(ParentId, new TEvents::TEvGone);
            return TBase::PassAway();
        }

    private:
        TIntrusivePtr<THullCtx> HullCtx;
        const TActorId ParentId;
        std::optional<TLevelIndexSnapshot> Snapshot;
        const TActorId Recipient;
        const ui64 Cookie;
        std::unique_ptr<TResponse> InitialResult;
        TLogoBlobIndexStatStreamAccumulator Accumulator;
        const TDbStatYieldPolicy YieldPolicy;
        std::optional<TYieldedState> YieldedState;
        ui64 LastSentSequence = 0;
        ui64 OutstandingSequence = 0;
        TMonotonic AckDeadline = TMonotonic::Max();
    };

    template <>
    void TLevelIndexStatActor<TKeyLogoBlob, TMemRecLogoBlob>::PrepareStat(IOutputStream& str,
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
                Update(key, memRec);
            }

            void UpdateLevel(const TLevelSstPtr &sstPtr,
                             const TKeyLogoBlob &key,
                             const TMemRecLogoBlob &memRec) {
                Y_UNUSED(sstPtr);
                Update(key, memRec);
            }

            void Update(const TKeyLogoBlob& key, const TMemRecLogoBlob& memRec) {
                Tablets.Update(key.LogoBlobID(), memRec);
            }

            void Finish() {
                Tablets.Finish(Str, Pretty);
            }

            TAllTablets Tablets;
            IOutputStream &Str;
            bool Pretty;
        };

        SetAggregator(std::make_shared<TAggr>(str, pretty));
    }

    template <>
    void TLevelIndexStatActor<TKeyLogoBlob, TMemRecLogoBlob,
            TEvGetLogoBlobIndexStatRequest, TEvGetLogoBlobIndexStatResponse
    >::PrepareStat(std::unique_ptr<TEvGetLogoBlobIndexStatResponse>& result) {
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
                Update(key, memRec);
            }

            void UpdateLevel(const TLevelSstPtr &sstPtr,
                             const TKeyLogoBlob &key,
                             const TMemRecLogoBlob &memRec) {
                Y_UNUSED(sstPtr);
                Update(key, memRec);
            }

            void Update(const TKeyLogoBlob& key, const TMemRecLogoBlob& memRec) {
                Tablets.Update(key.LogoBlobID(), memRec);
            }

            void Finish() {
                auto stat = Result->Record.mutable_stat();
                Tablets.Finish(stat->mutable_tablets(), stat->mutable_channels());
            }

            TAllTablets Tablets;
            std::unique_ptr<TEvGetLogoBlobIndexStatResponse> &Result;
        };

        SetAggregator(std::make_shared<TAggr>(result));
    }

    template <>
    void TLevelIndexStatActor<TKeyBlock, TMemRecBlock>::PrepareStat(IOutputStream& str,
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


        SetAggregator(std::make_shared<TAggr>(str, pretty));
    }

    template <>
    void TLevelIndexStatActor<TKeyBarrier, TMemRecBarrier>::PrepareStat(IOutputStream& str,
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


        SetAggregator(std::make_shared<TAggr>(str, pretty));
    }

    template <class TKey, class TMemRec, class TRequest, class TResponse>
        requires CStatDb<TKey, TMemRec>
    IActor* CreateLevelIndexStatActorImpl(
            const TIntrusivePtr<THullCtx>& hullCtx,
            const TActorId& parentId,
            TLevelIndexSnapshot<TKey, TMemRec>&& snapshot,
            typename TRequest::TPtr& ev,
            std::unique_ptr<TResponse> result)
    {
        using TActor = TLevelIndexStatActor<TKey, TMemRec, TRequest, TResponse>;
        return new TActor(hullCtx, parentId, std::move(snapshot), ev, std::move(result));
    }

    IActor* CreateLevelIndexStatActor(
            const TIntrusivePtr<THullCtx>& hullCtx,
            const TActorId& parentId,
            TLogoBlobsSnapshot&& snapshot,
            TEvBlobStorage::TEvVDbStat::TPtr& ev,
            std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result)
    {
        return CreateLevelIndexStatActorImpl<
            TKeyLogoBlob,
            TMemRecLogoBlob,
            TEvBlobStorage::TEvVDbStat,
            TEvBlobStorage::TEvVDbStatResult>(
            hullCtx, parentId, std::move(snapshot), ev, std::move(result));
    }

    IActor* CreateLevelIndexStatActor(
            const TIntrusivePtr<THullCtx>& hullCtx,
            const TActorId& parentId,
            TBlocksSnapshot&& snapshot,
            TEvBlobStorage::TEvVDbStat::TPtr& ev,
            std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result)
    {
        return CreateLevelIndexStatActorImpl<
            TKeyBlock,
            TMemRecBlock,
            TEvBlobStorage::TEvVDbStat,
            TEvBlobStorage::TEvVDbStatResult>(
            hullCtx, parentId, std::move(snapshot), ev, std::move(result));
    }

    IActor* CreateLevelIndexStatActor(
            const TIntrusivePtr<THullCtx>& hullCtx,
            const TActorId& parentId,
            TLevelIndexSnapshot<TKeyBarrier, TMemRecBarrier>&& snapshot,
            TEvBlobStorage::TEvVDbStat::TPtr& ev,
            std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result)
    {
        return CreateLevelIndexStatActorImpl<
            TKeyBarrier,
            TMemRecBarrier,
            TEvBlobStorage::TEvVDbStat,
            TEvBlobStorage::TEvVDbStatResult>(
            hullCtx, parentId, std::move(snapshot), ev, std::move(result));
    }

    IActor* CreateLevelIndexStatActor(
            const TIntrusivePtr<THullCtx>& hullCtx,
            const TActorId& parentId,
            TLogoBlobsSnapshot&& snapshot,
            TEvGetLogoBlobIndexStatRequest::TPtr& ev,
            std::unique_ptr<TEvGetLogoBlobIndexStatResponse> result)
    {
        if (ev->Get()->Record.stream()) {
            return new TLogoBlobIndexStatStreamActor(
                hullCtx, parentId, std::move(snapshot), ev, std::move(result));
        }
        return CreateLevelIndexStatActorImpl<
            TKeyLogoBlob,
            TMemRecLogoBlob,
            TEvGetLogoBlobIndexStatRequest,
            TEvGetLogoBlobIndexStatResponse>(
                hullCtx, parentId, std::move(snapshot), ev, std::move(result));
    }

} // NKikimr
