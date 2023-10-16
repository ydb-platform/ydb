#include "assimilation.h"

namespace NKikimr {

    class TAssimilationActor : public TActorBootstrapped<TAssimilationActor> {
        enum {
            EvResume = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        THullDsSnap Snap;
        std::unique_ptr<TEvBlobStorage::TEvVAssimilate::THandle> Ev;
        std::unique_ptr<TEvBlobStorage::TEvVAssimilateResult> Result;
        TActorId ParentId;
        std::optional<TKeyBlock> SkipBlocksUpTo;
        std::optional<TKeyBarrier> SkipBarriersUpTo;
        std::optional<TKeyLogoBlob> SkipBlobsUpTo;
        ui64 LastRawBlobId[3] = {0, 0, 0};
        size_t RecordSize;

        static constexpr TDuration MaxQuantumTime = TDuration::MilliSeconds(10);
        static constexpr size_t MaxResultSize = 5'000'000; // may overshoot a little

    public:
        TAssimilationActor(THullDsSnap&& snap, TEvBlobStorage::TEvVAssimilate::TPtr& ev, TVDiskID vdiskId)
            : Snap(std::move(snap))
            , Ev(ev.Release())
            , Result(std::make_unique<TEvBlobStorage::TEvVAssimilateResult>(NKikimrProto::OK, TString(), vdiskId))
        {
            const auto& record = Ev->Get()->Record;
            if (record.HasSkipBlocksUpTo()) {
                SkipBlocksUpTo.emplace(record.GetSkipBlocksUpTo());
            }
            if (record.HasSkipBarriersUpTo()) {
                const auto& r = record.GetSkipBarriersUpTo();
                SkipBarriersUpTo.emplace(r.GetTabletId(), r.GetChannel(), Max<ui32>(), Max<ui32>(), true);
            }
            if (record.HasSkipBlobsUpTo()) {
                SkipBlobsUpTo.emplace(LogoBlobIDFromLogoBlobID(record.GetSkipBlobsUpTo()));
            }

            RecordSize = Result->Record.ByteSizeLong();
        }

        void Bootstrap(const TActorId& parentId) {
            ParentId = parentId;
            Become(&TThis::StateFunc);
            Quantum();
        }

        void Quantum() {
            THPTimer timer;
            bool success = true;
            success = success && Iterate<TKeyBlock, TMemRecBlock>(&Snap.BlocksSnap, SkipBlocksUpTo, timer);
            success = success && Iterate<TKeyBarrier, TMemRecBarrier>(&Snap.BarriersSnap, SkipBarriersUpTo, timer);
            success = success && Iterate<TKeyLogoBlob, TMemRecLogoBlob>(&Snap.LogoBlobsSnap, SkipBlobsUpTo, timer);
            if (success) {
                SendResultAndDie();
            }
        }

        template<typename TKey, typename TMemRec>
        class TIterWrapper {
            typename TLevelIndexSnapshot<TKey, TMemRec>::TIndexForwardIterator Iter;
            std::optional<TKey>& SkipUpTo;
            ui32 NumItemsProcessed = 0;

        public:
            TIterWrapper(const TLevelIndexSnapshot<TKey, TMemRec> *snap, const THullCtxPtr& hullCtx, std::optional<TKey>& skipUpTo)
                : Iter(hullCtx, snap)
                , SkipUpTo(skipUpTo)
            {
                if (SkipUpTo) {
                    Iter.Seek(*SkipUpTo);
                    if (Iter.Valid() && Iter.GetCurKey() == *SkipUpTo) {
                        Iter.Next();
                    }
                } else {
                    Iter.SeekToFirst();
                }
            }

            operator bool() const { return Iter.Valid(); }
            operator TKey() const { return Iter.GetCurKey(); }
            operator TMemRec() const { return Iter.GetMemRec(); }

            void Next() {
                SkipUpTo = Iter.GetCurKey();
                ++NumItemsProcessed;
                Iter.Next();
            }

            bool NeedConstraintCheck() {
                if (NumItemsProcessed >= 1000) {
                    NumItemsProcessed = 0;
                    return true;
                } else {
                    return false;
                }
            }
        };

        using TBlockIter = TIterWrapper<TKeyBlock, TMemRecBlock>;
        using TBarrierIter = TIterWrapper<TKeyBarrier, TMemRecBarrier>;
        using TBlobIter = TIterWrapper<TKeyLogoBlob, TMemRecLogoBlob>;

        void Process(TBlockIter& iter) {
            const TKeyBlock& key = iter;
            const TMemRecBlock& memRec = iter;

            auto *pb = Result->Record.AddBlocks();
            pb->SetTabletId(key.TabletId);
            pb->SetBlockedGeneration(memRec.BlockedGeneration);

            // calculate size of the item
            using T = NKikimrBlobStorage::TEvVAssimilateResult::TBlock;
            size_t len = CountFixed64(T::kTabletIdFieldNumber) + CountUnsigned(T::kBlockedGenerationFieldNumber,
                memRec.BlockedGeneration);
            len += CountMessage(NKikimrBlobStorage::TEvVAssimilateResult::kBlocksFieldNumber, len);
            RecordSize += len;

            return iter.Next();
        }

        void Process(TBarrierIter& iter) {
            const TKeyBarrier key = iter;

            auto *pb = Result->Record.AddBarriers();
            pb->SetTabletId(key.TabletId);
            pb->SetChannel(key.Channel);

            for (; iter; iter.Next()) {
                const TKeyBarrier& next = iter;
                const TMemRecBarrier& memRec = iter;
                if (next.TabletId != key.TabletId || next.Channel != key.Channel) {
                    break;
                } else if (memRec.Ingress.IsQuorum(Snap.HullCtx->IngressCache.Get())) {
                    auto *value = next.Hard ? pb->MutableHard() : pb->MutableSoft();
                    value->SetRecordGeneration(next.Gen);
                    value->SetPerGenerationCounter(next.GenCounter);
                    value->SetCollectGeneration(memRec.CollectGen);
                    value->SetCollectStep(memRec.CollectStep);
                }
            }

            using T = NKikimrBlobStorage::TEvVAssimilateResult::TBarrier;
            size_t len = CountFixed64(T::kTabletIdFieldNumber) + CountUnsigned(T::kChannelFieldNumber, key.Channel);
            for (const auto *value : {pb->HasHard() ? &pb->GetHard() : nullptr, pb->HasSoft() ? &pb->GetSoft() : nullptr}) {
                if (value) {
                    size_t valueLen = 0;
                    valueLen += CountUnsigned(T::TValue::kRecordGenerationFieldNumber, value->GetRecordGeneration());
                    valueLen += CountUnsigned(T::TValue::kPerGenerationCounterFieldNumber, value->GetPerGenerationCounter());
                    valueLen += CountUnsigned(T::TValue::kCollectGenerationFieldNumber, value->GetCollectGeneration());
                    valueLen += CountUnsigned(T::TValue::kCollectStepFieldNumber, value->GetCollectStep());
                    valueLen += CountMessage(value == &pb->GetHard() ? T::kHardFieldNumber : T::kSoftFieldNumber, valueLen);
                    len += valueLen;
                }
            }

            len += CountMessage(NKikimrBlobStorage::TEvVAssimilateResult::kBarriersFieldNumber, len);
            RecordSize += len;
        }

        void Process(TBlobIter& iter) {
            const TKeyLogoBlob& key = iter;
            const TMemRecLogoBlob& memRec = iter;
            auto *pb = Result->Record.AddBlobs();

            const TLogoBlobID id(key.LogoBlobID());
            const ui64 *raw = id.GetRaw();

            using T = NKikimrBlobStorage::TEvVAssimilateResult::TBlob;
            size_t len = 0;

#define DIFF(INDEX, NAME) \
            if (raw[INDEX] != LastRawBlobId[INDEX]) { \
                const ui64 delta = raw[INDEX] - LastRawBlobId[INDEX]; \
                if (delta >> 49) { /* it's gonna be 8 bytes or more on the wire */ \
                    pb->SetRaw##NAME(raw[INDEX]); \
                    len += CountFixed64(T::kRaw##NAME##FieldNumber); \
                } else { \
                    pb->SetDiff##NAME(delta); \
                    len += CountUnsigned(T::kDiff##NAME##FieldNumber, delta); \
                } \
            }

            DIFF(0, X1)
            DIFF(1, X2)
            DIFF(2, X3)

            memcpy(LastRawBlobId, raw, 3 * sizeof(ui64));

            pb->SetIngress(memRec.GetIngress().Raw());

            len += CountUnsigned(T::kIngressFieldNumber, pb->GetIngress());
            len += CountMessage(NKikimrBlobStorage::TEvVAssimilateResult::kBlobsFieldNumber, len);
            RecordSize += len;

            return iter.Next();
        }

        template<typename TKey, typename TMemRec>
        bool Iterate(const TLevelIndexSnapshot<TKey, TMemRec> *snap, std::optional<TKey>& skipUpTo, THPTimer& timer) {
            for (TIterWrapper<TKey, TMemRec> iter(snap, Snap.HullCtx, skipUpTo); iter; ) {
                if (RecordSize >= MaxResultSize) {
                    SendResultAndDie();
                    return false;
                }

                Process(iter);
                if (iter.NeedConstraintCheck() && TDuration::Seconds(timer.Passed()) >= MaxQuantumTime) {
                    TActivationContext::Send(new IEventHandle(EvResume, 0, SelfId(), {}, nullptr, 0));
                    return false; // has to restart
                }
            }
            return true; // finished scanning this database
        }

        void SendResultAndDie() {
#ifndef NDEBUG
            const size_t actualLen = Result->Record.ByteSizeLong();
            Y_DEBUG_ABORT_UNLESS(actualLen == RecordSize, "actualLen# %zu != RecordSize# %zu", actualLen, RecordSize);
#endif
            Send(Ev->Sender, Result.release(), IEventHandle::MakeFlags(TInterconnectChannels::IC_BLOBSTORAGE_ASYNC_DATA, 0),
                Ev->Cookie);
            PassAway();
        }

        void PassAway() override {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::ActorDied, 0, ParentId, SelfId(), nullptr, 0));
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(StateFunc,
            cFunc(EvResume, Quantum);
            cFunc(TEvents::TSystem::Poison, PassAway);
        )

    private:
        size_t CountVarInt(ui64 value) {
            return 1 + MostSignificantBit(value) / 7; // protobuf varint encoding
        }

        size_t CountHeader(int fieldNumber) {
            return CountVarInt(fieldNumber << 3);
        }

        size_t CountMessage(int fieldNumber, size_t len) {
            return CountHeader(fieldNumber) + CountVarInt(len);
        }

        size_t CountFixed64(int fieldNumber) {
            return CountHeader(fieldNumber) + sizeof(ui64);
        }

        size_t CountUnsigned(int fieldNumber, ui64 value) {
            return CountHeader(fieldNumber) + CountVarInt(value);
        }
    };

    IActor *CreateAssimilationActor(THullDsSnap&& snap, TEvBlobStorage::TEvVAssimilate::TPtr& ev, TVDiskID vdiskId) {
        return new TAssimilationActor(std::move(snap), ev, vdiskId);
    }

} // NKikimr
