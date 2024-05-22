#pragma once

#include "blobstorage_hulldatamerger.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TRecordMergerBase
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TRecordMergerBase {
    public:
        bool Empty() const {
            return MemRecsMerged == 0;
        }

        const TMemRec &GetMemRec() const {
            Y_DEBUG_ABORT_UNLESS(Finished);
            return MemRec;
        }

        bool HaveToMergeData() const {
            return MergeData;
        }

        ui32 GetNumKeepFlags() const { return NumKeepFlags; }
        ui32 GetNumDoNotKeepFlags() const { return NumDoNotKeepFlags >> 1; }

    protected:
        const TBlobStorageGroupType GType;
        TMemRec MemRec;
        ui32 MemRecsMerged = 0; // number of items that took part in a merge for the current key
        ui32 NumKeepFlags = 0;
        ui32 NumDoNotKeepFlags = 0;
        bool Finished;
        const bool MergeData;

        TRecordMergerBase(const TBlobStorageGroupType &gtype, bool mergeData)
            : GType(gtype)
            , MemRec()
            , Finished(false)
            , MergeData(mergeData)
        {}

        void Clear() {
            MemRecsMerged = 0;
            NumKeepFlags = 0;
            NumDoNotKeepFlags = 0;
            Finished = false;
        }

        void AddBasic(const TMemRec &memRec, const TKey &key) {
            if constexpr (std::is_same_v<TMemRec, TMemRecLogoBlob>) {
                const int mode = memRec.GetIngress().GetCollectMode(TIngress::IngressMode(GType));
                static_assert(CollectModeKeep == 1);
                static_assert(CollectModeDoNotKeep == 2);
                NumKeepFlags += mode & CollectModeKeep;
                NumDoNotKeepFlags += mode & CollectModeDoNotKeep;
            }
            if (MemRecsMerged == 0) {
                MemRec = memRec;
                MemRec.SetNoBlob();
            } else {
                MemRec.Merge(memRec, key);
            }
            ++MemRecsMerged;
        }
    };


    ////////////////////////////////////////////////////////////////////////////
    // TIndexRecordMerger
    ////////////////////////////////////////////////////////////////////////////
    // Valid call sequence:
    // Clear(); Add(); ... Add(); Finish();
    // GetMemRec(); GetData();
    template <class TKey, class TMemRec>
    class TIndexRecordMerger : public TRecordMergerBase<TKey, TMemRec> {
        using TBase = TRecordMergerBase<TKey, TMemRec>;
        using TBase::MemRec;
        using TBase::GType;
        using TBase::Finished;
        using TBase::MergeData;
        using TBase::Empty;
        using TBase::AddBasic;

    public:
        TIndexRecordMerger(const TBlobStorageGroupType &gtype)
            : TBase(gtype, false)
        {}

        using TBase::Clear;
        using TBase::GetMemRec;
        using TBase::HaveToMergeData;

        void AddFromSegment(const TMemRec &memRec, const TDiskPart* /*outbound*/, const TKey &key, ui64 /*circaLsn*/) {
            AddBasic(memRec, key);
        }

        void AddFromFresh(const TMemRec &memRec, const TRope* /*data*/, const TKey &key, ui64 /*lsn*/) {
            AddBasic(memRec, key);
        }

        void Finish() {
            Y_DEBUG_ABORT_UNLESS(!Empty());
            Finished = true;
        }
    };


    ////////////////////////////////////////////////////////////////////////////
    // TCompactRecordMergerBase
    ////////////////////////////////////////////////////////////////////////////
    // Valid call sequence:
    // Clear(); Add(); ... Add(); Finish()
    // GetMemRec(); GetData();
    template <class TKey, class TMemRec>
    class TCompactRecordMergerBase : public TRecordMergerBase<TKey, TMemRec> {
    protected:
        using TBase = TRecordMergerBase<TKey, TMemRec>;
        using TBase::MemRec;
        using TBase::GType;
        using TBase::Finished;
        using TBase::MergeData;
        using TBase::Empty;
        using TBase::AddBasic;

        enum class ELoadData {
            NotSet = 0,
            LoadData = 1,
            DontLoadData = 2
        };

    public:
        TCompactRecordMergerBase(const TBlobStorageGroupType &gtype, bool addHeader)
            : TBase(gtype, true)
            , MemRecs()
            , ProducingSmallBlob(false)
            , NeedToLoadData(ELoadData::NotSet)
            , AddHeader(addHeader)
        {}

        void Clear() {
            TBase::Clear();
            MemRecs.clear();
            ProducingSmallBlob = false;
            NeedToLoadData = ELoadData::NotSet;
            DataMerger.Clear();
        }

        using TBase::GetMemRec;
        using TBase::HaveToMergeData;

        void SetLoadDataMode(bool loadData) {
            NeedToLoadData = loadData ? ELoadData::LoadData : ELoadData::DontLoadData;
        }

        void AddFromSegment(const TMemRec &memRec, const TDiskPart *outbound, const TKey &key, ui64 circaLsn) {
            Y_DEBUG_ABORT_UNLESS(NeedToLoadData != ELoadData::NotSet);
            AddBasic(memRec, key);
            switch (memRec.GetType()) {
                case TBlobType::DiskBlob: {
                    if (memRec.HasData() && NeedToLoadData == ELoadData::LoadData) {
                        MemRecs.push_back(memRec);
                        ProducingSmallBlob = true;
                    }
                    break;
                }
                case TBlobType::HugeBlob:
                case TBlobType::ManyHugeBlobs: {
                    TDiskDataExtractor extr;
                    memRec.GetDiskData(&extr, outbound);
                    const NMatrix::TVectorType v = memRec.GetLocalParts(GType);
                    DataMerger.AddHugeBlob(extr.Begin, extr.End, v, circaLsn);
                    break;
                }
                default:
                    Y_ABORT("Impossible case");
            }
            VerifyConsistency(memRec, outbound);
        }

        void AddFromFresh(const TMemRec &memRec, const TRope *data, const TKey &key, ui64 lsn) {
            Y_DEBUG_ABORT_UNLESS(NeedToLoadData != ELoadData::NotSet);
            AddBasic(memRec, key);
            if (memRec.HasData()) {
                if (data) {
                    Y_ABORT_UNLESS(memRec.GetType() == TBlobType::MemBlob || memRec.GetType() == TBlobType::DiskBlob);
                    if (NeedToLoadData == ELoadData::LoadData) {
                        DataMerger.AddBlob(TDiskBlob(data, memRec.GetLocalParts(GType), GType, key.LogoBlobID()));
                        ProducingSmallBlob = true;
                    } else {
                        // intentionally do nothing: don't add any data to DataMerger, because we don't need it
                    }
                } else {
                    Y_ABORT_UNLESS(memRec.GetType() == TBlobType::HugeBlob);
                    TDiskDataExtractor extr;
                    memRec.GetDiskData(&extr, nullptr);
                    const NMatrix::TVectorType v = memRec.GetLocalParts(GType);
                    DataMerger.AddHugeBlob(extr.Begin, extr.End, v, lsn);
                }
            }
            VerifyConsistency(memRec, nullptr);
        }

        void VerifyConsistency(const TMemRec& memRec, const TDiskPart *outbound) {
#ifdef NDEBUG
            return;
#endif
            if constexpr (std::is_same_v<TMemRec, TMemRecLogoBlob>) {
                if (const auto m = memRec.GetType(); m == TBlobType::HugeBlob || m == TBlobType::ManyHugeBlobs) {
                    TDiskDataExtractor extr;
                    memRec.GetDiskData(&extr, outbound);
                    Y_ABORT_UNLESS(extr.End - extr.Begin == memRec.GetIngress().LocalParts(GType).CountBits());
                }
                switch (memRec.GetType()) {
                    case TBlobType::DiskBlob:
                        Y_ABORT_UNLESS(!memRec.HasData() || DataMerger.GetHugeBlobMerger().Empty());
                        break;

                    case TBlobType::HugeBlob:
                    case TBlobType::ManyHugeBlobs:
                        Y_ABORT_UNLESS(memRec.HasData() && DataMerger.GetDiskBlobMerger().Empty());
                        break;

                    case TBlobType::MemBlob:
                        Y_ABORT_UNLESS(memRec.HasData() && DataMerger.GetHugeBlobMerger().Empty());
                        break;
                }
                VerifyConsistency();
            }
        }

        void VerifyConsistency() {
            if constexpr (std::is_same_v<TMemRec, TMemRecLogoBlob>) {
                Y_DEBUG_ABORT_UNLESS(DataMerger.GetHugeBlobMerger().Empty() || MemRec.GetIngress().LocalParts(GType).CountBits() ==
                    DataMerger.GetHugeBlobMerger().GetNumParts());
            }
        }

        void Finish() {
            Y_DEBUG_ABORT_UNLESS(!Empty());
            VerifyConsistency();

            // in case when we keep data and disk merger contains small blobs, we set up small blob record -- this logic
            // is used in replication and in fresh compaction
            if (NeedToLoadData == ELoadData::LoadData && DataMerger.HasSmallBlobs()) {
                TDiskPart addr(0, 0, DataMerger.GetDiskBlobRawSize(AddHeader));
                MemRec.SetDiskBlob(addr);
            }

            // clear local bits if we are not going to keep item's data
            if (NeedToLoadData == ELoadData::DontLoadData) {
                MemRec.ClearLocalParts(GType);
            }

            Finished = true;
            MemRec.SetType(DataMerger.GetType());
        }

        const TDataMerger *GetDataMerger() const {
            Y_DEBUG_ABORT_UNLESS(Finished);
            return &DataMerger;
        }

    protected:
        TStackVec<TMemRec, 16> MemRecs;
        bool ProducingSmallBlob;
        ELoadData NeedToLoadData;
        TDataMerger DataMerger;
        const bool AddHeader;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TCompactRecordMergerIndexPass
    ////////////////////////////////////////////////////////////////////////////
    template<typename TKey, typename TMemRec>
    class TCompactRecordMergerIndexPass : public TCompactRecordMergerBase<TKey, TMemRec> {
        using TBase = TCompactRecordMergerBase<TKey, TMemRec>;

        using ELoadData = typename TBase::ELoadData;

        using TBase::MemRecs;
        using TBase::ProducingSmallBlob;
        using TBase::NeedToLoadData;
        using TBase::DataMerger;
        using TBase::MemRec;

    public:
        TCompactRecordMergerIndexPass(const TBlobStorageGroupType &gtype, bool addHeader)
            : TBase(gtype, addHeader)
        {}

        void Finish() {
            if (NeedToLoadData == ELoadData::DontLoadData) {
                Y_ABORT_UNLESS(!DataMerger.HasSmallBlobs()); // we didn't put any small blob to the data merger
                // if we have huge blobs for the record, than we set TBlobType::HugeBlob or
                // TBlobType::ManyHugeBlobs a few lines below
                MemRec.SetNoBlob();
            }

            TBase::Finish();
        }

        template<typename TCallback>
        void ForEachSmallDiskBlob(TCallback&& callback) {
            for (const auto& memRec : MemRecs) {
                callback(memRec);
            }
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TCompactRecordMergerDataPass
    ////////////////////////////////////////////////////////////////////////////
    template<typename TKey, typename TMemRec>
    class TCompactRecordMergerDataPass : public TCompactRecordMergerBase<TKey, TMemRec> {
        using TBase = TCompactRecordMergerBase<TKey, TMemRec>;

        using TBase::ProducingSmallBlob;
        using TBase::MemRecs;
        using TBase::MemRec;
        using TBase::DataMerger;
        using TBase::GType;
        using TBase::SetLoadDataMode;

    public:
        TCompactRecordMergerDataPass(const TBlobStorageGroupType &gtype, bool addHeader)
            : TBase(gtype, addHeader)
        {
            SetLoadDataMode(true);
        }

        void Clear() {
            TBase::Clear();
            ReadSmallBlobs.clear();
            SetLoadDataMode(true);
        }

        // add read small blob content; they should come in order as returned from GetSmallBlobDiskParts by index merger
        void AddReadSmallBlob(TString data) {
            Y_ABORT_UNLESS(ProducingSmallBlob);
            ReadSmallBlobs.push_back(std::move(data));
        }

        void Finish() {
            // ensure we are producing small blobs; otherwise this merger should never be created
            Y_ABORT_UNLESS(ProducingSmallBlob);

            // add all read small blobs into blob merger
            const size_t count = ReadSmallBlobs.size();
            Y_ABORT_UNLESS(count == +MemRecs, "count# %zu +MemRecs# %zu", count, +MemRecs);
            for (size_t i = 0; i < count; ++i) {
                const TMemRec& memRec = MemRecs[i]->GetMemRec();
                const TString& buffer = ReadSmallBlobs[i];
                Y_ABORT_UNLESS(buffer.size() == memRec.DataSize());
                DataMerger.AddBlob(TDiskBlob(buffer.data(), buffer.size(), memRec.GetLocalParts(GType)));
            }


            // ensure that data merger has small blob
            Y_ABORT_UNLESS(DataMerger.HasSmallBlobs());

            // finalize base class logic; it also generates blob record
            TBase::Finish();

            // ensure that we have generated correct DiskBlob with full set of declared parts
            const TDiskBlob& blob = DataMerger.GetDiskBlobMerger().GetDiskBlob();
            Y_ABORT_UNLESS(blob.GetParts() == MemRec.GetLocalParts(GType));
            Y_ABORT_UNLESS(MemRec.GetType() == TBlobType::DiskBlob);
        }

    private:
        TVector<TString> ReadSmallBlobs;
    };


    ////////////////////////////////////////////////////////////////////////////
    // TRecordMergerCallback
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec, class TCallback>
    class TRecordMergerCallback : public TRecordMergerBase<TKey, TMemRec> {
        using TBase = TRecordMergerBase<TKey, TMemRec>;
        using TBase::MemRec;
        using TBase::GType;
        using TBase::Finished;
        using TBase::Empty;
        using TBase::AddBasic;

    public:
        TRecordMergerCallback(TCallback *callback, const TBlobStorageGroupType &gtype)
            : TBase(gtype, true)
            , Callback(callback)
            , LastWriteWinsMerger(gtype.TotalPartCount())
        {
            Clear();
        }

        void Clear() {
            TBase::Clear();
            LastWriteWinsMerger.Clear();
        }

        void AddFromSegment(const TMemRec &memRec, const TDiskPart *outbound, const TKey &key, ui64 circaLsn) {
            AddBasic(memRec, key);
            TDiskDataExtractor extr;
            memRec.GetDiskData(&extr, outbound);
            const NMatrix::TVectorType v = memRec.GetLocalParts(GType);
            ui32 n = extr.End - extr.Begin;
            switch (n) {
                case 0: break;
                case 1: {
                    if (memRec.GetType() == TBlobType::DiskBlob) {
                        // don't deduplicate inplaced data
                        const TDiskPart &data = extr.SwearOne();
                        if (data.ChunkIdx && data.Size) {
                            (*Callback)(data, v);
                        }
                    } else if (memRec.GetType() == TBlobType::HugeBlob) {
                        Y_ABORT_UNLESS(v.CountBits() == 1u);
                        // deduplicate huge blob
                        LastWriteWinsMerger.Add(extr.SwearOne(), v, circaLsn);
                    } else {
                        Y_ABORT("Unexpected case");
                    }

                    break;
                }
                default: {
                    // many blobs
                    Y_ABORT_UNLESS(memRec.GetType() == TBlobType::ManyHugeBlobs);
                    const TDiskPart *dataPtr = extr.Begin;
                    ui8 i = v.FirstPosition();
                    while (i != v.GetSize() && dataPtr != extr.End) {
                        NMatrix::TVectorType onePart(0, v.GetSize());
                        onePart.Set(i);
                        // deduplicate every huge blob
                        LastWriteWinsMerger.Add(*dataPtr, onePart, circaLsn);
                        i = v.NextPosition(i);
                        dataPtr++;
                    }
                    break;
                }
            }
        }

        void AddFromFresh(const TMemRec &memRec, const TRope *data, const TKey &key, ui64 lsn) {
            AddBasic(memRec, key);
            if (memRec.HasData()) {
                const NMatrix::TVectorType v = memRec.GetLocalParts(GType);
                if (data) {
                    Y_ABORT_UNLESS(memRec.GetType() == TBlobType::MemBlob);
                    // we have in-memory data in a rope, it always wins among other data,
                    // so we call Callback immediately and remove any data for this local part
                    // from LastWriteWinsMerger
                    (*Callback)(TDiskBlob(data, v, GType, key.LogoBlobID()));
                } else {
                    Y_ABORT_UNLESS(memRec.GetType() == TBlobType::HugeBlob && v.CountBits() == 1u);
                    TDiskDataExtractor extr;
                    memRec.GetDiskData(&extr, nullptr);
                    // deduplicate huge blob
                    LastWriteWinsMerger.Add(extr.SwearOne(), v, lsn);
                }
            }
        }

        void Finish() {
            Y_DEBUG_ABORT_UNLESS(!Empty());
            LastWriteWinsMerger.Finish(Callback);
            Finished = true;
        }

    private:
        class TLastWriteWins {
        public:
            TLastWriteWins(ui8 vecSize)
                : ResParts(0, vecSize)
            {}

            void Add(const TDiskPart &diskPart, NMatrix::TVectorType v, ui64 circaLsn) {
                Y_DEBUG_ABORT_UNLESS(v.CountBits() == 1u);
                const ui8 idx = v.FirstPosition();
                TResItem& item = Res[idx];
                if (item.CircaLsn < circaLsn) {
                    item = {circaLsn, diskPart};
                }
                ResParts.Set(idx);
            }

            void Finish(TCallback *callback) {
                for (ui8 i = ResParts.FirstPosition(); i != ResParts.GetSize(); i = ResParts.NextPosition(i)) {
                    (*callback)(Res[i].Part, NMatrix::TVectorType::MakeOneHot(i, ResParts.GetSize()));
                }
            }

            void Clear() {
                Res.fill(TResItem());
                ResParts.Clear();
            }

        private:
            struct TResItem {
                ui64 CircaLsn = 0;
                TDiskPart Part;
            };
            NMatrix::TVectorType ResParts;
            std::array<TResItem, 8> Res;
        };

        TCallback *Callback;
        TLastWriteWins LastWriteWinsMerger;
    };

} // NKikimr
