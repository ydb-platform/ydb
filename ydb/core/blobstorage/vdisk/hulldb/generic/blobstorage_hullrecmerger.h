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

        TIngress GetCurrentIngress() const {
            if constexpr (std::is_same_v<TKey, TKeyLogoBlob>) {
                return MemRec.GetIngress();
            } else {
                return {};
            }
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

        void AddBasic(const TMemRec &memRec, const TKey &key, bool clearLocal = false) {
            if constexpr (std::is_same_v<TMemRec, TMemRecLogoBlob>) {
                const int mode = memRec.GetIngress().GetCollectMode(TIngress::IngressMode(GType));
                static_assert(CollectModeKeep == 1);
                static_assert(CollectModeDoNotKeep == 2);
                NumKeepFlags += mode & CollectModeKeep;
                NumDoNotKeepFlags += mode & CollectModeDoNotKeep;
            }
            if (MemRecsMerged == 0) {
                if constexpr (std::is_same_v<TKey, TKeyLogoBlob>) {
                    MemRec = clearLocal ? TMemRec(memRec.GetIngress().CopyWithoutLocal(GType)) : memRec;
                } else {
                    MemRec = memRec;
                }
                MemRec.SetNoBlob();
            } else {
                MemRec.Merge(memRec, key, clearLocal, GType);
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

        void AddFromSegment(const TMemRec &memRec, const TDiskPart* /*outbound*/, const TKey &key, ui64 /*circaLsn*/,
                const void* /*sst*/) {
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
    // Valid call sequence:
    // Clear(); Add(); ... Add(); Finish()
    // GetMemRec(); GetData();
    template <class TKey, class TMemRec>
    class TCompactRecordMerger : public TRecordMergerBase<TKey, TMemRec> {
    protected:
        using TBase = TRecordMergerBase<TKey, TMemRec>;
        using TBase::MemRec;
        using TBase::GType;
        using TBase::Finished;
        using TBase::MergeData;
        using TBase::Empty;
        using TBase::AddBasic;

    public:
        TCompactRecordMerger(TBlobStorageGroupType gtype, bool addHeader)
            : TBase(gtype, true)
            , AddHeader(addHeader)
            , DataMerger(gtype, addHeader)
        {}

        void Clear() {
            TBase::Clear();
            DataMerger.Clear();
            Key = {};
            ExternalDataStage = false;
        }

        using TBase::GetMemRec;
        using TBase::HaveToMergeData;

        void AddFromSegment(const TMemRec &memRec, const TDiskPart *outbound, const TKey &key, ui64 circaLsn,
                const void* /*sst*/) {
            Add(memRec, outbound, key, circaLsn);
        }

        void AddFromFresh(const TMemRec &memRec, const TRope *data, const TKey &key, ui64 lsn) {
            Add(memRec, data, key, lsn);
        }

        void Add(const TMemRec& memRec, std::variant<const TRope*, const TDiskPart*> dataOrOutbound, const TKey& key, ui64 lsn) {
            Y_DEBUG_ABORT_UNLESS(Key == TKey{} || Key == key);
            Key = key;
            AddBasic(memRec, key, /*clearLocal=*/ ExternalDataStage);
            if constexpr (std::is_same_v<TKey, TKeyLogoBlob>) {
                if (ExternalDataStage) {
                    const TDiskPart **outbound = std::get_if<const TDiskPart*>(&dataOrOutbound);
                    DataMerger.CheckExternalData(memRec, outbound ? *outbound : nullptr, lsn);
                } else {
                    DataMerger.Add(memRec, dataOrOutbound, lsn, key.LogoBlobID());
                }
            }
        }

        void Finish(bool targetingHugeBlob, bool keepData) {
            Y_DEBUG_ABORT_UNLESS(!Finished);
            Y_DEBUG_ABORT_UNLESS(!Empty());

            if constexpr (std::is_same_v<TKey, TKeyLogoBlob>) {
                DataMerger.Finish(targetingHugeBlob, Key.LogoBlobID(), keepData);
                MemRec.ReplaceLocalParts(GType, DataMerger.GetParts());
                MemRec.SetDiskBlob(TDiskPart(0, 0, DataMerger.GetInplacedBlobSize(Key.LogoBlobID())));
                MemRec.SetType(DataMerger.GetType());
            }

            Finished = true;
        }

        TDataMerger& GetDataMerger() {
            Y_DEBUG_ABORT_UNLESS(Finished);
            return DataMerger;
        }

        bool IsDataMergerEmpty() const {
            return DataMerger.Empty();
        }

        void SetExternalDataStage() {
            Y_DEBUG_ABORT_UNLESS(!ExternalDataStage);
            ExternalDataStage = true;
        }

        const TMemRec& GetMemRecForBarriers() const {
            return MemRec;
        }

    protected:
        const bool AddHeader;
        TDataMerger DataMerger;
        TKey Key;
        bool ExternalDataStage = false;
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

        void AddFromSegment(const TMemRec &memRec, const TDiskPart *outbound, const TKey &key, ui64 circaLsn,
                const void* /*sst*/) {
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
                        if (!v.Empty()) {
                            (*Callback)(extr.SwearOne(), v);
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
            if (const NMatrix::TVectorType local = memRec.GetLocalParts(GType); !local.Empty()) {
                TDiskDataExtractor extr;
                static TRope rope;
                switch (memRec.GetType()) {
                    case TBlobType::MemBlob:
                        // we have in-memory data in a rope, it always wins among other data,
                        // so we call Callback immediately and remove any data for this local part
                        // from LastWriteWinsMerger
                        Y_ABORT_UNLESS(data); // HaveToMergeData is true, so data must be present
                        (*Callback)(TDiskBlob(data, local, GType, key.LogoBlobID()));
                        break;

                    case TBlobType::DiskBlob:
                        Y_ABORT_UNLESS(!memRec.HasData());
                        (*Callback)(TDiskBlob(&rope, local, GType, key.LogoBlobID())); // pure metadata parts only
                        break;

                    case TBlobType::HugeBlob:
                        Y_ABORT_UNLESS(local.CountBits() == 1);
                        memRec.GetDiskData(&extr, nullptr);
                        LastWriteWinsMerger.Add(extr.SwearOne(), local, lsn);
                        break;

                    case TBlobType::ManyHugeBlobs:
                        Y_ABORT("unexpected case");
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
