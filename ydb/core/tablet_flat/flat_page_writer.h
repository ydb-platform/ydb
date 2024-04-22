#pragma once

#include "flat_page_conf.h"
#include "flat_page_label.h"
#include "flat_page_data.h"
#include "flat_page_flat_index.h"
#include "flat_part_iface.h"
#include "flat_part_pinout.h"
#include "flat_row_state.h"
#include "util_fmt_abort.h"
#include "util_deref.h"

#include <util/generic/vector.h>
#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NTable {
namespace NPage {

    struct TDataPageBuilder {
        TDataPageBuilder(EPage type, ui16 version, bool label, ui32 extra)
            : Type(type)
            , Version(version)
            , V2Label(label)
            , Extra(extra)
            , Prefix(sizeof(TLabel) + (label ? sizeof(TLabelExt) : 0) + sizeof(TRecordsHeader) + Extra)
        {
            Y_ABORT_UNLESS((version & 0x8000) == 0, "Invalid version value");
        }

        explicit operator bool() const noexcept
        {
            return Tail && Blob;
        }

        template<typename T>
        T* ExtraAs() noexcept
        {
            Y_ABORT_UNLESS(sizeof(T) == Extra, "Cannot cast extra block to T");

            return TDeref<T>::At(Blob.mutable_data(), Prefix - Extra);
        }

        void Grow(size_t more, size_t least, float factor) noexcept
        {
            if (Blob) {
                size_t desired = BytesUsed() + more;
                PageBytes = Max(PageBytes, desired);
                if (Left() < more) {
                    size_t min = size_t(Blob.size() * factor);
                    Resize(Max(desired, min));
                }
            } else {
                Open(more, least);
            }
        }

        void Open(size_t more, size_t least, ui32 rows = 0) noexcept
        {
            Y_ABORT_UNLESS(!Blob, "TDataPageBuilder is already has live page");

            PageBytes = Max(least, BytesUsed() + more);
            PageRows = rows ? rows : Max<ui32>();
            Resize(PageBytes);
        }

        bool Overflow(size_t more, ui32 rows) const noexcept
        {
            return Blob && Deltas.empty() && ((BytesUsed() + more) > PageBytes || (Offsets.size() + rows > PageRows));
        }

        TSharedData Close() noexcept
        {
            Y_ABORT_UNLESS(Deltas.empty());

            if (!Blob)
                return { };

            Y_DEBUG_ABORT_UNLESS(BytesUsed() <= PageBytes);

            Blob.TrimBack(BytesUsed());

            char *ptr = Blob.mutable_begin();

            if (V2Label) {
                WriteUnaligned<NPage::TLabel>(ptr, TLabel::Encode(Type, Version | 0x8000, Blob.size()));
                ptr += sizeof(TLabel);
                WriteUnaligned<NPage::TLabelExt>(ptr, TLabelExt::Encode(ECodec::Plain));
                ptr += sizeof(TLabelExt);
            } else {
                WriteUnaligned<NPage::TLabel>(ptr, TLabel::Encode(Type, Version, Blob.size()));
                ptr += sizeof(TLabel);
            }

            if (auto *hdr = TDeref<TRecordsHeader>::At(ptr, 0)) {
                hdr->Count = Offsets.size();
            }

            { /* Place on the end reconds offsets */
                auto *buf = TDeref<char>::At(Offsets.data(), 0);

                Write(buf, Offsets.size() * sizeof(TPgSize));
            }

            NSan::CheckMemIsInitialized(Blob.data(), Blob.size());

            return Reset();
        }

        TSharedData Reset() noexcept
        {
            Tail = nullptr;
            Deltas.clear();
            if (Deltas.capacity() > 1024) {
                TVector<ui64>().swap(Deltas);
            }
            Offsets.clear();
            if (Offsets.capacity() > 10240) {
                TVector<TPgSize>().swap(Offsets);
            }

            return std::exchange(Blob, TSharedData{ });
        }

        ui32 PrefixSize() const noexcept
        {
            return Prefix;
        }

        size_t BytesUsed() const noexcept
        {
            return Tail ? (Offset() + sizeof(ui64) * Deltas.size() + sizeof(TPgSize) * Offsets.size()) : Prefix;
        }

        size_t Left() const noexcept
        {
            return Tail ? (Blob.size() - BytesUsed()) : 0;
        }

        void Zero(size_t size) noexcept
        {
            auto *from = Advance(size);
            std::fill(from, Tail, 0);
        }

        template <class T>
        T& Place()
        {
            return *reinterpret_cast<T*>(Advance(TPgSizeOf<T>::Value));
        }

        bool HasDeltas() const noexcept
        {
            return !Deltas.empty();
        }

        void PushDelta(TPgSize recordSize) noexcept
        {
            Y_DEBUG_ABORT_UNLESS(recordSize > 0);

            if (Deltas.empty()) {
                Y_DEBUG_ABORT_UNLESS(recordSize <= Left());
                Y_DEBUG_ABORT_UNLESS(BytesUsed() + recordSize <= PageBytes);
                Offsets.push_back(Max<ui32>());
            } else {
                Grow(recordSize, 0, 1.5);
            }

            size_t offset = Offset();
            Deltas.push_back(offset);
        }

        void PushOffset(TPgSize recordSize) noexcept
        {
            if (Deltas.empty()) {
                Y_DEBUG_ABORT_UNLESS(recordSize > 0);
                Y_DEBUG_ABORT_UNLESS(recordSize <= Left());
                Y_DEBUG_ABORT_UNLESS(BytesUsed() + recordSize <= PageBytes);
                size_t offset = Offset();
                Y_ABORT_UNLESS(offset < Max<ui32>(), "Record offset is out of bounds");
                Offsets.push_back(offset);
            } else {
                if (recordSize) {
                    Grow(recordSize, 0, 1.5);
                }

                // Memory bounds for all currently serialized deltas
                size_t memoryStart = Deltas.front();
                size_t memoryEnd = Offset();

                // Add a delta table before the first delta, moving everything downwards
                const size_t shift = Deltas.size() * sizeof(ui64);
                Advance(shift);

                char* ptr = Blob.mutable_begin() + memoryStart;
                ::memmove(ptr + shift, ptr, memoryEnd - memoryStart);

                // The first (actually last) offset is either zero or relative committed record offset
                WriteUnaligned<ui64>(ptr, recordSize ? memoryEnd - memoryStart : 0);
                ptr += sizeof(ui64);

                // Next we add other relative delta offsets, back to front
                while (Deltas.size() > 1) {
                    WriteUnaligned<ui64>(ptr, Deltas.back() - memoryStart);
                    ptr += sizeof(ui64);
                    Deltas.pop_back();
                }

                // Delta chain starts at the first delta
                ui64 start = Deltas.back() + shift;
                Deltas.pop_back();

                // Chain start must be within the first 4GB or the offset would be corrupted
                Y_ABORT_UNLESS(start < Max<ui32>(), "Record offset is out of bounds");
                Y_ABORT_UNLESS(Offsets.back() == Max<ui32>());
                Offsets.back() = start;

                Y_ABORT_UNLESS(Deltas.empty());
            }
        }

        template<typename TRecord>
        auto* AddValue(const TPartScheme::TColumn& info, TCell value, TRecord& rec)
        {
            auto* item = rec.GetItem(info);
            if (info.IsFixed) {
                Y_ABORT_UNLESS(value.Size() == info.FixedSize, "invalid fixed cell size)");
                memcpy(rec.template GetFixed<void>(item), value.Data(), value.Size());
            } else {
                auto *ref = rec.template GetFixed<TDataRef>(item);
                ref->Offset = Offset(rec.Base());
                ref->Size = value.Size();
                Write(value.Data(), value.Size());
            }
            return item;
        }

    private:
        void Resize(size_t bytes) noexcept
        {
            Y_ABORT_UNLESS(bytes > Prefix, "Too few bytes for page");

            if (auto was = std::exchange(Blob, TSharedData::Uninitialized(bytes))) {
                char *end = Blob.mutable_data();

                Tail = std::copy(was.begin(), static_cast<const char*>(Tail), end);
            } else {
                char *end = Blob.mutable_data();

                Tail = TDeref<char>::At(end, Prefix);

                std::fill(Tail - Prefix, Tail, 0);
            }
        }

        size_t Offset(const void *base = nullptr) const noexcept
        {
            return Tail - (const char*)(base ? base : Blob.begin());
        }

        void Write(const char *buf, size_t size) noexcept
        {
            std::copy(buf, buf + size, Advance(size));
        }

        char* Advance(size_t size) noexcept
        {
            size_t offset = Tail - Blob.mutable_begin();
            size_t available = Blob.size() - offset;
            Y_ABORT_UNLESS(size <= available, "Requested %" PRISZT " bytes, have %" PRISZT " available", size, available);
            Y_DEBUG_ABORT_UNLESS(offset + size <= PageBytes, "Requested bytes are out of current page limits");
            return std::exchange(Tail, Tail + size);
        }

    private:
        const EPage Type = EPage::Undef;
        const ui16 Version = Max<ui16>();
        const bool V2Label = false;     /* Put new style NPage label    */
        const ui32 Extra = 0;           /* Size of extra data in prefix */
        const ui32 Prefix = 0;          /* Prefix size (label + heder)  */

        TSharedData Blob;
        char* Tail = nullptr;
        TVector<TPgSize> Offsets;
        TVector<ui64> Deltas;
        ui64 PageBytes = Max<ui64>();   /* Max bytes per each page blob */
        ui32 PageRows = Max<ui32>();    /* Max rows per each rows blob  */
    };


    class TDataPageWriter {
    public:
        struct TSizeInfo {
            TPgSize DataPageSize = 0;
            TPgSize SmallSize = 0;
            TPgSize LargeSize = 0;
            ui32 NewSmallRefs = 0;
            ui32 NewLargeRefs = 0;
            ui32 ReusedSmallRefs = 0;
            ui32 ReusedLargeRefs = 0;
            bool Overflow = false;
        };

    public:
        TDataPageWriter(TIntrusiveConstPtr<TPartScheme> scheme, const TConf &conf, TTagsRef tags, TGroupId groupId)
            : Scheme(std::move(scheme))
            , PageSize(conf.Groups[groupId.Index].PageSize)
            , PageRows(conf.Groups[groupId.Index].PageRows)
            , SmallEdge(conf.SmallEdge)
            , LargeEdge(conf.LargeEdge)
            , MaxLargeBlob(conf.MaxLargeBlob)
            , Pinout(Scheme->MakePinout(tags, groupId.Index))
            , GroupId(groupId)
            , GroupInfo(Scheme->GetLayout(groupId))
            , DataPageBuilder(EPage::DataPage, 1, bool(conf.Groups[groupId.Index].Codec), sizeof(TDataPage::TExtra))
        {
            size_t expected = GroupInfo.Columns.size() - GroupInfo.ColsKeyData.size();

            Y_ABORT_UNLESS(Pinout.size() == expected, "TDataPageWriter got an invalid pinout");
        }

        ui32 PrefixSize() const noexcept
        {
            return DataPageBuilder.PrefixSize();
        }

        ui64 BytesUsed() const noexcept
        {
            return DataPageBuilder.BytesUsed();
        }

        TSizeInfo CalcSize(TCellsRef key, const TRowState& row, bool finalRow, TRowVersion minVersion, TRowVersion maxVersion, ui64 txId) const
        {
            Y_ABORT_UNLESS(key.size() == GroupInfo.KeyTypes.size());

            const bool isErased = GroupId.Index == 0 && maxVersion < TRowVersion::Max();
            const bool isVersioned = GroupId.Index == 0 && minVersion > TRowVersion::Min();
            const bool isDelta = txId != 0;

            TSizeInfo ret;
            ret.DataPageSize = TPgSizeOf<NPage::TDataPage::TRecord>::Value;
            ret.DataPageSize += !DataPageBuilder.HasDeltas() ? sizeof(TPgSize) : 0; // entry in the offsets table
            ret.DataPageSize += isDelta ? sizeof(ui64) : 0; // every row delta has an additional offset delta
            ret.DataPageSize += GroupInfo.FixedSize;
            ret.DataPageSize += isErased ? sizeof(NPage::TDataPage::TVersion) : 0;
            ret.DataPageSize += isVersioned ? sizeof(NPage::TDataPage::TVersion) : 0;
            ret.DataPageSize += GroupId.Index == 0 && isDelta ? sizeof(NPage::TDataPage::TDelta) : 0;

            // Only the main group includes the key
            for (TPos it = 0; it < GroupInfo.ColsKeyData.size(); it++) {
                ret.DataPageSize += GroupInfo.ColsKeyData[it].IsFixed ? 0 : key[it].Size();
            }

            for (const auto& pin : Pinout) {
                auto &info = GroupId.Historic ? Scheme->HistoryColumns[pin.From] : Scheme->AllColumns[pin.From];

                if (!row.IsFinalized(pin.To) || info.IsKey() || info.IsFixed) {

                } else if (row.GetCellOp(pin.To) != ELargeObj::Inline) {
                    /* External blob occupies only fixed technical field */
                    ++ret.ReusedLargeRefs;
                } else if (!isDelta && IsLargeSize(row.Get(pin.To).Size())) {
                    ret.LargeSize += row.Get(pin.To).Size();
                    ++ret.NewLargeRefs;
                } else if (!isDelta && IsSmallSize(row.Get(pin.To).Size())) {
                    ret.SmallSize += row.Get(pin.To).Size();
                    ++ret.NewSmallRefs;
                } else if (isDelta || !finalRow || row.GetCellOp(pin.To) != ECellOp::Reset) {
                    ret.DataPageSize += row.Get(pin.To).Size();
                }
            }

            ret.Overflow = DataPageBuilder.Overflow(ret.DataPageSize, 1);
            return ret;
        }

        void Add(const TSizeInfo& more, TCellsRef key, const TRowState& row, ISaver &saver, bool finalRow, TRowVersion minVersion, TRowVersion maxVersion, ui64 txId) noexcept
        {
            if (more.Overflow) {
                LastRecord = nullptr;
                saver.Save(DataPageBuilder.Close(), GroupId);
            }

            if (!DataPageBuilder) {
                DataPageBuilder.Open(more.DataPageSize, PageSize, PageRows);
                DataPageBuilder.ExtraAs<TDataPage::TExtra>()->BaseRow = RowId;
            }

            Put(key, row, saver, finalRow, minVersion, maxVersion, txId, more.DataPageSize);

            if (txId == 0) {
                BlobRowId = ++RowId;
            }
        }

        void FlushDeltas() noexcept
        {
            DataPageBuilder.PushOffset(0);
            BlobRowId = ++RowId;
        }

        void Flush(ISaver &saver) noexcept
        {
            LastRecord = nullptr;
            if (auto flesh = DataPageBuilder.Close())
                saver.Save(flesh, GroupId);
        }

        void Reset() noexcept
        {
            BlobRowId = RowId = 0;
            DataPageBuilder.Reset();
        }

        void SetBlobRowId(TRowId rowId) noexcept
        {
            BlobRowId = rowId;
        }

        TRowId GetLastRowId() const noexcept
        {
            return RowId - 1;
        }

        NPage::TDataPage::TRecord& GetLastRecord() const noexcept
        {
            Y_ABORT_UNLESS(LastRecord != nullptr);
            return *LastRecord;
        }

    private:
        void Put(TCellsRef key, const TRowState& row, ISaver &saver, bool finalRow, TRowVersion minVersion, TRowVersion maxVersion, ui64 txId, TPgSize recordSize) noexcept
        {
            const bool isErased = !maxVersion.IsMax();
            const bool isVersioned = !minVersion.IsMin();
            const bool isDelta = txId != 0;

            if (isDelta) {
                Y_ABORT_UNLESS(!isErased && !isVersioned);
                DataPageBuilder.PushDelta(recordSize);
            } else {
                DataPageBuilder.PushOffset(recordSize);
            }

            auto &rec = DataPageBuilder.Place<NPage::TDataPage::TRecord>();

            for (const auto &info: GroupInfo.Columns) {
                DataPageBuilder.Place<NPage::TDataPage::TItem>().Flg = ui8(ECellOp::Empty);
                DataPageBuilder.Zero(info.FixedSize);
            }

            if (GroupId.Index == 0) {
                rec.SetFields(row.GetRowState(), isErased, isVersioned, isDelta);

                if (isErased) {
                    DataPageBuilder.Place<NPage::TDataPage::TVersion>().Set(maxVersion);
                }

                if (isVersioned) {
                    DataPageBuilder.Place<NPage::TDataPage::TVersion>().Set(minVersion);
                }

                if (isDelta) {
                    DataPageBuilder.Place<NPage::TDataPage::TDelta>().SetTxId(txId);
                }
            } else {
                rec.SetZero(); // We don't store flags in alternative groups
            }

            // Only the main group includes the key
            for (TPos i = 0; i < GroupInfo.ColsKeyData.size(); i++) {
                const TCell& val = key[i];
                auto &info = GroupInfo.ColsKeyData[i];
                if (!val.IsNull()) {
                    DataPageBuilder.AddValue(info, val, rec)->Null = false;
                } else {
                    rec.GetItem(info)->Null = true;
                }
            }

            for (const auto& pin : Pinout) {
                auto &info = GroupId.Historic ? Scheme->HistoryColumns[pin.From] : Scheme->AllColumns[pin.From];

                if (!row.IsFinalized(pin.To) || info.IsKey()) {

                } else if (row.GetCellOp(pin.To) == ECellOp::Reset) {
                    if (!finalRow)
                        rec.GetItem(info)->Flg = ui8(ECellOp::Reset);
                } else if (auto cell = row.Get(pin.To)) {
                    auto cellOp = row.GetCellOp(pin.To);

                    if (info.IsFixed /* may place only as ELargeObj::Inline */) {
                        Y_ABORT_UNLESS(cellOp == ELargeObj::Inline, "Got fixed non-inlined");

                        DataPageBuilder.AddValue(info, cell, rec)->Flg = *cellOp;
                    } else if (auto lob = SaveBlob(cellOp, pin.To, cell, saver, isDelta)) {
                        auto *item = rec.GetItem(info);

                        WriteUnaligned<ui64>(rec.template GetFixed<void>(item), lob.Ref);
                        item->Flg = *TCellOp{ cellOp, lob.Lob };
                    } else
                        DataPageBuilder.AddValue(info, cell, rec)->Flg = *cellOp;
                } else {
                    rec.GetItem(info)->Flg = ui8(ECellOp::Null);
                }
            }

            LastRecord = isDelta ? nullptr : &rec;
        }

        TLargeObj SaveBlob(TCellOp cellOp, ui32 pin, const TCell &cell, ISaver &saver, bool isDelta) noexcept
        {
            if (cellOp == ELargeObj::GlobId) {
                return saver.Save(BlobRowId, pin, cell.AsValue<NPageCollection::TGlobId>());
            } else if (cellOp != ELargeObj::Inline) {
                Y_Fail("Got an unexpected ELargeObj ref type " << int(ELargeObj(cellOp)));
            } else if (!isDelta && (IsLargeSize(cell.Size()) || IsSmallSize(cell.Size()))) {
                // FIXME: we cannot handle blob references during scans, so we
                //        avoid creating large objects when they are in deltas
                return saver.Save(BlobRowId, pin, { cell.Data(), cell.Size() });
            }

            return { };
        }

    private:
        bool IsLargeSize(TPgSize size) const noexcept {
            return size >= LargeEdge && size <= MaxLargeBlob;
        }

        bool IsSmallSize(TPgSize size) const noexcept {
            return size >= SmallEdge;
        }

    public:
        const TIntrusiveConstPtr<TPartScheme> Scheme;

    private:
        const TPgSize PageSize;
        const ui32 PageRows = Max<ui32>();
        const TPgSize SmallEdge;
        const TPgSize LargeEdge;
        const TPgSize MaxLargeBlob;
        const TPinout Pinout;
        const TGroupId GroupId;
        const TPartScheme::TGroupInfo& GroupInfo;
        TRowId RowId = 0;
        TRowId BlobRowId = 0;
        TDataPageBuilder DataPageBuilder;

        NPage::TDataPage::TRecord* LastRecord = nullptr;
    };


    class TFlatIndexWriter {
    public:
        TFlatIndexWriter(TIntrusiveConstPtr<TPartScheme> scheme, const TConf &conf, TGroupId groupId)
            : Scheme(std::move(scheme))
            , MinSize(conf.Groups[groupId.Index].IndexMin)
            , GroupId(groupId)
            , GroupInfo(Scheme->GetLayout(groupId))
            , DataPageBuilder(EPage::FlatIndex, groupId.IsMain() ? 3 : 2, false, 0 /* no extra data before block */)
        {

        }

        ui64 BytesUsed() const noexcept
        {
            return DataPageBuilder.BytesUsed();
        }

        TPgSize CalcSize(TCellsRef key) const noexcept
        {
            Y_ABORT_UNLESS(key.size() == GroupInfo.KeyTypes.size());

            TPgSize ret = TPgSizeOf<NPage::TFlatIndex::TRecord>::Value;
            ret += sizeof(TPgSize);

            ret += GroupInfo.IdxRecFixedSize;
            for (TPos it = 0; it < GroupInfo.ColsKeyIdx.size(); it++) {
                ret += GroupInfo.ColsKeyIdx[it].IsFixed ? 0 : key[it].Size();
            }

            return ret;
        }

        void Add(TPgSize more, TCellsRef key, TRowId row, TPageId page) noexcept
        {
            DataPageBuilder.Grow(more, MinSize, 1.42);

            return Put(key, row, page, more);
        }

        void Add(TCellsRef key, TRowId row, TPageId page) noexcept
        {
            return Add(CalcSize(key), key, row, page);
        }

        TSharedData Flush() noexcept
        {
            return DataPageBuilder.Close();
        }

        void Reset() noexcept
        {
            DataPageBuilder.Reset();
        }

    private:
        void Put(TCellsRef key, TRowId row, TPageId page, TPgSize recordSize) noexcept
        {
            DataPageBuilder.PushOffset(recordSize);

            auto &rec = DataPageBuilder.Place<NPage::TFlatIndex::TRecord>();
            rec.SetRowId(row);
            rec.SetPageId(page);

            for (const auto &info: GroupInfo.ColsKeyIdx) {
                DataPageBuilder.Place<NPage::TFlatIndex::TItem>().Null = true;
                DataPageBuilder.Zero(info.FixedSize);
            }

            for (TPos it = 0; it < GroupInfo.ColsKeyIdx.size(); it++) {
                if (const auto &val = key[it]) {
                    DataPageBuilder.AddValue(GroupInfo.ColsKeyIdx[it], val, rec)->Null = false;
                }
            }
        }

    public:
        const TIntrusiveConstPtr<TPartScheme> Scheme;

    private:
        const TPgSize MinSize;
        const TGroupId GroupId;
        const TPartScheme::TGroupInfo& GroupInfo;
        TDataPageBuilder DataPageBuilder;
    };

}
}
}
