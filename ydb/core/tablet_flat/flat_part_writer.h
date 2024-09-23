#pragma once

#include "flat_abi_evol.h"
#include "flat_page_btree_index_writer.h"
#include "flat_page_conf.h"
#include "flat_page_gstat.h"
#include "flat_page_txidstat.h"
#include "flat_page_writer.h"
#include "flat_page_other.h"
#include "flat_part_iface.h"
#include "flat_part_overlay.h"
#include "flat_part_slice.h"
#include "flat_part_laid.h"
#include "flat_row_state.h"
#include "flat_bloom_writer.h"
#include "util_fmt_abort.h"

#include <ydb/core/tablet_flat/protos/flat_table_part.pb.h>
#include <ydb/core/util/intrusive_heap.h>
#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NTable {

    class TPartWriter final : protected ISaver {
        using ECodec = NPage::ECodec;
        using ICodec = NBlockCodecs::ICodec;

        enum : size_t {
            GarbageStatsMaxSize = 100,
            GarbageStatsMaxBuildSize = 10000,
        };

    public:
        TPartWriter() = delete;
        TPartWriter(const TPartWriter&) = delete;

        TPartWriter(TIntrusiveConstPtr<TPartScheme> scheme, TTagsRef tags, IPageWriter& pager,
                        const NPage::TConf &conf, TEpoch epoch)
            : Final(conf.Final)
            , CutIndexKeys(conf.CutIndexKeys)
            , WriteBTreeIndex(conf.WriteBTreeIndex)
            , WriteFlatIndex(conf.WriteFlatIndex || !conf.WriteBTreeIndex)
            , SmallEdge(conf.SmallEdge)
            , LargeEdge(conf.LargeEdge)
            , MaxLargeBlob(conf.MaxLargeBlob)
            , Epoch(epoch)
            , SliceSize(conf.SliceSize)
            , MainPageCollectionEdge(conf.MainPageCollectionEdge)
            , SmallPageCollectionEdge(conf.SmallPageCollectionEdge)
            , UnderlayMask(conf.UnderlayMask)
            , SplitKeys(conf.SplitKeys)
            , MinRowVersion(conf.MinRowVersion)
            , Scheme(scheme)
            , Pager(pager)
            , FrameL(tags.size())
            , FrameS(tags.size())
            , EraseRowState(tags.size())
            , SchemeData(scheme->Serialize())
        {
            for (ui32 group : xrange(conf.Groups.size())) {
                Groups.emplace_back(scheme, conf, tags, NPage::TGroupId(group));
                Histories.emplace_back(scheme, conf, tags, NPage::TGroupId(group, true));
            }

            if (conf.ByKeyFilter) {
                if (MainPageCollectionEdge || SmallPageCollectionEdge || !conf.MaxRows) {
                    ByKey.Reset(new NBloom::TQueue(0.0001));
                } else {
                    ByKey.Reset(new NBloom::TWriter(conf.MaxRows, 0.0001));
                }
            }

            if (!Final && UnderlayMask) {
                UnderlayMask->Reset();
            }

            if (SplitKeys) {
                SplitKeys->Reset();
            }

            // This is used to write delayed erase markers
            EraseRowState.Touch(ERowOp::Erase);
        }

        void BeginKey(TCellsRef key) noexcept {
            Y_ABORT_UNLESS(Phase == 0, "BeginKey called after Finish");

            KeyState.Key = key;
            KeyState.LastVersion = TRowVersion::Max();
            KeyState.LastWritten = TRowVersion::Max();
            KeyState.RowId = Max<TRowId>();
            KeyState.WrittenDeltas = 0;
            KeyState.Written = 0;
            KeyState.Final = Final || (UnderlayMask && !UnderlayMask->HasKey(key));
            KeyState.DelayedErase = false;

            if (SplitKeys && SplitKeys->ShouldSplit(key) && NextSliceFirstRowId != Max<TRowId>()) {
                // Force a new slice on flush
                NextSliceForce = true;

                // Perform a forced flush of all data pages
                for (auto& g : Groups) {
                    g.Data.Flush(*this);
                }

                Y_DEBUG_ABORT_UNLESS(!NextSliceForce);
                Y_DEBUG_ABORT_UNLESS(NextSliceFirstRowId == Max<TRowId>());
            }
        }

        void AddKeyDelta(const TRowState& row, ui64 txId) noexcept
        {
            Y_ABORT_UNLESS(KeyState.Written == 0, "Cannot add deltas after committed versions");
            Y_ABORT_UNLESS(txId != 0, "Cannot add delta with txId == 0");

            WriteDeltaRow(row, txId);
        }

        void AddKeyVersion(const TRowState& row, TRowVersion version) noexcept
        {
            Y_DEBUG_ABORT_UNLESS(version < KeyState.LastVersion, "Key versions must be in descending order");

            if (row != ERowOp::Erase) {
                WriteRow(row, version, KeyState.DelayedErase ? KeyState.LastVersion : TRowVersion::Max());
                KeyState.LastWritten = version;
            }

            KeyState.LastVersion = version;
            KeyState.DelayedErase = row == ERowOp::Erase;
        }

        ui32 EndKey() noexcept
        {
            if (!KeyState.Final && KeyState.DelayedErase) {
                WriteRow(EraseRowState, KeyState.LastVersion, TRowVersion::Max());
                KeyState.LastWritten = KeyState.LastVersion;
                KeyState.DelayedErase = false;
            } else if (KeyState.WrittenDeltas && !KeyState.Written) {
                // We have written some deltas, but no committed versions
                // We need to properly flush uncommitted deltas
                FlushDeltaRows();
            }

            return KeyState.Written + KeyState.WrittenDeltas;
        }

        ERowOp AddRowLegacy(TCellsRef key, const TRowState& row) noexcept
        {
            BeginKey(key);
            AddKeyVersion(row, TRowVersion::Min());
            EndKey();

            if (KeyState.Written == 0) {
                return ERowOp::Absent;
            }

            return row.GetRowState();
        }

        TWriteStats Finish() noexcept
        {
            Flush(true);

            return std::move(WriteStats);
        }

    private:
        void WriteRow(const TRowState& row, TRowVersion minVersion, TRowVersion maxVersion) noexcept
        {
            if (KeyState.Written == 0) {
                WriteMainRow(row, minVersion, maxVersion);
            } else {
                WriteHistoryRow(row, minVersion, maxVersion);
            }

            ++KeyState.Written;
        }

        void WriteDeltaRow(const TRowState& row, ui64 txId) noexcept
        {
            Y_ABORT_UNLESS(Phase == 0, "WriteDeltaRow called after Finish");

            ui64 overheadBytes = 0;
            for (size_t groupIdx : xrange(Groups.size())) {
                auto& g = Groups[groupIdx];
                // N.B. non-main groups have no key
                TCellsRef groupKey = groupIdx == 0 ? KeyState.Key : TCellsRef{ };
                g.NextDataSize = g.Data.CalcSize(groupKey, row, KeyState.Final, TRowVersion::Min(), TRowVersion::Max(), txId);
                g.NextIndexSize = WriteFlatIndex ? g.FlatIndex.CalcSize(groupKey) : 0;
                g.NextBTreeIndexSize = WriteBTreeIndex ? g.BTreeIndex.CalcSize(groupKey) : 0;
                overheadBytes += (
                    g.NextDataSize.DataPageSize +
                    g.NextDataSize.SmallSize +
                    g.NextDataSize.LargeSize);
            }

            if (KeyState.WrittenDeltas == 0 && NeedFlush()) {
                Flush(false);

                // Next part would not have overflow
                for (auto& g : Groups) {
                    g.NextDataSize.Overflow = false;
                }
            }

            Current.TxIdStatsBuilder.AddRow(txId, overheadBytes);
            Current.DeltaRows += 1;

            // Uncommitted rows may we be committed using any version in the future
            Y_DEBUG_ABORT_UNLESS(MinRowVersion == TRowVersion::Min());
            Current.MinRowVersion = TRowVersion::Min();
            Current.MaxRowVersion = TRowVersion::Max();
            Current.Versioned = true;

            // Flush previous (possibly duplicate) row
            FrameS.FlushRow();
            FrameL.FlushRow();

            for (size_t groupIdx : xrange(Groups.size())) {
                auto& g = Groups[groupIdx];
                // N.B. non-main groups have no key
                TCellsRef groupKey = groupIdx == 0 ? KeyState.Key : TCellsRef{ };
                g.Data.Add(g.NextDataSize, groupKey, row, *this, KeyState.Final, TRowVersion::Min(), TRowVersion::Max(), txId);
            }

            ++KeyState.WrittenDeltas;
        }

        void FlushDeltaRows() noexcept
        {
            Y_ABORT_UNLESS(Phase == 0, "FlushDeltaRows called after Finish");

            for (size_t groupIdx : xrange(Groups.size())) {
                auto& g = Groups[groupIdx];
                g.Data.FlushDeltas();
            }

            Current.Rows += 1;

            FinishMainKey(false);
        }

        void WriteMainRow(const TRowState& row, TRowVersion minVersion, TRowVersion maxVersion) noexcept
        {
            Y_ABORT_UNLESS(Phase == 0, "WriteMainRow called after Finish");

            Y_DEBUG_ABORT_UNLESS(minVersion < maxVersion);

            ui64 overheadBytes = 0;
            for (size_t groupIdx : xrange(Groups.size())) {
                auto& g = Groups[groupIdx];
                // N.B. non-main groups have no key
                TCellsRef groupKey = groupIdx == 0 ? KeyState.Key : TCellsRef{ };
                g.NextDataSize = g.Data.CalcSize(groupKey, row, KeyState.Final, minVersion, maxVersion, /* txId */ 0);
                g.NextIndexSize = WriteFlatIndex ? g.FlatIndex.CalcSize(groupKey) : 0;
                g.NextBTreeIndexSize = WriteBTreeIndex ? g.BTreeIndex.CalcSize(groupKey) : 0;

                // FIXME: not each row produces index row so overhead bytes shouldn't add index size
                overheadBytes += (
                        g.NextDataSize.DataPageSize +
                        g.NextDataSize.SmallSize +
                        g.NextDataSize.LargeSize +
                        g.NextIndexSize +
                        g.NextBTreeIndexSize);
            }

            if (KeyState.WrittenDeltas == 0 && NeedFlush()) {
                Flush(false);

                // Next part would not have overflow
                for (auto& g : Groups) {
                    g.NextDataSize.Overflow = false;
                }
            }

            Current.Rows += 1;
            bool erased = (row == ERowOp::Erase || maxVersion < TRowVersion::Max());
            Current.Drops += erased ? 1 : 0;
            Current.HiddenRows += (maxVersion < TRowVersion::Max() ? 1 : 0);

            if (!Current.Versioned && (minVersion > TRowVersion::Min() || maxVersion < TRowVersion::Max())) {
                Current.Versioned = true;
            }

            Current.MinRowVersion = Min(Current.MinRowVersion, minVersion);
            Current.MaxRowVersion = Max(Current.MaxRowVersion, minVersion);
            if (maxVersion < TRowVersion::Max()) {
                Current.MaxRowVersion = Max(Current.MaxRowVersion, maxVersion);
            }

            Y_DEBUG_ABORT_UNLESS(minVersion >= MinRowVersion);
            if (minVersion == MinRowVersion) {
                // Don't waste bytes writing a statically known minimum version
                minVersion = TRowVersion::Min();
            }

            // Flush previous (possibly duplicate) row
            FrameS.FlushRow();
            FrameL.FlushRow();

            for (size_t groupIdx : xrange(Groups.size())) {
                auto& g = Groups[groupIdx];
                // N.B. non-main groups have no key
                TCellsRef groupKey = groupIdx == 0 ? KeyState.Key : TCellsRef{ };
                g.Data.Add(g.NextDataSize, groupKey, row, *this, KeyState.Final, minVersion, maxVersion, /* txId */ 0);
            }

            FinishMainKey(erased);

            if (maxVersion < TRowVersion::Max()) {
                // Count overhead bytes if everything up to maxVersion is removed
                Current.GarbageStatsBuilder.Add(maxVersion, overheadBytes);
                if (Current.GarbageStatsBuilder.Size() > GarbageStatsMaxBuildSize) {
                    Current.GarbageStatsBuilder.ShrinkTo(GarbageStatsMaxSize);
                }
            }
        }

        void FinishMainKey(bool erased) noexcept
        {
            KeyState.RowId = Groups[0].Data.GetLastRowId();

            if (ByKey) {
                ByKey->Add(KeyState.Key);
            }

            for (auto& g : Groups) {
                g.LastKeyIndexSize = g.NextIndexSize;
                if (!g.FirstKeyIndexSize) {
                    g.FirstKeyIndexSize = g.NextIndexSize;
                    g.FirstKeyBTreeIndexSize = g.NextBTreeIndexSize;
                }
            }

            if (NextSliceFirstRowId == Max<TRowId>()) {
                NextSliceFirstRowId = Groups[0].Data.GetLastRowId();
                NextSliceFirstKey = TSerializedCellVec(KeyState.Key);
            }

            if (erased) {
                Current.BTreeIndexErasedRowCount++;
            }
        }

        void WriteHistoryRow(const TRowState& row, TRowVersion minVersion, TRowVersion maxVersion) noexcept
        {
            Y_ABORT_UNLESS(Phase == 0, "WriteHistoryRow called after Finish");

            Y_DEBUG_ABORT_UNLESS(minVersion < maxVersion);

            // Mark main group row as having a history
            Groups[0].Data.GetLastRecord().MarkHasHistory();

            // We store minVersion as part of the key
            TCell syntheticKeyCells[3] = {
                TCell::Make(KeyState.RowId),
                TCell::Make(minVersion.Step),
                TCell::Make(minVersion.TxId),
            };
            TCellsRef syntheticKey{ syntheticKeyCells, 3 };

            ui64 overheadBytes = 0;
            for (size_t groupIdx : xrange(Histories.size())) {
                auto& g = Histories[groupIdx];
                // N.B. non-main groups have no key
                TCellsRef groupKey = groupIdx == 0 ? syntheticKey : TCellsRef{ };
                g.NextDataSize = g.Data.CalcSize(groupKey, row, KeyState.Final, TRowVersion::Min(), maxVersion, /* txId */ 0);
                g.NextIndexSize = WriteFlatIndex ? g.FlatIndex.CalcSize(groupKey) : 0;
                g.NextBTreeIndexSize = WriteBTreeIndex ? g.BTreeIndex.CalcSize(groupKey) : 0;

                // FIXME: not each row produces index row so overhead bytes shouldn't add index size
                overheadBytes += (
                        g.NextDataSize.DataPageSize +
                        g.NextDataSize.SmallSize +
                        g.NextDataSize.LargeSize +
                        g.NextIndexSize + 
                        g.NextBTreeIndexSize);
            }

            // When max version is not max there are 2 rows (one is a virtual drop)
            Current.HiddenRows += (maxVersion < TRowVersion::Max() ? 2 : 1);
            Current.HiddenDrops += (row == ERowOp::Erase || maxVersion < TRowVersion::Max() ? 1 : 0);

            Current.HistoryWritten += 1;
            Current.Versioned = true;

            Current.MinRowVersion = Min(Current.MinRowVersion, minVersion);
            Current.MaxRowVersion = Max(Current.MaxRowVersion, minVersion);
            if (maxVersion < TRowVersion::Max()) {
                Current.MaxRowVersion = Max(Current.MaxRowVersion, maxVersion);
            }

            // Flush previous (possibly duplicate) row
            FrameS.FlushRow();
            FrameL.FlushRow();

            for (size_t groupIdx : xrange(Histories.size())) {
                auto& g = Histories[groupIdx];
                // Use the main row id for saved blobs
                g.Data.SetBlobRowId(KeyState.RowId);
                // N.B. non-main groups have no key
                TCellsRef groupKey = groupIdx == 0 ? syntheticKey : TCellsRef{ };
                g.Data.Add(g.NextDataSize, groupKey, row, *this, KeyState.Final, TRowVersion::Min(), maxVersion, /* txId */ 0);
            }

            for (auto& g : Histories) {
                g.LastKeyIndexSize = g.NextIndexSize;
                if (!g.FirstKeyIndexSize) {
                    g.FirstKeyIndexSize = g.NextIndexSize;
                    g.FirstKeyBTreeIndexSize = g.NextBTreeIndexSize;
                }
            }

            // Count overhead bytes if everything up to LastWritten is removed
            Current.GarbageStatsBuilder.Add(KeyState.LastWritten, overheadBytes);
            if (Current.GarbageStatsBuilder.Size() > GarbageStatsMaxBuildSize) {
                Current.GarbageStatsBuilder.ShrinkTo(GarbageStatsMaxSize);
            }
        }

        bool NeedFlush() const noexcept
        {
            // Check if adding this row would overflow page collection size limits
            if (Current.Rows > 0) {
                if (SmallPageCollectionEdge != Max<ui64>()) {
                    ui64 smallPageCollectionSize = Current.SmallWritten;
                    for (auto& g : Groups) {
                        smallPageCollectionSize += g.NextDataSize.SmallSize;
                        smallPageCollectionSize += g.NextDataSize.NewSmallRefs * sizeof(NPage::TLabel);
                    }

                    if (smallPageCollectionSize > SmallPageCollectionEdge) {
                        return true;
                    }
                }

                if (MainPageCollectionEdge != Max<ui64>()) {
                    ui64 indexSize = 0;
                    ui32 smallRefs = 0;
                    ui32 largeRefs = 0;
                    for (auto& g : Groups) {
                        if (WriteFlatIndex) {
                            indexSize += g.FlatIndex.BytesUsed() + g.FirstKeyIndexSize;
                        }
                        if (WriteBTreeIndex) {
                            indexSize += g.BTreeIndex.EstimateBytesUsed() + g.FirstKeyBTreeIndexSize;
                        }
                        if (g.NextDataSize.Overflow) {
                            // On overflow we would have to start a new data page
                            // This would require a new entry in the index
                            indexSize += g.NextIndexSize;
                            indexSize += g.NextBTreeIndexSize;
                        }
                        smallRefs += g.NextDataSize.NewSmallRefs + g.NextDataSize.ReusedSmallRefs;
                        largeRefs += g.NextDataSize.NewLargeRefs + g.NextDataSize.ReusedLargeRefs;
                    }

                    // Main index always includes an entry for the last key
                    indexSize += Groups[0].NextIndexSize;

                    ui64 mainPageCollectionSize = Current.MainWritten
                            + Groups[0].Data.BytesUsed()
                            + Groups[0].NextDataSize.DataPageSize
                            + indexSize
                            + FrameS.EstimateBytesUsed(smallRefs)
                            + FrameL.EstimateBytesUsed(largeRefs)
                            + Globs.EstimateBytesUsed(largeRefs)
                            + (ByKey ? ByKey->EstimateBytesUsed(1) : 0)
                            + SchemeData.size();

                    // On overflow we would have to start a new data page
                    if (Groups[0].NextDataSize.Overflow) {
                        mainPageCollectionSize += Groups[0].Data.PrefixSize();
                    }

                    if (mainPageCollectionSize > MainPageCollectionEdge) {
                        return true;
                    }
                }
            }

            return false;
        }

        void Flush(bool last) noexcept
        {
            // The first group must write the last key
            Y_ABORT_UNLESS(std::exchange(Phase, 1) == 0, "Called twice");

            for (size_t i : xrange<size_t>(1, Groups.size())) {
                Groups[i].Data.Flush(*this);
            }
            for (auto& g : Histories) {
                g.Data.Flush(*this);
            }
            // Main index should have correct groups data size
            Groups[0].Data.Flush(*this);

            if (Current.Rows > 0) {
                Y_ABORT_UNLESS(Phase == 2, "Missed the last Save call");

                WriteStats.Rows += Current.Rows;
                WriteStats.Drops += Current.Drops;
                WriteStats.Bytes += Current.Bytes;
                WriteStats.Coded += Current.Coded;
                WriteStats.HiddenRows += Current.HiddenRows;
                WriteStats.HiddenDrops += Current.HiddenDrops;

                Current.FlatHistoricIndexes.clear();
                Current.FlatGroupIndexes.clear();
                Current.FlatIndex = Max<TPageId>();
                if (WriteFlatIndex) {
                    if (Current.HistoryWritten > 0) {
                        Current.FlatHistoricIndexes.reserve(Histories.size());
                        for (auto& g : Histories) {
                            Current.FlatHistoricIndexes.push_back(WritePage(g.FlatIndex.Flush(), EPage::FlatIndex));
                        }
                    }

                    if (Groups.size() > 1) {
                        Current.FlatGroupIndexes.reserve(Groups.size() - 1);
                        for (ui32 group : xrange(ui32(1), ui32(Groups.size()))) {
                            Current.FlatGroupIndexes.push_back(WritePage(Groups[group].FlatIndex.Flush(), EPage::FlatIndex));
                        }
                    }

                    Current.FlatIndex = WritePage(Groups[0].FlatIndex.Flush(), EPage::FlatIndex);
                }
                
                Current.BTreeGroupIndexes.clear();
                Current.BTreeHistoricIndexes.clear();
                if (WriteBTreeIndex) {
                    Current.BTreeGroupIndexes.reserve(Groups.size());
                    for (auto& g : Groups) {
                        Current.BTreeGroupIndexes.push_back(g.BTreeIndex.Finish(Pager));
                    }
                    if (Current.HistoryWritten > 0) {
                        Current.BTreeHistoricIndexes.reserve(Histories.size());
                        for (auto& g : Histories) {
                            Current.BTreeHistoricIndexes.push_back(g.BTreeIndex.Finish(Pager));
                        }
                    }
                }

                Current.Large = WriteIf(FrameL.Make(), EPage::Frames);
                Current.Small = WriteIf(FrameS.Make(), EPage::Frames);
                Current.Globs = WriteIf(Globs.Make(), EPage::Globs);
                if (ByKey) {
                    Current.ByKey = WriteIf(ByKey->Make(), EPage::Bloom);
                }

                if (Current.GarbageStatsBuilder) {
                    Current.GarbageStatsBuilder.ShrinkTo(GarbageStatsMaxSize);
                    Current.GarbageStats = WriteIf(Current.GarbageStatsBuilder.Finish(), EPage::GarbageStats);
                }

                if (Current.TxIdStatsBuilder) {
                    Current.TxIdStats = WriteIf(Current.TxIdStatsBuilder.Finish(), EPage::TxIdStats);
                }

                Current.Scheme = WritePage(SchemeData, EPage::Schem2);
                WriteInplace(Current.Scheme, MakeMetaBlob(last));

                Y_ABORT_UNLESS(Slices && *Slices, "Flushing bundle without a run");

                Pager.Finish(TOverlay{ nullptr, std::move(Slices) }.Encode());
                ++WriteStats.Parts;
            }

            if (!last) {
                for (auto& g : Groups) {
                    g.Data.Reset();
                    g.FlatIndex.Reset();
                    g.BTreeIndex.Reset();
                }
                for (auto& g : Histories) {
                    g.Data.Reset();
                    g.FlatIndex.Reset();
                    g.BTreeIndex.Reset();
                }
                FrameL.Reset();
                FrameS.Reset();
                Globs.Reset();
                if (ByKey) {
                    ByKey->Reset();
                }
                Slices.Reset();

                RegisteredGlobs.clear();

                for (auto& g : Groups) {
                    Y_ABORT_UNLESS(g.FirstKeyIndexSize == 0);
                    Y_ABORT_UNLESS(g.FirstKeyBTreeIndexSize == 0);
                    Y_ABORT_UNLESS(g.LastKeyIndexSize == 0);
                }

                NextSliceFirstRowId = Max<TRowId>();
                NextSliceFirstKey = { };
                LastSliceBytes = 0;

                Phase = 0;
                Current = { };

                Y_ABORT_UNLESS(!PrevPageLastKey);
                Y_ABORT_UNLESS(!PrevPageData);
            }
        }

        TString MakeMetaBlob(bool last) const noexcept
        {
            NProto::TRoot proto;

            proto.SetEpoch(Epoch.ToProto());

            if (auto *abi = proto.MutableEvol()) {
                ui32 head = ui32(NTable::ECompatibility::Head);

                if (Current.Small != Max<TPageId>())
                    head = Max(head, ui32(15) /* ELargeObj:Outer packed blobs */);

                if (!last || WriteStats.Parts > 0)
                    head = Max(head, ui32(20) /* Multiple part outputs */);

                if (!Current.FlatGroupIndexes.empty() || Current.BTreeGroupIndexes.size() > 1)
                    head = Max(head, ui32(26) /* Multiple column groups */);

                if (Current.Versioned)
                    head = Max(head, ui32(27) /* Versioned data present */);

                if (Current.TxIdStatsBuilder)
                    head = Max(head, ui32(28) /* Uncommitted deltas present */);

                abi->SetTail(head);
                abi->SetHead(ui32(NTable::ECompatibility::Edge));
            }

            if (auto *stat = proto.MutableStat()) {
                stat->SetBytes(Current.Bytes);
                stat->SetCoded(Current.Coded);
                stat->SetDrops(Current.Drops);
                stat->SetRows(Current.Rows);
                if (Current.HiddenRows > 0) {
                    stat->SetHiddenRows(Current.HiddenRows);
                    stat->SetHiddenDrops(Current.HiddenDrops);
                }
            }

            if (auto *lay = proto.MutableLayout()) {
                lay->SetScheme(Current.Scheme);

                if (Current.FlatIndex != Max<TPageId>())
                    lay->SetIndex(Current.FlatIndex);
                if (Current.Globs != Max<TPageId>())
                    lay->SetGlobs(Current.Globs);
                if (Current.Large != Max<TPageId>())
                    lay->SetLarge(Current.Large);
                if (Current.Small != Max<TPageId>())
                    lay->SetSmall(Current.Small);
                if (Current.ByKey != Max<TPageId>())
                    lay->SetByKey(Current.ByKey);

                for (TPageId page : Current.FlatGroupIndexes) {
                    lay->AddGroupIndexes(page);
                }
                for (TPageId page : Current.FlatHistoricIndexes) {
                    lay->AddHistoricIndexes(page);
                }

                if (WriteBTreeIndex) {
                    lay->SetBTreeIndexesFormatVersion(NPage::TBtreeIndexNode::FormatVersion);
                    for (bool history : {false, true}) {
                        for (auto meta : history ? Current.BTreeHistoricIndexes : Current.BTreeGroupIndexes) {
                            auto m = history ? lay->AddBTreeHistoricIndexes() : lay->AddBTreeGroupIndexes();
                            m->SetRootPageId(meta.GetPageId());
                            m->SetLevelCount(meta.LevelCount);
                            m->SetIndexSize(meta.IndexSize);
                            m->SetDataSize(meta.GetDataSize());
                            m->SetGroupDataSize(meta.GetGroupDataSize());
                            m->SetRowCount(meta.GetRowCount());
                            m->SetErasedRowCount(meta.GetErasedRowCount());
                        }
                    }
                }

                if (Current.GarbageStats != Max<TPageId>()) {
                    lay->SetGarbageStats(Current.GarbageStats);
                }

                if (Current.TxIdStats != Max<TPageId>()) {
                    lay->SetTxIdStats(Current.TxIdStats);
                }
            }

            // There must have been at least one row
            Y_DEBUG_ABORT_UNLESS(Current.MinRowVersion <= Current.MaxRowVersion);

            if (Current.Versioned) {
                if (Current.MinRowVersion) {
                    auto* p = proto.MutableMinRowVersion();
                    p->SetStep(Current.MinRowVersion.Step);
                    p->SetTxId(Current.MinRowVersion.TxId);
                }
                if (Current.MaxRowVersion) {
                    auto* p = proto.MutableMaxRowVersion();
                    p->SetStep(Current.MaxRowVersion.Step);
                    p->SetTxId(Current.MaxRowVersion.TxId);
                }
            } else {
                // Unversioned parts must have min/max equal to zero
                Y_DEBUG_ABORT_UNLESS(!Current.MinRowVersion);
                Y_DEBUG_ABORT_UNLESS(!Current.MaxRowVersion);
            }

            TString blob;
            Y_PROTOBUF_SUPPRESS_NODISCARD proto.SerializeToString(&blob);

            return blob;
        }

        TPageId WritePage(TSharedData page, EPage type, ui32 group = 0) noexcept
        {
            NSan::CheckMemIsInitialized(page.data(), page.size());

            if (group == 0) {
                Current.MainWritten += page.size();
            }

            return Pager.Write(std::move(page), type, group);
        }

        void WriteInplace(TPageId page, TArrayRef<const char> body) noexcept
        {
            NSan::CheckMemIsInitialized(body.data(), body.size());

            Pager.WriteInplace(page, std::move(body));
        }

        TPageId WriteIf(TSharedData page, EPage type) noexcept
        {
            return page ? WritePage(std::move(page), type) : Max<TPageId>();
        }

        void Save(TSharedData raw, NPage::TGroupId groupId) noexcept override
        {
            auto& g = groupId.Historic ? Histories[groupId.Index] : Groups[groupId.Index];

            if (groupId.IsMain()) {
                Y_ABORT_UNLESS(Phase < 2, "Called twice on Finish(...)");
            }
            Y_ABORT_UNLESS(raw, "Save(...) accepts only non-trivial blobs");

            if (auto dataPage = NPage::TDataPage(&raw)) {
                TSharedData keep; /* should preserve original data for Key */

                /* Need to extract first key from page. Just written key
                    columns may not hold EOp::Reset cells (and now this
                    isn't possible technically), thus there isn't required
                    TCellDefaults object for expanding defaults.
                 */

                Y_ABORT_UNLESS(dataPage->Count, "Invalid EPage::DataPage blob");
                TPgSize flatKeyIndexSize = g.FirstKeyIndexSize;

                if (groupId.IsMain()) {
                    Y_DEBUG_ABORT_UNLESS(NextSliceFirstRowId != Max<TRowId>());

                    InitKey(Key, dataPage->Record(0), groupId);

                    if (CutIndexKeys) {
                        CutKey(groupId);
                        flatKeyIndexSize = WriteFlatIndex ? g.FlatIndex.CalcSize(Key) : 0;
                    }
                } else if (groupId.Index == 0) {
                    InitKey(Key, dataPage->Record(0), groupId);
                } else {
                    Key.clear();
                }

                if (CutIndexKeys && groupId.IsMain()) {
                    InitKey(PrevPageLastKey, dataPage->Record(dataPage->Count - 1), groupId);
                    // Note: keep page alive while we need PrevPageLastKey bytes
                    PrevPageData = raw;
                }

                Current.Bytes += raw.size(); /* before encoding */

                if (g.Codec == NPage::ECodec::Plain) {
                    /* Encoding was not enabled, keep as is */
                } else if (keep = Encode(raw, g.Codec, g.ForceCompression)) {
                    std::swap(raw, keep);
                }

                Current.Coded += raw.size(); /* after encoding */

                auto page = WritePage(raw, EPage::DataPage, groupId.Index);

                if (WriteFlatIndex) {
                    // N.B. non-main groups have no key
                    Y_DEBUG_ABORT_UNLESS(g.FlatIndex.CalcSize(Key) == flatKeyIndexSize);

                    g.FlatIndex.Add(flatKeyIndexSize, Key, dataPage.BaseRow(), page);
                }

                if (WriteBTreeIndex) {
                    if (dataPage.BaseRow()) {
                        g.BTreeIndex.AddKey(Key);
                    }
                    if (groupId.IsMain()) {
                        g.BTreeIndex.AddChild({page, dataPage->Count, raw.size(), Current.BTreeGroupDataSize, Current.BTreeIndexErasedRowCount});
                        Current.BTreeGroupDataSize = 0;
                        Current.BTreeIndexErasedRowCount = 0;
                    } else {
                        g.BTreeIndex.AddShortChild({page, dataPage->Count, raw.size()});
                        // Note: group data size is approximate, includes only finished pages
                        Current.BTreeGroupDataSize += raw.size();
                    }
                    g.BTreeIndex.Flush(Pager);
                }

                // N.B. hack to save the last row/key for the main group
                // SliceSize is wrong, but it's a hack for tests right now
                if (groupId.IsMain() && (NextSliceForce || Phase == 1 || Current.Bytes - LastSliceBytes >= SliceSize)) {
                    NextSliceForce = false;

                    TRowId lastRowId = dataPage.BaseRow() + dataPage->Count - 1;
                    InitKey(Key, dataPage->Record(dataPage->Count - 1), groupId);

                    SaveSlice(lastRowId, TSerializedCellVec(Key));

                    if (Phase == 1) {
                        if (WriteFlatIndex) {
                            Y_DEBUG_ABORT_UNLESS(g.FlatIndex.CalcSize(Key) == g.LastKeyIndexSize);
                            g.FlatIndex.Add(g.LastKeyIndexSize, Key, lastRowId, page);
                        }
                        Y_ABORT_UNLESS(std::exchange(Phase, 2) == 1);
                        PrevPageLastKey.clear(); // new index will be started
                        PrevPageData = { };
                    }
                }

                g.FirstKeyIndexSize = 0;
                g.FirstKeyBTreeIndexSize = 0;
                g.LastKeyIndexSize = 0;
            }
        }

        TLargeObj Save(TRowId row, ui32 tag, const TGlobId &glob) noexcept override
        {
            return Register(row, tag, glob);
        }

        TLargeObj Save(TRowId row, ui32 tag, TArrayRef<const char> plain) noexcept override
        {
            if (plain.size() >= LargeEdge && plain.size() <= MaxLargeBlob) {
                auto blob = NPage::TLabelWrapper::WrapString(plain, EPage::Opaque, 0);
                ui64 ref = Globs.Size(); /* is the current blob index */

                Current.BTreeGroupDataSize += blob.size();

                return Register(row, tag, Pager.WriteLarge(std::move(blob), ref));

            } else if (plain.size() >= SmallEdge) {
                auto blob = NPage::TLabelWrapper::Wrap(plain, EPage::Opaque, 0);
                Current.Bytes += blob.size();
                Current.Coded += blob.size();

                FrameS.Put(row, tag, blob.size());

                Current.SmallWritten += blob.size();
                Current.BTreeGroupDataSize += blob.size();

                return { ELargeObj::Outer, Pager.WriteOuter(std::move(blob)) };

            } else {
                Y_Fail("Got ELargeObj blob " << plain.size() << "b out of limits"
                        << " { " << SmallEdge << "b, " << LargeEdge << "b }");
            }
        }

        TLargeObj Register(TRowId row, ui32 tag, const TGlobId &glob) noexcept
        {
            ui32 ref;

            auto it = RegisteredGlobs.find(glob.Logo);
            if (it != RegisteredGlobs.end()) {
                // It's ok to reuse, as long as the glob is on the same row and column
                Y_ABORT_UNLESS(row == it->second.Row && tag == it->second.Tag,
                    "Glob %s is on row %" PRIu64 " tag %" PRIu32 " but was on row %" PRIu64 " tag %" PRIu32,
                    glob.Logo.ToString().c_str(), row, tag, it->second.Row, it->second.Tag);

                ref = it->second.Ref;
            } else {
                FrameL.Put(row, tag, glob.Logo.BlobSize());

                ref = Globs.Put(glob);

                auto& registered = RegisteredGlobs[glob.Logo];
                registered.Row = row;
                registered.Tag = tag;
                registered.Ref = ref;
            }

            return { ELargeObj::Extern, ref };
        }

        TSharedData Encode(TArrayRef<const char> page, ECodec codec, bool force) noexcept
        {
            Y_ABORT_UNLESS(codec == ECodec::LZ4, "Only LZ4 encoding allowed");

            auto got = NPage::TLabelWrapper().Read(page, EPage::DataPage);

            Y_ABORT_UNLESS(got == ECodec::Plain, "Page is already encoded");
            Y_ABORT_UNLESS(got.Page.data() - page.data() == 16, "Page compression would change page header size");

            if (!CodecImpl) {
                CodecImpl = NBlockCodecs::Codec("lz4fast");
            }
            auto size = CodecImpl->MaxCompressedLength(got.Page);

            TSharedData out = TSharedData::Uninitialized(size + 16 /* label */);

            size = CodecImpl->Compress(got.Page, out.mutable_begin() + 16);

            auto trimmed = out.TrimBack(size + 16 /* label */);
            if (trimmed >= out.size()) {
                // Make a hard copy and avoid wasting space in caches
                out = TSharedData::Copy(out);
            }

            if (!force && out.size() + (page.size() >> 3) > page.size()) {
                return { }; /* Compressed page is almost the same in size */
            } else {
                auto label = ReadUnaligned<NPage::TLabel>(page.begin());

                Y_ABORT_UNLESS(label.IsExtended(), "Expected an extended label");

                auto ext = ReadUnaligned<NPage::TLabelExt>(page.begin() + 8);

                ext.Codec = ECodec::LZ4;

                WriteUnaligned<NPage::TLabel>(out.mutable_begin(), NPage::TLabel::Encode(label.Type, label.Format, out.size()));
                WriteUnaligned<NPage::TLabelExt>(out.mutable_begin() + 8, ext);

                NSan::CheckMemIsInitialized(out.data(), out.size());

                return out;
            }
        }

        void InitKey(TStackVec<TCell, 16>& key, const NPage::TDataPage::TRecord* record, NPage::TGroupId groupId) noexcept
        {
            const auto& layout = Scheme->GetLayout(groupId);
            key.resize(layout.ColsKeyData.size());
            for (const auto &info: layout.ColsKeyData) {
                key[info.Key] = record->Cell(info);
            }
        }

        void CutKey(NPage::TGroupId groupId) noexcept
        {
            if (!PrevPageLastKey) {
                return;
            }

            Y_ABORT_UNLESS(PrevPageLastKey.size() == Key.size());

            const auto& layout = Scheme->GetLayout(groupId);
            
            TPos it;
            for (it = 0; it < Key.size(); it++) {
                if (int cmp = CompareTypedCells(PrevPageLastKey[it], Key[it], layout.KeyTypes[it])) {
                    break;
                }
            }

            Y_ABORT_UNLESS(it < Key.size(), "All keys should be different");

            if (!layout.Columns[it].IsFixed && IsCharPointerType(layout.KeyTypes[it].GetTypeId())) {
                auto &prevCell = PrevPageLastKey[it];
                auto &cell = Key[it];

                Y_ABORT_UNLESS(!cell.IsNull(), "Keys should be in ascendic order");

                size_t index;
                for (index = 0; index < Min(prevCell.Size(), cell.Size()); index++) {
                    if (prevCell.AsBuf()[index] != cell.AsBuf()[index]) {
                        break;
                    }
                }

                index++; // last taken symbol

                if (layout.KeyTypes[it].GetTypeId() == NKikimr::NScheme::NTypeIds::Utf8) {
                    while (index < cell.Size() && ((u_char)cell.AsBuf()[index] >> 6) == 2) {
                        // skip tail character bits
                        index++;
                    }
                }

                if (index < cell.Size()) {
                    Key[it] = TCell(cell.Data(), index);
                }
            }

            for (it++; it < Key.size(); it++) {
                Key[it] = TCell();
            }
        }

        constexpr bool IsCharPointerType(NKikimr::NScheme::TTypeId typeId) {
            // Note: we don't cut Json/Yson/JsonDocument/DyNumber as will lead to invalid shard bounds
            switch (typeId) {
                case NKikimr::NScheme::NTypeIds::String:
                case NKikimr::NScheme::NTypeIds::String4k:
                case NKikimr::NScheme::NTypeIds::String2m:
                case NKikimr::NScheme::NTypeIds::Utf8:
                    return true;
            }

            return false;
        }

        void SaveSlice(TRowId lastRowId, TSerializedCellVec lastKey) noexcept
        {
            Y_ABORT_UNLESS(NextSliceFirstRowId != Max<TRowId>());
            Y_ABORT_UNLESS(NextSliceFirstRowId <= lastRowId);
            if (!Slices) {
                Slices = new TSlices;
            }
            Slices->emplace_back(
                std::move(NextSliceFirstKey),
                std::move(lastKey),
                NextSliceFirstRowId,
                lastRowId,
                true /* first key inclusive */,
                true /* last key inclusive */);
            NextSliceFirstRowId = Max<TRowId>();
            LastSliceBytes = Current.Bytes;
        }

    private:
        const bool Final = false;
        const bool CutIndexKeys;
        const bool WriteBTreeIndex;
        const bool WriteFlatIndex;
        const ui32 SmallEdge;
        const ui32 LargeEdge;
        const ui32 MaxLargeBlob;
        const TEpoch Epoch;
        const ui64 SliceSize;
        const ui64 MainPageCollectionEdge;
        const ui64 SmallPageCollectionEdge;
        NPage::IKeySpace* const UnderlayMask;
        NPage::ISplitKeys* const SplitKeys;
        const TRowVersion MinRowVersion;
        const TIntrusiveConstPtr<TPartScheme> Scheme;

        const ICodec *CodecImpl = nullptr;
        IPageWriter& Pager;
        NPage::TFrameWriter FrameL; /* Large blobs inverted index   */
        NPage::TFrameWriter FrameS; /* Packed blobs inverted index */
        NPage::TExtBlobsWriter Globs;
        THolder<NBloom::IWriter> ByKey;
        TWriteStats WriteStats;
        TStackVec<TCell, 16> Key;
        TStackVec<TCell, 16> PrevPageLastKey;
        TSharedData PrevPageData;
        ui32 Phase = 0; // 0 - writing rows, 1 - flushing current page collection, 2 - flushed current page collection

        struct TRegisteredGlob {
            TRowId Row;
            ui32 Tag;
            ui32 Ref;
        };

        THashMap<TLogoBlobID, TRegisteredGlob> RegisteredGlobs;

        struct TGroupState {
            const bool ForceCompression;
            const ECodec Codec;

            NPage::TDataPageWriter Data;
            NPage::TFlatIndexWriter FlatIndex;
            NPage::TBtreeIndexBuilder BTreeIndex;

            NPage::TDataPageWriter::TSizeInfo NextDataSize;
            TPgSize NextIndexSize;
            TPgSize NextBTreeIndexSize;

            TPgSize FirstKeyIndexSize = 0;
            TPgSize FirstKeyBTreeIndexSize = 0;
            TPgSize LastKeyIndexSize = 0;

            TGroupState(const TIntrusiveConstPtr<TPartScheme>& scheme, const NPage::TConf& conf, TTagsRef tags, NPage::TGroupId groupId)
                : ForceCompression(conf.Groups[groupId.Index].ForceCompression)
                , Codec(conf.Groups[groupId.Index].Codec)
                , Data(scheme, conf, tags, groupId)
                , FlatIndex(scheme, conf, groupId)
                , BTreeIndex(scheme, groupId, conf.Groups[groupId.Index].BTreeIndexNodeTargetSize, conf.Groups[groupId.Index].BTreeIndexNodeKeysMin, conf.Groups[groupId.Index].BTreeIndexNodeKeysMax)
            { }
        };

        TDeque<TGroupState> Groups;
        TDeque<TGroupState> Histories;

        struct TKeyState {
            TCellsRef Key;
            TRowVersion LastVersion = TRowVersion::Max();
            TRowVersion LastWritten = TRowVersion::Max();
            TRowId RowId = Max<TRowId>();
            ui32 WrittenDeltas = 0;
            ui32 Written = 0;
            bool Final = false;
            bool DelayedErase = false;
        };

        TKeyState KeyState;
        TRowState EraseRowState;

        struct TCurrent {
            ui64 Rows = 0;
            ui64 Drops = 0;
            ui64 Bytes = 0;
            ui64 Coded = 0;
            ui64 HiddenRows = 0;
            ui64 HiddenDrops = 0;
            ui64 BTreeIndexErasedRowCount = 0;
            ui64 BTreeGroupDataSize = 0;

            // doesn't include written B-Tree index bytes
            ui64 MainWritten = 0;
            ui64 SmallWritten = 0;

            ui64 HistoryWritten = 0;

            TVector<TPageId> FlatGroupIndexes;
            TVector<TPageId> FlatHistoricIndexes;
            TVector<NPage::TBtreeIndexMeta> BTreeGroupIndexes;
            TVector<NPage::TBtreeIndexMeta> BTreeHistoricIndexes; 
            TPageId FlatIndex = Max<TPageId>();
            TPageId Scheme = Max<TPageId>();
            TPageId Large = Max<TPageId>();
            TPageId Small = Max<TPageId>();
            TPageId Globs = Max<TPageId>();
            TPageId ByKey = Max<TPageId>();
            TPageId GarbageStats = Max<TPageId>();
            TPageId TxIdStats = Max<TPageId>();

            TRowVersion MinRowVersion = TRowVersion::Max();
            TRowVersion MaxRowVersion = TRowVersion::Min();

            NPage::TGarbageStatsBuilder GarbageStatsBuilder;

            NPage::TTxIdStatsBuilder TxIdStatsBuilder;
            ui64 DeltaRows = 0;

            bool Versioned = false;
        } Current;

        TIntrusivePtr<TSlices> Slices;

        const TSharedData SchemeData;

        TRowId NextSliceFirstRowId = Max<TRowId>();
        TSerializedCellVec NextSliceFirstKey;
        ui64 LastSliceBytes = 0;
        bool NextSliceForce = false;
    };

}}
