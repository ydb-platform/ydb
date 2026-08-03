#pragma once

#include "flat_scan_iface.h"
#include "flat_scan_spent.h"
#include "flat_writer_bundle.h"
#include "flat_sausage_chop.h"
#include "flat_row_misc.h"
#include "flat_part_writer.h"
#include "flat_part_loader.h"
#include "util_fmt_abort.h"
#include "util_fmt_logger.h"
#include "util_fmt_desc.h"
#include "util_basics.h"
#include "flat_comp.h"
#include "flat_executor_misc.h"
#include "flat_bio_stats.h"
#include "shared_cache_pages.h"
#include "shared_sausagecache.h"
#include "util_channel.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/fulltext.h>
#include <ydb/core/base/table_index.h>
#include <ydb/library/actors/core/actor.h>

#include <bitset>

namespace NKikimr {
namespace NTabletFlatExecutor {

    struct TProdCompact: public IDestructable {
        struct TResult {
            NTable::TPartView Part;
            TDeque<NTable::TScreen::THole> Growth;
        };

        using TResults = TDeque<TResult>;

        TProdCompact(bool ok, ui32 step, THolder<NTable::TCompactionParams> params,
                TVector<ui32>&& yellowMoveChannels,
                TVector<ui32>&& yellowStopChannels)
            : Success(ok)
            , Step(step)
            , Params(std::move(params))
            , YellowMoveChannels(std::move(yellowMoveChannels))
            , YellowStopChannels(std::move(yellowStopChannels))
        {

        }

        bool Success = false;
        std::exception_ptr Exception;
        ui32 Step = Max<ui32>();
        TResults Results;
        TVector<TIntrusiveConstPtr<NTable::TTxStatusPart>> TxStatus;
        THolder<NTable::TCompactionParams> Params;
        TVector<ui32> YellowMoveChannels;
        TVector<ui32> YellowStopChannels;
    };

    class TFulltextCompact {
        using ELockMode = NTable::ELockMode;
        using TScheme = NTable::TRowScheme;
        using TRow = NTable::TRowState;
        using TPartWriter = NTable::TPartWriter;

        // Deep-copy of a TRowState for fulltext buffering
        struct TSavedRow {
            NTable::ERowOp Op = NTable::ERowOp::Absent;
            TString Buf;
            struct TSlot { ui32 Offset = 0; ui32 Size = 0; NTable::TCellOp CellOp; };
            TVector<TSlot> Slots;

            void Save(const NTable::TRowState& row) {
                Op = row.GetRowState();
                Slots.resize(row.Size());
                Buf.clear();
                for (ui32 i = 0; i < row.Size(); i++) {
                    Slots[i].CellOp = row.GetCellOp(i);
                    const auto& cell = row.Get(i);
                    Slots[i].Offset = Buf.size();
                    if (!cell.IsNull()) {
                        Slots[i].Size = cell.Size();
                        Buf.append(cell.Data(), cell.Size());
                    } else {
                        Slots[i].Size = 0;
                    }
                }
            }

            void Restore(NTable::TRowState& out) const {
                out.Init(Slots.size());
                out.Touch(Op);
                if (Op == NTable::ERowOp::Erase || Op == NTable::ERowOp::Reset) {
                    return;
                }
                for (ui32 i = 0; i < Slots.size(); i++) {
                    if (Slots[i].CellOp != NTable::ECellOp::Empty) {
                        TCell cell;
                        if (Slots[i].Size > 0) {
                            cell = TCell(Buf.data() + Slots[i].Offset, Slots[i].Size);
                        }
                        out.Set(i, Slots[i].CellOp, cell);
                    }
                }
            }
        };

        // Per-key buffer for fulltext compaction
        struct TFtKeyBuf {
            // Part of the key (Gen + MaxId). Current token is stored outside
            NTableIndex::NFulltext::TGen Gen = 0;
            ui64 MaxId = 0;

            // Lock (if any)
            ELockMode LockMode = ELockMode::None;
            ui64 LockTxId = 0;

            // Deltas
            struct TDelta { ui64 TxId; TSavedRow Row; };
            TVector<TDelta> SavedDeltas;
            TVector<ui64> SavedDeltaOrder;

            // Committed versions (descending order)
            struct TVersion {
                TRowVersion Ver;
                TSavedRow Row;
                bool Added = true;
                TString Segment;
            };
            TVector<TVersion> Versions;

            bool IsMergeable(TRowVersion minVer) const {
                if (SavedDeltas.size() > 0 || LockMode != ELockMode::None || Versions.empty()) {
                    return false;
                }
                for (const auto& v : Versions) {
                    if (v.Ver > minVer) {
                        return false;
                    }
                }
                return true;
            }

            bool IsErased() const {
                return Versions.empty() || Versions[0].Row.Op == NTable::ERowOp::Erase;
            }
        };

        // Fulltext compaction state
        TSerializedCellVec FtCurrentPrefix;
        TString FtCurrentToken;
        TVector<TFtKeyBuf> FtTokenBuf;  // all keys for current token
        TFtKeyBuf FtCurKey;             // key currently being built
        ui32 PrefixSize = 0;            // prefix column count
        ui32 FtAddedPos = 0;            // position of __ydb_added in Scheme->Cols
        ui32 FtSegmentPos = 0;          // position of __ydb_segment in Scheme->Cols
        TVector<ui32> FtKeyColPos;      // keyOrder -> Cols position
        TRowVersion FtMinRowVersion;
        bool FtMergerStarted = false;
        ui64 AddedGen = 0;
        size_t AddedDeltas = 0;
        NFulltext::TMultiDeltaReader FtMerger;
        NFulltext::TDeltaWriter FtWriter;
        // We have to preserve delete markers (removed segments) on non-final compaction (!Conf->Params->IsFinal)
        ui64 RemovedGen = 0;
        size_t RemovedDeltas = 0;
        NFulltext::TMultiDeltaReader RemovedMerger;
        NFulltext::TDeltaWriter RemovedWriter;

        const TCompactCfg* Conf;
        TIntrusiveConstPtr<TScheme> Scheme;
        TIntrusivePtr<TPartWriter> Writer;

        TFulltextCompact(const TCompactCfg* conf, TIntrusiveConstPtr<TScheme> scheme, TIntrusivePtr<TPartWriter> writer):
            Conf(conf), Scheme(scheme), Writer(writer) {
            // Resolve column positions for fulltext columns
            auto* addedInfo = Scheme->ColInfo(Conf->FulltextAddedTag);
            auto* segmentInfo = Scheme->ColInfo(Conf->FulltextSegmentTag);
            FtAddedPos = addedInfo->Pos;
            FtSegmentPos = segmentInfo->Pos;
            // Build key column position map (keyOrder -> Cols position)
            for (const auto& col : Scheme->Cols) {
                if (col.IsKey()) {
                    if (col.Key >= FtKeyColPos.size()) {
                        FtKeyColPos.resize(col.Key + 1);
                    }
                    FtKeyColPos[col.Key] = col.Pos;
                }
            }
            Y_ENSURE(FtKeyColPos.size() >= 3);
            PrefixSize = FtKeyColPos.size()-3;
            FtMinRowVersion = Conf->Layout.MinRowVersion;
        }

    public:
        static std::unique_ptr<TFulltextCompact> Init(TCompactCfg* conf, TIntrusiveConstPtr<TScheme> scheme, TIntrusivePtr<TPartWriter> writer)
        {
            // Resolve column positions for fulltext columns
            auto* addedInfo = scheme->ColInfo(conf->FulltextAddedTag);
            auto* segmentInfo = scheme->ColInfo(conf->FulltextSegmentTag);
            if (addedInfo && segmentInfo) {
                return std::unique_ptr<TFulltextCompact>(new TFulltextCompact(conf, scheme, writer));
            }
            // Columns not found, fall back to normal compaction
            return nullptr;
        }

        void BeginKey(TArrayRef<const TCell> key)
        {
            // Check if token changed
            Y_ENSURE(key.size() == FtKeyColPos.size());
            TStringBuf newToken = !key[PrefixSize].IsNull() ? key[PrefixSize].AsBuf() : TStringBuf();
            TSerializedCellVec newPrefix(key.Slice(0, PrefixSize));
            if (newToken != FtCurrentToken || !TCellVectorsEquals{}(newPrefix.GetCells(), FtCurrentPrefix.GetCells())) {
                FlushFulltextToken();
                FtCurrentPrefix = std::move(newPrefix);
                FtCurrentToken = TString(newToken);
            }

            // Start buffering a new key (DON'T call Writer->BeginKey)
            FtCurKey = {};
            FtCurKey.Gen = key[PrefixSize+1].AsValue<NTableIndex::NFulltext::TGen>();
            FtCurKey.MaxId = Conf->FulltextKeySize == 8
                ? key[PrefixSize+2].AsValue<ui64>()
                : key[PrefixSize+2].AsValue<ui32>();
        }

        void SetLock(ELockMode mode, ui64 txId)
        {
            FtCurKey.LockMode = mode;
            FtCurKey.LockTxId = txId;
        }

        void SaveDeltas(THashMap<ui64, TRow>& Deltas, TSmallVec<ui64>& DeltasOrder)
        {
            if (!Deltas.empty()) {
                // FIXME: Maybe save as is
                for (ui64 txId : DeltasOrder) {
                    auto it = Deltas.find(txId);
                    Y_ENSURE(it != Deltas.end());
                    auto& d = FtCurKey.SavedDeltas.emplace_back();
                    d.TxId = txId;
                    d.Row.Save(it->second);
                }
                FtCurKey.SavedDeltaOrder = TVector<ui64>(DeltasOrder.begin(), DeltasOrder.end());
                Deltas.clear();
                DeltasOrder.clear();
            }
        }

        void SaveVersion(const TRow &row, TRowVersion &rowVersion)
        {
            auto& ver = FtCurKey.Versions.emplace_back();
            ver.Ver = rowVersion;
            ver.Row.Save(row);
            if (row.GetRowState() != NTable::ERowOp::Erase) {
                const auto& addedCell = row.Get(FtAddedPos);
                if (!addedCell.IsNull() && addedCell.Size() >= 1) {
                    ver.Added = *reinterpret_cast<const bool*>(addedCell.Data());
                }
                const auto& segCell = row.Get(FtSegmentPos);
                if (!segCell.IsNull()) {
                    ver.Segment = TString(segCell.Data(), segCell.Size());
                }
            }
        }

        void EndKey()
        {
            if (FtCurKey.Gen < std::numeric_limits<NTableIndex::NFulltext::TGen>::max()) {
                Y_ENSURE(!FtMergerStarted);
                if (Conf->Params->IsFinal && !FtCurKey.IsMergeable(FtMinRowVersion)) {
                    ReplayKey(FtCurKey);
                } else {
                    // Add current key to the per-token buffer
                    FtTokenBuf.push_back(std::move(FtCurKey));
                }
            } else {
                // Gen==MAX keys are usually always mergeable because they always originate
                // either from the index build process or from the previous compaction.
                // The user may enable a feature flag and insert them into the fulltext index table
                // manually, but then we assume he's OK with the fact he can easily break compaction
                // and knows what he does.
                if (FtTokenBuf.empty() || !FtCurKey.IsMergeable(FtMinRowVersion) || !Conf->Params->IsFinal && FtCurKey.IsErased()) {
                    // Abort compaction with non-mergeable or erased gen==max keys - the user is modifying the table manually!
                    // (We can't compact correctly because they may be interleaved with our output keys)
                    for (auto& key: FtTokenBuf) {
                        ReplayKey(key);
                    }
                    ReplayKey(FtCurKey);
                    FtTokenBuf.clear();
                } else if (FtCurKey.IsErased()) {
                    // Skip erased gen==max keys at the final level
                } else if (!Conf->Params->IsFinal) {
                    // Try to not break compaction if the user inserts valid rows with Gen==MAX:
                    // either replay keys as is or merge them into deltas.
                    FtTokenBuf.push_back(std::move(FtCurKey));
                } else {
                    StartFulltextMerge();
                    const auto& ver = FtCurKey.Versions[0];
                    FtMerger.Add(ver.Added, TConstArrayRef<ui8>((const ui8*)ver.Segment.data(), ver.Segment.size()));
                    // Try to merge up to MaxId
                    FtMerger.SetMaxId(FtCurKey.MaxId);
                    FtMerger.Start();
                    ui64 docId = 0;
                    ui32 freq = 0;
                    while (FtMerger.Read(docId, freq)) {
                        FtWriter.Add(docId, freq);
                        if (FtWriter.GetCount() >= Conf->FulltextMaxSegment) {
                            // Flush current segment
                            // It's the final level, so we don't have to preserve deltas here
                            WriteFulltextSegment(FtWriter, true, std::numeric_limits<NTableIndex::NFulltext::TGen>::max());
                            FtWriter.Reset(Conf->FulltextWithRelevance, Conf->FulltextKeySigned);
                        }
                    }
                    FtMerger.Stop();
                    FtMerger.SetMaxId(Conf->FulltextKeySigned ? INT64_MAX : UINT64_MAX);
                    FtMerger.Pop();
                    // Less than FulltextMaxSegment documents may be left in Writer
                }
            }
            FtCurKey = {};
        }

        // Replay a buffered key through Writer as-is (pass-through)
        void ReplayKey(const TFtKeyBuf& key) {
            TSmallVec<TCell> cells(FtCurrentPrefix.GetCells().begin(), FtCurrentPrefix.GetCells().end());
            cells.emplace_back(FtCurrentToken);
            cells.emplace_back(TCell::Make(key.Gen));
            cells.emplace_back((const char*)&key.MaxId, Conf->FulltextKeySize);
            Writer->BeginKey(cells);

            if (key.LockMode != ELockMode::None) {
                Writer->AddKeyLock(key.LockMode, key.LockTxId);
            }

            for (ui64 txId : key.SavedDeltaOrder) {
                for (const auto& d : key.SavedDeltas) {
                    if (d.TxId == txId) {
                        NTable::TRowState rs;
                        d.Row.Restore(rs);
                        Writer->AddKeyDelta(rs, txId);
                        break;
                    }
                }
            }

            for (const auto& v : key.Versions) {
                NTable::TRowState rs;
                v.Row.Restore(rs);
                TRowVersion ver = v.Ver;
                Writer->AddKeyVersion(rs, ver);
            }

            Writer->EndKey();
        }

        // Write a single merged fulltext segment through Writer
        void WriteFulltextSegment(const NFulltext::TDeltaWriter& wr, bool added, NTableIndex::NFulltext::TGen gen) {
            ui64 maxId = wr.GetMaxId();

            // Build key cells: (token, gen, maxId) — gen before maxId
            TSmallVec<TCell> keyCells(FtCurrentPrefix.GetCells().begin(), FtCurrentPrefix.GetCells().end());
            keyCells.push_back(TCell(FtCurrentToken.data(), FtCurrentToken.size()));
            keyCells.push_back(TCell::Make(gen));
            keyCells.push_back(TCell((const char*)&maxId, Conf->FulltextKeySize));

            // Build row state with ALL columns
            NTable::TRowState rs(Scheme->Cols.size());
            rs.Touch(NTable::ERowOp::Upsert);

            // Set key columns from key cells
            for (size_t k = 0; k < keyCells.size() && k < FtKeyColPos.size(); k++) {
                rs.Set(FtKeyColPos[k], NTable::ECellOp::Set, keyCells[k]);
            }

            // Set __ydb_added
            rs.Set(FtAddedPos, NTable::ECellOp::Set, TCell::Make(added));

            // Set __ydb_segment = merged segment data
            auto segBuf = wr.GetBuf();
            rs.Set(FtSegmentPos, NTable::ECellOp::Set, TCell((const char*)segBuf.data(), segBuf.size()));

            Writer->BeginKey(keyCells);
            Writer->AddKeyVersion(rs, FtMinRowVersion);
            Writer->EndKey();
        }

        void StartFulltextMerge() {
            if (!FtMergerStarted) {
                FtMergerStarted = true;
                FtMerger.Reset(Conf->FulltextWithRelevance, Conf->FulltextKeySigned);
                RemovedMerger.Reset(Conf->FulltextWithRelevance, Conf->FulltextKeySigned);
                AddedDeltas = 0;
                RemovedDeltas = 0;
                AddedGen = std::numeric_limits<NTableIndex::NFulltext::TGen>::max();
                RemovedGen = std::numeric_limits<NTableIndex::NFulltext::TGen>::max();
                for (const auto& key: FtTokenBuf) {
                    if (!key.IsMergeable(FtMinRowVersion)) {
                        // Non-final mode, non-mergeable keys will be interleaved with AddedGen/RemovedGen
                        continue;
                    }
                    const auto& ver = key.Versions[0];
                    if (ver.Row.Op == NTable::ERowOp::Erase || ver.Segment.empty()) {
                        // Skip erased keys
                        continue;
                    }
                    if (Conf->Params->IsFinal || ver.Added) {
                        FtMerger.Add(ver.Added, TConstArrayRef<ui8>((const ui8*)ver.Segment.data(), ver.Segment.size()));
                        if (!Conf->Params->IsFinal && AddedGen > key.Gen) {
                            AddedGen = key.Gen;
                        }
                        AddedDeltas++;
                    } else {
                        RemovedMerger.Add(true, TConstArrayRef<ui8>((const ui8*)ver.Segment.data(), ver.Segment.size()));
                        if (RemovedGen > key.Gen) {
                            RemovedGen = key.Gen;
                        }
                        RemovedDeltas++;
                    }
                }
                if (Conf->Params->IsFinal) {
                    RemovedGen = AddedGen = std::numeric_limits<NTableIndex::NFulltext::TGen>::max();
                } else if (AddedDeltas > 0 && RemovedDeltas > 0 && AddedGen < RemovedGen) {
                    // Make sure AddedGen is always > RemovedGen -- because if we have only 1
                    // added segment with gen=max on non-final level, we want to preserve its gen=max
                    auto tmp = AddedGen;
                    AddedGen = RemovedGen;
                    RemovedGen = tmp;
                }
                FtWriter.Reset(Conf->FulltextWithRelevance, Conf->FulltextKeySigned);
                RemovedWriter.Reset(Conf->FulltextWithRelevance, Conf->FulltextKeySigned);
            }
        }

        // Flush all buffered keys for the current token
        void FlushFulltextToken() {
            if (FtTokenBuf.empty()) {
                return;
            }

            // Flush remaining delta-lists
            StartFulltextMerge();
            for (const auto& key: FtTokenBuf) {
                if (!key.IsMergeable(FtMinRowVersion) && key.Gen < RemovedGen && key.Gen < AddedGen) {
                    ReplayKey(key);
                }
            }
            if (RemovedDeltas > 0) {
                RemovedMerger.Start();
                FlushSegments(RemovedMerger, RemovedWriter, false, RemovedGen);
            }
            for (const auto& key: FtTokenBuf) {
                if (!key.IsMergeable(FtMinRowVersion) && key.Gen > RemovedGen && key.Gen < AddedGen) {
                    ReplayKey(key);
                }
            }
            if (AddedDeltas > 0) {
                FtMerger.Start();
                FlushSegments(FtMerger, FtWriter, true, AddedGen);
            }
            for (const auto& key: FtTokenBuf) {
                if (!key.IsMergeable(FtMinRowVersion) && key.Gen > AddedGen) {
                    ReplayKey(key);
                }
            }

            FtMergerStarted = false;
            FtMerger.Reset(Conf->FulltextWithRelevance, Conf->FulltextKeySigned);
            FtWriter.Reset(Conf->FulltextWithRelevance, Conf->FulltextKeySigned);
            RemovedMerger.Reset(Conf->FulltextWithRelevance, Conf->FulltextKeySigned);
            RemovedWriter.Reset(Conf->FulltextWithRelevance, Conf->FulltextKeySigned);
            FtTokenBuf.clear();
        }

        void FlushSegments(NFulltext::TMultiDeltaReader& merger, NFulltext::TDeltaWriter& writer, bool added, NTableIndex::NFulltext::TGen gen) {
            ui64 docId = 0;
            ui32 freq = 0;
            while (merger.Read(docId, freq)) {
                writer.Add(docId, freq);
                if (writer.GetCount() >= Conf->FulltextMaxSegment) {
                    // Flush current segment
                    WriteFulltextSegment(writer, added, gen);
                    writer.Reset(Conf->FulltextWithRelevance, Conf->FulltextKeySigned);
                }
            }
            if (writer.GetCount() > 0) {
                WriteFulltextSegment(writer, added, gen);
            }
        }
    };

    class TOpsCompact: private ::NActors::IActorCallback, public IActorExceptionHandler, public NTable::IVersionScan {
        using TEvPut = TEvBlobStorage::TEvPut;
        using TEvPutResult = TEvBlobStorage::TEvPutResult;
        using ELockMode = NTable::ELockMode;
        using TScheme = NTable::TRowScheme;
        using TPartWriter = NTable::TPartWriter;
        using TBundle = NWriter::TBundle;
        using TStorage = TIntrusivePtr<TTabletStorageInfo>;
        using TEventHandlePtr = TAutoPtr<::NActors::IEventHandle>;
        using ELnLev = NUtil::ELnLev;

    public:
        constexpr static ui64 MaxFlight = 20ll * (1ll << 20);

        TOpsCompact(TActorId owner, TLogoBlobID mask, TAutoPtr<TCompactCfg> conf)
            : ::NActors::IActorCallback(static_cast<TReceiveFunc>(&TOpsCompact::Inbox), NKikimrServices::TActivity::OPS_COMPACT_A)
            , Mask(mask)
            , Owner(owner)
            , Conf(std::move(conf))
        {
            Bundle = new TBundle(Mask, Conf->Writer);
        }

        ~TOpsCompact()
        {
        }

        void Describe(IOutputStream &out) const override
        {
            out
                << "Compact{" << Mask.TabletID()
                << "." << Mask.Generation()
                << "." << Mask.Step()
                << ", eph " << Conf->Epoch
                << "}";
        }

    private:
        void Registered(TActorSystem *sys, const TActorId&) override
        {
            Logger = new NUtil::TLogger(sys, NKikimrServices::OPS_COMPACT);
        }

        TInitialState Prepare(IDriver *driver, TIntrusiveConstPtr<TScheme> scheme) override
        {
            TActivationContext::AsActorContext().RegisterWithSameMailbox(this);

            Spent = new TSpent(TAppData::TimeProvider.Get());
            Registry = AppData()->TypeRegistry;
            SharedCachePages = AppData()->SharedCachePages.Get();
            Scheme = std::move(scheme);
            Driver = driver;

            NTable::IScan::TConf conf;

            conf.NoErased = false; /* emit erase markers */
            conf.LargeEdge = Conf->Layout.LargeEdge;

            return { EScan::Feed, conf };
        }

        EScan Seek(TLead &lead, ui64 seq) override
        {
            if (seq == 0) /* on first Seek() init compaction */ {
                Y_ENSURE(!Writer, "Initial IScan::Seek(...) called twice");

                const auto tags = Scheme->Tags();

                lead.To(tags, { }, NTable::ESeek::Lower);

                auto *scheme = new NTable::TPartScheme(Scheme->Cols);

                Writer = new TPartWriter(scheme, tags, *Bundle, Conf->Layout, Conf->Epoch);

                if (Conf->IsFulltextCompact) {
                    FtState = TFulltextCompact::Init(Conf.Get(), Scheme, Writer);
                }

                return EScan::Feed;

            } else if (seq == 1) /* after the end(), stop compaction */ {
                // Flush remaining fulltext buffer before finishing
                if (FtState) {
                    FtState->FlushFulltextToken();
                }

                if (!Finished) {
                    WriteStats = Writer->Finish();
                    Results = Bundle->Results();
                    Y_ENSURE(WriteStats.Parts == Results.size());
                    WriteTxStatus();
                    Finished = true;
                }

                return Flush(true /* final flush, sleep or finish */);
            } else {
                Y_TABLET_ERROR("Compaction scan op should get only two Seeks()");
            }
        }

        EScan BeginKey(TArrayRef<const TCell> key) override
        {
            if (FtState) {
                FtState->BeginKey(key);
                if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                    logl
                        << NFmt::Do(*this) << " ft begin key { "
                        << NFmt::TCells(key, *Scheme->Keys, Registry)
                        << "}";
                }
                Y_DEBUG_ABORT_UNLESS(!IsLocked);
                return Flush(false);
            }

            Writer->BeginKey(key);

            if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                logl
                    << NFmt::Do(*this) << " begin key { "
                    << NFmt::TCells(key, *Scheme->Keys, Registry)
                    << "}";
            }

            Y_DEBUG_ABORT_UNLESS(!IsLocked);
            return Flush(false /* intermediate, sleep or feed */);
        }

        EScan BeginDeltas() override
        {
            if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                logl << NFmt::Do(*this) << " begin deltas";
            }

            return Flush(false /* intermediate, sleep or feed */);
        }

        EScan Feed(const TRow &row, ui64 txId) override
        {
            if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                logl << NFmt::Do(*this) << " feed row { ";

                if (row.GetRowState() == NTable::ERowOp::Erase) {
                    logl << "erased";
                } else {
                    logl << NFmt::TCells(*row, *Scheme->RowCellDefaults, Registry);
                }

                logl << " txId " << txId << " }";
            }

            // Note: we assume the number of uncommitted transactions is limited
            auto res = Deltas.try_emplace(txId, row);
            if (res.second) {
                DeltasOrder.emplace_back(txId);
            } else if (!res.first->second.IsFinalized()) {
                res.first->second.Merge(row);
            }

            return Flush(false /* intermediate, sleep or feed */);
        }

        EScan Feed(ELockMode mode, ui64 txId) override
        {
            if (FtState) {
                // Buffer lock for later replay
                if (!IsLocked) {
                    FtState->SetLock(mode, txId);
                    IsLocked = true;
                }
                return Flush(false);
            }

            // We write the first (latest) lock we observe
            if (!IsLocked) {
                Writer->AddKeyLock(mode, txId);
                IsLocked = true;
            }

            return Flush(false /* intermediate, sleep or feed */);
        }

        EScan EndDeltas() override
        {
            if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                logl << NFmt::Do(*this) << " end deltas";
            }

            if (FtState) {
                // Save accumulated deltas to the key buffer
                FtState->SaveDeltas(Deltas, DeltasOrder);
                return Flush(false);
            }

            if (!Deltas.empty()) {
                if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                    logl << NFmt::Do(*this) << " flushing " << Deltas.size() << " deltas";
                }

                for (ui64 txId : DeltasOrder) {
                    auto it = Deltas.find(txId);
                    Y_ENSURE(it != Deltas.end(), "Unexpected failure to find txId " << txId);
                    Writer->AddKeyDelta(it->second, txId);
                }

                Deltas.clear();
                DeltasOrder.clear();
            }

            return Flush(false /* intermediate, sleep or feed */);
        }

        EScan Feed(const TRow &row, TRowVersion &rowVersion) override
        {
            if (Conf->RemovedRowVersions) {
                // Adjust rowVersion so removed versions become compacted
                rowVersion = Conf->RemovedRowVersions.AdjustDown(rowVersion);
            }

            if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                logl << NFmt::Do(*this) << " feed row { ";

                if (row.GetRowState() == NTable::ERowOp::Erase) {
                    logl << "erased";
                } else {
                    logl << NFmt::TCells(*row, *Scheme->RowCellDefaults, Registry);
                }

                logl << " at " << rowVersion << " }";
            }

            if (FtState) {
                FtState->SaveVersion(row, rowVersion);
                return Flush(false);
            }

            Writer->AddKeyVersion(row, rowVersion);

            return Flush(false /* intermediate, sleep or feed */);
        }

        EScan EndKey() override
        {
            if (FtState) {
                FtState->EndKey();
                IsLocked = false;

                if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                    logl << NFmt::Do(*this) << " ft end key { buffered for token }";
                }
                return Flush(false);
            }

            ui32 written = Writer->EndKey();

            if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                logl << NFmt::Do(*this) << " end key { written " << written << " row versions }";
            }

            IsLocked = false;

            return Flush(false /* intermediate, sleep or feed */);
        }

        void WriteTxStatus()
        {
            if (!Conf->Frozen && !Conf->TxStatus) {
                // Nothing to compact
            }

            absl::flat_hash_map<ui64, std::optional<TRowVersion>> status;
            auto mergeStatus = [&](ui64 txId, const std::optional<TRowVersion>& version) {
                if (Conf->GarbageTransactions.Contains(txId)) {
                    // We don't write garbage transactions
                    return;
                }
                auto it = status.find(txId);
                if (it == status.end()) {
                    status[txId] = version;
                } else if (version) {
                    if (!it->second) {
                        // commit wins over remove
                        it->second = version;
                    } else if (*version < *it->second) {
                        // lowest commit version wins
                        it->second = version;
                    }
                }
            };

            for (const auto& memTable : Conf->Frozen) {
                for (const auto& pr : memTable->GetCommittedTransactions()) {
                    mergeStatus(pr.first, pr.second);
                }
                for (const ui64 txId : memTable->GetRemovedTransactions()) {
                    mergeStatus(txId, std::nullopt);
                }
            }
            for (const auto& txStatus : Conf->TxStatus) {
                for (const auto& item : txStatus->TxStatusPage->GetCommittedItems()) {
                    mergeStatus(item.GetTxId(), item.GetRowVersion());
                }
                for (const auto& item : txStatus->TxStatusPage->GetRemovedItems()) {
                    mergeStatus(item.GetTxId(), std::nullopt);
                }
            }

            if (status.empty()) {
                // Nothing to write
                return;
            }

            NTable::NPage::TTxStatusBuilder builder;
            for (const auto& pr : status) {
                if (pr.second) {
                    builder.AddCommitted(pr.first, *pr.second);
                } else {
                    builder.AddRemoved(pr.first);
                }
            }

            auto data = builder.Finish();
            if (!data) {
                // Don't write an empty page
                return;
            }

            auto dataId = Bundle->WriteExtra(data);

            if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                logl << NFmt::Do(*this) << " written tx status " << dataId.Lead << " size=" << dataId.Bytes;
            }

            TxStatus.emplace_back(new NTable::TTxStatusPartStore(dataId, Conf->Epoch, data));
        }

        TAutoPtr<IDestructable> Finish(EStatus status) override
        {
            const auto fail = Failed || !Finished || status != EStatus::Done;

            auto *prod = new TProdCompact(!fail, Mask.Step(), std::move(Conf->Params),
                    std::move(YellowMoveChannels), std::move(YellowStopChannels));
            if (status == EStatus::Exception) {
                prod->Exception = std::current_exception();
            }

            if (fail) {
                Results.clear(); /* shouldn't sent w/o fixation in bs */
            }

            for (auto &result : Results) {
                Y_ENSURE(result.PageCollections, "Compaction produced a part without page collections");
                TVector<TIntrusivePtr<TPrivatePageCache::TPageCollection>> resultingPageCollections;
                for (auto& pageCollection : result.PageCollections) {
                    auto resultingPageCollection = MakeIntrusive<NTable::TLoader::TPageCollection>(pageCollection.PageCollection);
                    auto saveCompactedPages = MakeHolder<NSharedCache::TEvSaveCompactedPages>(pageCollection.PageCollection);
                    auto gcList = SharedCachePages->GCList;
                    auto addPage = [&saveCompactedPages, &pageCollection, &resultingPageCollection, &gcList](NPageCollection::TLoadedPage& loadedPage, bool sticky) {
                        auto pageId = loadedPage.PageId;
                        auto pageSize = pageCollection.PageCollection->Page(pageId).Size;
                        auto sharedPage = MakeIntrusive<TPage>(pageId, pageSize, nullptr);
                        sharedPage->ProvideBody(std::move(loadedPage.Data));
                        saveCompactedPages->Pages.push_back(sharedPage);
                        if (sticky) {
                            resultingPageCollection->AddStickyPage(pageId, TSharedPageRef::MakeUsed(std::move(sharedPage), gcList));
                        } else {
                            resultingPageCollection->AddPage(pageId, TSharedPageRef::MakeUsed(std::move(sharedPage), gcList));
                        }
                    };
                    for (auto &page : pageCollection.StickyPages) {
                        addPage(page, true);
                    }
                    for (auto &page : pageCollection.RegularPages) {
                        addPage(page, false);
                    }

                    Send(MakeSharedPageCacheId(), saveCompactedPages.Release());

                    resultingPageCollections.push_back(std::move(resultingPageCollection));
                }

                NTable::TLoader loader(
                    std::move(resultingPageCollections),
                    { },
                    std::move(result.Overlay));

                // do not preload index as it may be already offloaded
                auto fetch = loader.Run({.PreloadIndex = false, .PreloadData = false});

                if (Y_UNLIKELY(fetch)) {
                    TStringBuilder error;
                    error << "Just compacted part needs to load page collection " << fetch.PageCollection->Label() << " pages";
                    for (auto page : fetch.Pages) {
                        error << " " << page;
                    }
                    Y_TABLET_ERROR(error);
                }

                auto& res = prod->Results.emplace_back();
                res.Part = loader.Result();
                res.Growth = std::move(result.Growth);
                Y_ENSURE(res.Part, "Unexpected result without a part after compaction");
            }

            prod->TxStatus = std::move(TxStatus);

            if (auto logl = Logger->Log(fail ? ELnLev::Error : ELnLev::Info)) {
                auto raito = WriteStats.Bytes ? (WriteStats.Coded + 0.) / WriteStats.Bytes : 0.;

                logl
                    << NFmt::Do(*this) << " end=" << status
                    << ", " << Blobs << " blobs " << WriteStats.Rows << "r"
                    << " (max " << Conf->Layout.MaxRows << ")"
                    << ", put " << NFmt::If(Spent.Get());

                for (const auto &result : prod->Results) {
                    if (auto *part = result.Part.As<NTable::TPartStore>()) {
                        auto lobs = part->Blobs ? part->Blobs->Total() : 0;
                        auto small = part->Small ? part->Small->Stats().Size : 0;
                        auto large = part->Large ? part->Large->Stats().Size : 0;
                        auto grow = NTable::TScreen::Sum(result.Growth);

                        logl
                            << " Part{ " << part->PageCollections.size() << " pk"
                            << ", lobs " << (lobs - grow) << " +" << grow
                            << ", (" << part->DataSize()
                                << " " << small << " " << large <<")b"
                            << " }";
                    }
                }

                if (prod->Results) {
                    logl << ", ecr=" << Sprintf("%.3f", raito);
                }

                for (const auto &txStatus : prod->TxStatus) {
                    logl << " TxStatus{ " << txStatus->Label << " }";
                }
            }

            if (fail) {
                Y_ENSURE(!prod->Results); /* shouldn't sent w/o fixation in bs */
            } else if (bool(prod->Results) != bool(WriteStats.Rows > 0)) {
                Y_TABLET_ERROR("Unexpected rows production result after compaction");
            } else if ((bool(prod->Results) || bool(prod->TxStatus)) != bool(Blobs > 0)) {
                Y_TABLET_ERROR("Unexpected blobs production result after compaction");
            }

            Driver = nullptr;

            PassAway();

            return prod;
        }

        bool OnUnhandledException(const std::exception& exc) override
        {
            if (!Driver) {
                return false;
            }
            Driver->Throw(exc);
            return true;
        }

        EScan Flush(bool last)
        {
            for (NPageCollection::TGlob& one : Bundle->GetBlobsToSave())
                FlushToBs(std::move(one));

            EScan scan = EScan::Sleep;

            if (last) {
                scan = (Flushing > 0 ? EScan::Sleep : EScan::Final);
            } else {
                scan = (Flushing >= MaxFlight ? EScan::Sleep : EScan::Feed);
            }

            Spent->Alter(scan != EScan::Sleep);

            return scan;
        }

        void Inbox(TEventHandlePtr &eh)
        {
            if (auto *ev = eh->CastAsLocal<TEvPutResult>()) {
                Handle(*ev);
            } else if (eh->CastAsLocal<TEvents::TEvUndelivered>()) {
                if (auto logl = Logger->Log(ELnLev::Error)) {
                    logl
                        << NFmt::Do(*this) << " cannot send put event to BS";
                }

                if (!std::exchange(Failed, true))
                    Driver->Touch(EScan::Final);
            } else {
                Y_TABLET_ERROR("Compaction actor got an unexpected event");
            }
        }

        void Handle(TEvPutResult &msg)
        {
            if (!NPageCollection::TGroupBlobsByCookie::IsInPlane(msg.Id, Mask)) {
                Y_TABLET_ERROR("TEvPutResult Id mask is differ from used");
            } else if (Writing < msg.Id.BlobSize()) {
                Y_TABLET_ERROR("Compaction writing bytes counter is out of sync");
            } else if (Flushing < msg.Id.BlobSize()) {
                Y_TABLET_ERROR("Compaction flushing bytes counter is out of sync");
            }

            Writing -= msg.Id.BlobSize();
            Flushing -= msg.Id.BlobSize();

            const ui32 channel = msg.Id.Channel();

            if (msg.StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)) {
                Y_DEBUG_ABORT_UNLESS(channel < 256);
                if (!SeenYellowMoveChannels[channel]) {
                    SeenYellowMoveChannels[channel] = true;
                    YellowMoveChannels.push_back(channel);
                }
            }
            if (msg.StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop)) {
                Y_DEBUG_ABORT_UNLESS(channel < 256);
                if (!SeenYellowStopChannels[channel]) {
                    SeenYellowStopChannels[channel] = true;
                    YellowStopChannels.push_back(channel);
                }
            }

            Conf->Writer.ChannelsShares.Update(channel, msg.ApproximateFreeSpaceShare);

            const auto ok = (msg.Status == NKikimrProto::OK);

            if (auto logl = Logger->Log(ok ? ELnLev::Debug: ELnLev::Error)) {
                logl
                    << NFmt::Do(*this)
                    << " put " << msg.Id.ToString()
                    << " result " << msg.Status
                    << " flags " << msg.StatusFlags
                    << " left " << Flushing << "b";
            }

            if (ok) {
                Send(Owner, new NBlockIO::TEvStat(NBlockIO::EDir::Write, NBlockIO::EPriority::Bulk, msg.GroupId, msg.Id));

                while (!WriteQueue.empty() && Writing < MaxFlight) {
                    SendToBs(std::move(WriteQueue.front()));
                    WriteQueue.pop_front();
                }

                Y_DEBUG_ABORT_UNLESS(Flushing == 0 || Writing > 0, "Unexpected: Flushing > 0 and Writing == 0");

                if (Flushing == 0) {
                    Spent->Alter(true /* resource available again */);
                    Driver->Touch(Finished ? EScan::Final : EScan::Feed);
                }
            } else if (!std::exchange(Failed, true)) {
                Driver->Touch(EScan::Final);
            }
        }

        void FlushToBs(NPageCollection::TGlob&& glob)
        {
            Y_ENSURE(glob.GId.Logo.BlobSize() == glob.Data.size(),
                "Written LogoBlob size doesn't match id");

            Flushing += glob.GId.Logo.BlobSize();
            Blobs++;

            if (Writing < MaxFlight && WriteQueue.empty()) {
                SendToBs(std::move(glob));
            } else {
                Y_DEBUG_ABORT_UNLESS(Failed || Writing > 0, "Unexpected: enqueued blob when Writing == 0");
                WriteQueue.emplace_back(std::move(glob));
            }
        }

        void SendToBs(NPageCollection::TGlob&& glob)
        {
            auto id = glob.GId;

            Writing += id.Logo.BlobSize();
            Y_DEBUG_ABORT_UNLESS(Writing <= Flushing, "Unexpected: Writing > Flushing");

            if (auto logl = Logger->Log(ELnLev::Debug)) {
                logl
                    << NFmt::Do(*this)
                    << " saving " << id.Logo.ToString()
                    << " left " << Flushing << "b";
            }

            auto flag = NKikimrBlobStorage::AsyncBlob;
            auto *ev = new TEvPut(TEvPut::TParameters{
                .BlobId = id.Logo,
                .Buffer = TRope(std::exchange(glob.Data, TString{ })),
                .Deadline = TInstant::Max(),
                .HandleClass = flag,
                .Tactic = TEvBlobStorage::TEvPut::ETactic::TacticMaxThroughput,
                .WriteSource = TWriteSource::FlatCompactionPut,
                .ExternalRelevanceWatcher = RelevanceTracker,
            });
            auto ctx = ActorContext();

            SendToBSProxy(ctx, id.Group, ev);
        }

    private:
        const TLogoBlobID Mask;
        const TActorId Owner;
        TMessageRelevanceOwner RelevanceTracker = std::make_shared<TMessageRelevanceTracker>();
        TAutoPtr<NUtil::ILogger> Logger;
        IDriver * Driver = nullptr;
        THolder<TCompactCfg> Conf;
        TIntrusiveConstPtr<TScheme> Scheme;
        TAutoPtr<TBundle> Bundle;
        TIntrusivePtr<TPartWriter> Writer;
        NTable::TWriteStats WriteStats;
        TVector<TBundle::TResult> Results;
        TVector<TIntrusiveConstPtr<NTable::TTxStatusPart>> TxStatus;
        const NScheme::TTypeRegistry * Registry = nullptr;
        NSharedCache::TSharedCachePages * SharedCachePages;

        bool Finished = false;
        bool Failed = false;/* Failed to write blobs    */
        TAutoPtr<TSpent> Spent; /* Blockage on write stats  */
        ui64 Blobs = 0;     /* Blobs produced by writer */
        ui64 Writing = 0;   /* Bytes flying to storage  */
        ui64 Flushing = 0;  /* Bytes flushing to storage */

        std::bitset<256> SeenYellowMoveChannels;
        std::bitset<256> SeenYellowStopChannels;
        TVector<ui32> YellowMoveChannels;
        TVector<ui32> YellowStopChannels;
        TDeque<NPageCollection::TGlob> WriteQueue;

        THashMap<ui64, TRow> Deltas;
        TSmallVec<ui64> DeltasOrder;
        bool IsLocked = false;

        std::unique_ptr<TFulltextCompact> FtState;
    };
}
}
