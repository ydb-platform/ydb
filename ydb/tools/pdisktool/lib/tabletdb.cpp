#include "tabletdb.h"

#include <ydb/core/tablet_flat/flat_boot_cookie.h>
#include <ydb/core/tablet_flat/flat_boot_switch.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/flat_dbase_apply.h>
#include <ydb/core/tablet_flat/flat_dbase_naked.h>
#include <ydb/core/tablet_flat/flat_executor.pb.h>
#include <ydb/core/tablet_flat/flat_mem_warm.h>
#include <ydb/core/tablet_flat/flat_part_loader.h>
#include <ydb/core/tablet_flat/flat_part_store.h>
#include <ydb/core/tablet_flat/flat_sausage_chop.h>
#include <ydb/core/tablet_flat/flat_sausage_layout.h>
#include <ydb/core/tablet_flat/flat_sausage_packet.h>
#include <ydb/core/tablet_flat/flat_table_misc.h>

#include <library/cpp/blockcodecs/codecs.h>

namespace NKikimr::NPDiskTool {

namespace {

using namespace NKikimr::NTabletFlatExecutor;

using NBoot::TCookie;
using NBoot::TSwitch;
using EIdx = TCookie::EIdx;
using TPrivateCollection = TPrivatePageCache::TPageCollection;

// One page of a part, and one commit body, always stay well below this; a larger length has been read
// off a damaged blob and must not become an allocation.
constexpr ui64 MaxBody = NTable::MaxDecompressedBlobSize;

// A commit body, either external or embedded, in log order.
struct TCommit {
    NTable::TTxStamp Stamp = 0;
    NPageCollection::TLargeGlobId LargeGlobId;
    TString Body;
};

// A scheme change log body, resolved lazily.
struct TMeta {
    NPageCollection::TLargeGlobId LargeGlobId;
    TString Body;
};

// NPageCollection::TMeta trusts its input: it reads the blob and page tables at offsets taken from the
// header without checking that they are inside the blob. A meta blob that came back damaged would make
// it walk off the end, so the layout is checked here first.
bool PlausibleMeta(TStringBuf raw) {
    if (raw.size() < sizeof(NPageCollection::THeader)) {
        return false;
    }
    const auto* header = reinterpret_cast<const NPageCollection::THeader*>(raw.data());
    if (header->Magic != NPageCollection::Magic) {
        return false;
    }
    if (!header->Pages) {
        return true; // an empty page collection stops right after the header
    }
    const ui64 tables = sizeof(NPageCollection::THeader)
        + ui64(header->Blobs) * sizeof(NPageCollection::TBlobId)
        + ui64(header->Pages) * (sizeof(NPageCollection::TEntry) + sizeof(NPageCollection::TExtra));
    if (raw.size() < tables) {
        return false;
    }
    const auto* index = reinterpret_cast<const NPageCollection::TEntry*>(raw.data()
        + sizeof(NPageCollection::THeader)
        + ui64(header->Blobs) * sizeof(NPageCollection::TBlobId));
    // The inplace bodies follow the tables and are addressed by end offset.
    return index[header->Pages - 1].Inplace <= raw.size() - tables;
}

// Pages read straight out of the blob store. This backs both NTable::TLoader while the parts are being
// built and the iterators while the tables are being dumped, so what has been read is kept: a page is
// touched again and again during a scan, and IPages hands out plain pointers that have to stay valid.
class TOfflinePages : public NTable::IPages {
public:
    TOfflinePages(TBlobStore& store, TTabletBootStats& stats, TRepeatedIssues& repeated)
        : Store(store)
        , Stats(stats)
        , Repeated(repeated)
    {}

    TResult Locate(const NTable::TMemTable* memTable, ui64 ref, ui32 tag) override {
        try {
            return NTable::MemTableRefLookup(memTable, ref, tag);
        } catch (...) {
            // The annex blob holding this cell never arrived.
            ++Stats.PagesMissing;
            Repeated.Add("A memtable external value cannot be resolved", ToString(ref));
            return {true, nullptr};
        }
    }

    TResult Locate(const NTable::TPart* part, ui64 ref, NTable::ELargeObj lob) override {
        const auto* partStore = dynamic_cast<const NTable::TPartStore*>(part);
        if (!partStore) {
            return {true, nullptr};
        }
        // TPartStore::Locate aborts on anything it does not expect, and reaches past the end of its
        // page collection list for an outer reference of a part that has no outer group.
        if ((lob != NTable::ELargeObj::Extern && lob != NTable::ELargeObj::Outer) || (ref >> 32)) {
            ++Stats.PagesCorrupt;
            Repeated.Add("A large value reference is malformed", ToString(ref));
            return {true, nullptr};
        }
        if (lob == NTable::ELargeObj::Outer
                && partStore->GroupsCount >= partStore->PageCollections.size()) {
            ++Stats.PagesMissing;
            Repeated.Add("A part refers to an outer value group it does not have",
                partStore->Label.ToString());
            return {true, nullptr};
        }
        return {true, GetPage(partStore->Locate(lob, ref), ref)};
    }

    const TSharedData* TryGetPage(const NTable::TPart* part, TPageId pageId, TGroupId groupId) override {
        const auto* partStore = dynamic_cast<const NTable::TPartStore*>(part);
        if (!partStore || groupId.Index >= partStore->PageCollections.size()) {
            return nullptr;
        }
        return GetPage(partStore->PageCollections[groupId.Index].Get(), pageId);
    }

    const TSharedData* GetPage(const TPrivateCollection* collection, ui64 pageId) {
        if (!collection || !collection->PageCollection) {
            return nullptr;
        }
        return GetPage(collection->Id, *collection->PageCollection, pageId);
    }

    const TSharedData* GetPage(const TLogoBlobID& label,
            const NPageCollection::IPageCollection& pageCollection, ui64 pageId)
    {
        auto& perCollection = Cache[label];
        if (const auto it = perCollection.find(pageId); it != perCollection.end()) {
            return it->second ? &it->second : nullptr;
        }

        TSharedData page;
        const bool ok = Read(pageCollection, label, pageId, page);
        auto& slot = perCollection[pageId];
        if (!ok) {
            return nullptr; // the empty slot remembers that this page is not coming
        }
        slot = std::move(page);
        ++Stats.PagesRead;
        return &slot;
    }

private:
    // A page lives on a run of blobs, the first one entered at an offset; this is the walk the block IO
    // layer does, with the blob bodies coming from the input directories instead of BlobStorage.
    bool Read(const NPageCollection::IPageCollection& pageCollection, const TLogoBlobID& label,
            ui64 pageId, TSharedData& out)
    {
        NPageCollection::TBorder bound;
        try {
            bound = pageCollection.Bounds(pageId);
        } catch (...) {
            ++Stats.PagesCorrupt;
            Repeated.Add("Page collection has no bounds for a page it is asked for", label.ToString());
            return false;
        }
        if (!bound || !bound.Bytes || bound.Bytes > MaxBody || bound.Up.Blob < bound.Lo.Blob) {
            ++Stats.PagesCorrupt;
            Repeated.Add("Page bounds are implausible", label.ToString());
            return false;
        }

        TString body;
        body.reserve(bound.Bytes);
        ui64 left = bound.Bytes;
        for (ui32 index = bound.Lo.Blob; index <= bound.Up.Blob && left; ++index) {
            NPageCollection::TGlobId glob;
            try {
                glob = pageCollection.Glob(index);
            } catch (...) {
                ++Stats.PagesCorrupt;
                Repeated.Add("Page collection has no blob for a page it is asked for", label.ToString());
                return false;
            }
            const TString* blob = Store.Get(glob.Logo);
            if (!blob) {
                ++Stats.PagesMissing;
                Repeated.Add("Page data blob is missing from the input", glob.Logo.ToString());
                return false;
            }
            const ui64 skip = index > bound.Lo.Blob ? 0 : bound.Lo.Skip;
            if (skip >= blob->size()) {
                ++Stats.PagesCorrupt;
                Repeated.Add("Page starts past the end of its blob", glob.Logo.ToString());
                return false;
            }
            const ui64 take = Min(left, blob->size() - skip);
            body.append(blob->data() + skip, take);
            left -= take;
        }
        if (left) {
            ++Stats.PagesCorrupt;
            Repeated.Add("Page is shorter than its bounds claim", label.ToString());
            return false;
        }

        bool verified = false;
        try {
            verified = pageCollection.Verify(pageId, body);
        } catch (...) {
        }
        if (!verified) {
            // The bytes are all there but do not match the recorded checksum: either the wrong blob or
            // a damaged one, and parsing it as a page would produce nonsense.
            ++Stats.PagesCorrupt;
            Repeated.Add("Page checksum does not match", label.ToString());
            return false;
        }

        out = TSharedData::Copy(body.data(), body.size());
        return true;
    }

    TBlobStore& Store;
    TTabletBootStats& Stats;
    TRepeatedIssues& Repeated;
    // Node based, so the pointers handed out through IPages survive later insertions.
    THashMap<TLogoBlobID, THashMap<ui64, TSharedData>> Cache;
};

} // namespace

class TTabletBoot::TImpl {
public:
    TImpl(TBlobStore& store, ui64 tabletId, TIssueLog& issues)
        : Store(store)
        , TabletId(tabletId)
        , Issues(issues)
        , Repeated("tablet-db", "blob")
        , Codec(NBlockCodecs::Codec("lz4fast"))
        , Env(store, Stats_, Repeated)
    {}

    bool Run(const TTabletLogHistory& history);

    NTable::TDatabase& Database() {
        return *Database_;
    }

    NTable::IPages& Pages() {
        return Env;
    }

    const TTabletBootStats& Stats() const {
        return Stats_;
    }

private:
    bool Classify(const TTabletLogHistory& history);
    void ApplySnapshot(const NPageCollection::TLargeGlobId& snap, const TString& raw);
    void ReadRedoSnap(const NKikimrExecutorFlat::TLogSnapshot& proto);
    void ReadAlterLog(const NKikimrExecutorFlat::TLogSnapshot& proto);
    void SortLogoSpan(NTable::TTxStamp stamp, TArrayRef<const TLogoBlobID> span);
    void ApplyAlterLog();
    void LoadSwitches();
    void FoldSwitches();
    void ApplySwitches();
    bool LoadBundle(ui32 table, TSwitch::TBundle& bundle);
    bool LoadTxStatus(ui32 table, TSwitch::TTxStatus& txStatus);
    void ReplayRedo();
    void LoadAnnex();
    void SetTableEdge(const NKikimrExecutorFlat::TLogMemSnap& edge);

    // Bodies written through a TLargeGlobId say in their cookie whether they are packed; embedded ones
    // always are. A length read off a damaged blob is refused before it becomes an allocation.
    bool Decode(bool packed, const TString& in, TString& out, const TString& what);

    TBlobStore& Store;
    const ui64 TabletId;
    TIssueLog& Issues;
    TRepeatedIssues Repeated;
    const NBlockCodecs::ICodec* Codec = nullptr;

    TTabletBootStats Stats_;
    TOfflinePages Env;

    TAutoPtr<NTable::TScheme> Scheme;
    TAutoPtr<NTable::TDatabaseImpl> DatabaseImpl;
    THolder<NTable::TDatabase> Database_;

    ui64 Serial = 0;
    TVector<TCommit> RedoLog;
    TVector<TMeta> AlterLog;
    TDeque<TSwitch> Switches;
    THashMap<ui32, NTable::TSnapEdge> Edges;
};

bool TTabletBoot::TImpl::Decode(bool packed, const TString& in, TString& out, const TString& what) {
    if (!packed) {
        out = in;
        return true;
    }
    try {
        const size_t size = Codec->DecompressedLength(in);
        if (size > MaxBody) {
            Issues.Warning("tablet-db", TStringBuilder() << what << " claims " << size
                << " bytes when unpacked, which is not plausible; skipped");
            return false;
        }
        out = Codec->Decode(in);
        return true;
    } catch (...) {
        // Old enough tablets wrote some of these bodies plain, and a body that is neither is caught by
        // whoever parses it.
        Repeated.Add(TStringBuilder() << what << " does not unpack, so it is taken as it is",
            ToString(in.size()));
        out = in;
        return true;
    }
}

void TTabletBoot::TImpl::SetTableEdge(const NKikimrExecutorFlat::TLogMemSnap& edge) {
    const ui64 stamp = NTable::TTxStamp(edge.GetGeneration(), edge.GetStep());
    if (stamp == Max<ui64>()) {
        Repeated.Add("A table snapshot edge has an undefined stamp", ToString(edge.GetTable()));
        return;
    }
    auto& last = Edges[edge.GetTable()];
    last.TxStamp = Max(last.TxStamp, stamp);
    last.Head = Max(last.Head, NTable::TEpoch(edge.HasHead() ? edge.GetHead() : 0));
}

// The counterpart of NBoot::TSnap::SortLogoSpan: a run of blobs with adjacent cookies is one logical
// body, and the cookie says what kind of body it is.
void TTabletBoot::TImpl::SortLogoSpan(NTable::TTxStamp stamp, TArrayRef<const TLogoBlobID> span) {
    if (span.empty()) {
        return;
    }
    const TCookie cookie(span[0].Cookie());
    if (cookie.Type() != TCookie::EType::Log) {
        Repeated.Add("Log blob has an unknown cookie type", span[0].ToString());
        return;
    }

    NPageCollection::TLargeGlobId largeGlobId;
    try {
        largeGlobId = NPageCollection::TGroupBlobsByCookie::ToLargeGlobId(span, 0);
    } catch (...) {
        Repeated.Add("Log blobs of one body have inconsistent sizes", span[0].ToString());
        return;
    }

    switch (cookie.Index()) {
        case EIdx::Redo:
        case EIdx::RedoLz4:
            RedoLog.push_back({stamp, largeGlobId, {}});
            break;
        case EIdx::Alter:
            AlterLog.push_back({largeGlobId, {}});
            break;
        case EIdx::Turn:
        case EIdx::TurnLz4:
            for (const auto& one : span) {
                Switches.emplace_back(NPageCollection::TLargeGlobId{0, one});
            }
            break;
        case EIdx::Loan:
            // Borrow bookkeeping does not change the data, and a borrowed part is loaded from whatever
            // the input happens to hold anyway.
            Stats_.LoanEntries += span.size();
            break;
        case EIdx::GCExt:
            Stats_.GcEntries += span.size();
            break;
        default:
            if (!TCookie::CookieRangeRaw().Has(span[0].Cookie())) {
                Repeated.Add("Log blob has an unknown cookie index", span[0].ToString());
            }
            // Otherwise it is annex, the external values of a redo record, loaded with the memtables.
            break;
    }
}

bool TTabletBoot::TImpl::Classify(const TTabletLogHistory& history) {
    const auto& entries = history.Graph->Entries;
    size_t first = 0;

    if (!entries.empty() && entries[0].IsSnapshot) {
        const auto& entry = entries[0];
        const NTable::TTxStamp stamp{entry.Id.first, entry.Id.second};
        const auto span = NPageCollection::TGroupBlobsByCookie(entry.References).Do();
        if (span.size() != entry.References.size()) {
            Issues.Error("tablet-db", "The snapshot entry references blobs that are not one body");
            return false;
        }
        if (span.empty() || TCookie(span[0].Cookie()).Type() != TCookie::EType::Log
                || span[0].Step() == 0) {
            Issues.Error("tablet-db", "The snapshot entry has no usable blob id");
            return false;
        }
        NPageCollection::TLargeGlobId largeGlobId;
        try {
            largeGlobId = NPageCollection::TGroupBlobsByCookie::ToLargeGlobId(span, 0);
        } catch (...) {
            Issues.Error("tablet-db", TStringBuilder() << "The snapshot blobs of "
                << span[0].ToString() << " have inconsistent sizes");
            return false;
        }

        TString raw;
        if (!Store.Get(largeGlobId, raw)) {
            Issues.Error("tablet-db", TStringBuilder() << "The executor snapshot at generation "
                << stamp.Gen() << " step " << stamp.Step() << " (" << span[0].ToString()
                << ") is not in the input, so the schema, the parts and the older log it points at"
                << " cannot be recovered");
            return false;
        }
        ApplySnapshot(largeGlobId, raw);
        if (!Stats_.HasSnapshot) {
            return false;
        }
        first = 1;
    } else {
        // Without a snapshot there is no schema and there are no parts, only whatever the log tail
        // happens to create on its own.
        Issues.Warning("tablet-db", "The log history does not start with an executor snapshot, so only"
            " the tables and rows the remaining log creates can be recovered");
    }

    for (size_t i = first; i < entries.size(); ++i) {
        const auto& entry = entries[i];
        const NTable::TTxStamp stamp{entry.Id.first, entry.Id.second};
        if (entry.EmbeddedLogBody) {
            RedoLog.push_back({stamp, {}, entry.EmbeddedLogBody});
            continue;
        }
        NPageCollection::TGroupBlobsByCookie chop(entry.References);
        while (auto span = chop.Do()) {
            SortLogoSpan(stamp, span);
        }
    }
    return true;
}

void TTabletBoot::TImpl::ReadAlterLog(const NKikimrExecutorFlat::TLogSnapshot& proto) {
    TVector<TLogoBlobID> blobs;
    blobs.reserve(proto.SchemeInfoBodiesSize());
    for (const auto& one : proto.GetSchemeInfoBodies()) {
        blobs.emplace_back(LogoBlobIDFromLogoBlobID(one));
    }

    NPageCollection::TGroupBlobsByCookie chop(blobs);
    while (auto span = chop.Do()) {
        try {
            AlterLog.push_back({NPageCollection::TGroupBlobsByCookie::ToLargeGlobId(span, 0), {}});
        } catch (...) {
            Repeated.Add("Scheme blobs of one body have inconsistent sizes", span[0].ToString());
        }
    }
}

// Two ordered lists of redo records, one external and one embedded, merged by stamp exactly as the
// executor does it.
void TTabletBoot::TImpl::ReadRedoSnap(const NKikimrExecutorFlat::TLogSnapshot& proto) {
    TVector<TLogoBlobID> logos;
    logos.reserve(proto.NonSnapLogBodiesSize());
    for (const auto& x : proto.GetNonSnapLogBodies()) {
        logos.emplace_back(LogoBlobIDFromLogoBlobID(x));
    }

    NPageCollection::TGroupBlobsByCookie chop(logos);

    size_t offset = 0;
    const size_t size = proto.EmbeddedLogBodiesSize();

    for (auto span = chop.Do(); span || offset < size;) {
        const auto* lx = offset < size ? &proto.GetEmbeddedLogBodies(offset) : nullptr;
        const auto right = lx ? NTable::TTxStamp{lx->GetGeneration(), lx->GetStep()}
            : NTable::TTxStamp{Max<ui64>()};
        const auto left = span ? NTable::TTxStamp{span[0].Generation(), span[0].Step()}
            : NTable::TTxStamp{Max<ui64>()};

        if (left < right) {
            try {
                RedoLog.push_back({left,
                    NPageCollection::TGroupBlobsByCookie::ToLargeGlobId(span, 0), {}});
            } catch (...) {
                Repeated.Add("Redo blobs of one body have inconsistent sizes", span[0].ToString());
            }
        } else {
            RedoLog.push_back({right, {}, lx->GetBody()});
            ++offset;
        }

        span = left > right ? span : chop.Do();
    }
}

void TTabletBoot::TImpl::ApplySnapshot(const NPageCollection::TLargeGlobId& snap, const TString& raw) {
    TString body;
    const bool packed = TCookie(snap.Lead.Cookie()).Index() == EIdx::SnapLz4;
    if (!Decode(packed, raw, body, "The executor snapshot")) {
        return;
    }

    NKikimrExecutorFlat::TLogSnapshot proto;
    if (!proto.ParseFromString(body)) {
        Issues.Error("tablet-db", TStringBuilder() << "The executor snapshot " << snap.Lead.ToString()
            << " is " << body.size() << " bytes that do not parse as a snapshot");
        return;
    }

    Serial = proto.GetSerial();
    Stats_.HasSnapshot = true;
    Stats_.SnapshotSerial = Serial;

    ReadAlterLog(proto);
    ReadRedoSnap(proto);

    auto initSwitch = [&](const auto& one) {
        try {
            Switches.emplace_back().Init(one);
            ++Stats_.Switches;
        } catch (...) {
            Switches.pop_back();
            ++Stats_.SwitchesSkipped;
            Repeated.Add(TStringBuilder() << "A part list in the snapshot is inconsistent: "
                << CurrentExceptionMessage(), snap.Lead.ToString());
        }
    };

    for (const auto& one : proto.GetRowVersionStates()) {
        initSwitch(one);
    }
    for (const auto& one : proto.GetDbParts()) {
        initSwitch(one);
    }
    for (const auto& one : proto.GetTxStatusParts()) {
        initSwitch(one);
    }

    for (const auto& x : proto.GetTableSnapshoted()) {
        SetTableEdge(x);
    }

    Stats_.LoanEntries += proto.BorrowInfoIdsSize();
}

void TTabletBoot::TImpl::ApplyAlterLog() {
    Scheme = new NTable::TScheme;

    for (auto& entry : AlterLog) {
        TString body = entry.Body;
        if (!body && entry.LargeGlobId && !Store.Get(entry.LargeGlobId, body)) {
            ++Stats_.AlterSkipped;
            Repeated.Add("A scheme log body is missing from the input, so the tables it describes will"
                " be missing or incomplete", entry.LargeGlobId.Lead.ToString());
            continue;
        }
        if (!body) {
            ++Stats_.AlterSkipped;
            continue;
        }

        NTable::TSchemeChanges alter;
        if (!alter.ParseFromString(body)) {
            ++Stats_.AlterSkipped;
            Repeated.Add("A scheme log body does not parse", entry.LargeGlobId.Lead.ToString());
            continue;
        }
        try {
            NTable::TSchemeModifier(*Scheme).Apply(alter);
            ++Stats_.AlterEntries;
        } catch (...) {
            ++Stats_.AlterSkipped;
            Repeated.Add(TStringBuilder() << "A scheme change cannot be applied: "
                << CurrentExceptionMessage(), entry.LargeGlobId.Lead.ToString());
        }
    }
}

void TTabletBoot::TImpl::LoadSwitches() {
    for (auto& one : Switches) {
        if (!one.LargeGlobId) {
            continue; // came from the snapshot, already initialized
        }
        TString raw;
        if (!Store.Get(one.LargeGlobId, raw)) {
            ++Stats_.SwitchesSkipped;
            Repeated.Add("A part switch body is missing from the input, so the parts it adds or removes"
                " are not accounted for", one.LargeGlobId.Lead.ToString());
            continue;
        }
        TString body;
        const bool packed = TCookie(one.LargeGlobId.Lead.Cookie()).Index() == EIdx::TurnLz4;
        if (!Decode(packed, raw, body, "A part switch")) {
            ++Stats_.SwitchesSkipped;
            continue;
        }

        NKikimrExecutorFlat::TTablePartSwitch proto;
        if (!proto.ParseFromString(body)) {
            ++Stats_.SwitchesSkipped;
            Repeated.Add("A part switch body does not parse", one.LargeGlobId.Lead.ToString());
            continue;
        }
        try {
            one.Init(proto);
            ++Stats_.Switches;
        } catch (...) {
            ++Stats_.SwitchesSkipped;
            Repeated.Add(TStringBuilder() << "A part switch cannot be applied: "
                << CurrentExceptionMessage(), one.LargeGlobId.Lead.ToString());
            continue;
        }
        if (proto.HasTableSnapshoted()) {
            SetTableEdge(proto.GetTableSnapshoted());
        }
    }
}

// NBoot::TTurns::Process: the switches are folded in log order so that only the bundles still alive at
// the end are loaded. Production treats a reference to an unknown bundle as fatal; here it is counted
// and ignored, because a switch whose body was lost leaves exactly such references behind.
void TTabletBoot::TImpl::FoldSwitches() {
    THashSet<TLogoBlobID> leaving;
    THashMap<TLogoBlobID, TSwitch::TBundle*> bundles;
    THashSet<TLogoBlobID> leavingTxStatus;
    THashMap<TLogoBlobID, TSwitch::TTxStatus*> txStatus;

    for (auto& front : Switches) {
        if (!front.Loaded()) {
            continue; // the body never arrived, there is nothing to fold
        }

        for (const auto& id : front.LeavingTxStatus) {
            if (const auto it = txStatus.find(id); it != txStatus.end()) {
                it->second->Load = false;
                txStatus.erase(it);
                leavingTxStatus.insert(id);
            } else {
                Repeated.Add("A part switch removes a transaction status that was never added",
                    id.ToString());
            }
        }

        for (auto& one : front.TxStatus) {
            if (!one.DataId || txStatus.contains(one.DataId.Lead)
                    || leavingTxStatus.contains(one.DataId.Lead))
            {
                one.Load = false;
                Repeated.Add("A part switch adds a transaction status that is already known or was"
                    " removed", one.DataId.Lead.ToString());
                continue;
            }
            txStatus[one.DataId.Lead] = &one;
        }

        for (const auto& id : front.Leaving) {
            if (const auto it = bundles.find(id); it != bundles.end()) {
                it->second->Load = false;
                bundles.erase(it);
                leaving.insert(id);
            } else {
                Repeated.Add("A part switch removes a part that was never added", id.ToString());
            }
        }

        for (auto& change : front.Changes) {
            if (auto* bundle = bundles.Value(change.Label, nullptr)) {
                bundle->Legacy = std::move(change.Legacy);
                bundle->Opaque = std::move(change.Opaque);
                bundle->Deltas.clear();
            } else {
                Repeated.Add("A part switch changes a part that was never added",
                    change.Label.ToString());
            }
        }

        for (auto& delta : front.Deltas) {
            if (auto* bundle = bundles.Value(delta.Label, nullptr)) {
                bundle->Deltas.push_back(std::move(delta.Delta));
            } else {
                Repeated.Add("A part switch has a delta for a part that was never added",
                    delta.Label.ToString());
            }
        }

        for (auto& bundle : front.Bundles) {
            if (bundle.LargeGlobIds.empty()) {
                bundle.Load = false;
                Repeated.Add("A part switch adds a part without page collections",
                    ToString(front.Table));
                continue;
            }
            const auto& label = bundle.LargeGlobIds[0].Lead;
            if (bundles.contains(label) || leaving.contains(label)) {
                bundle.Load = false;
                Repeated.Add("A part switch adds a part that is already known or was removed",
                    label.ToString());
                continue;
            }
            bundles[label] = &bundle;
        }

        front.MovedBundles.resize(front.Moves.size());
        for (size_t index = 0; index < front.Moves.size(); ++index) {
            const auto& move = front.Moves[index];
            auto* source = bundles.Value(move.Label, nullptr);
            if (!source) {
                Repeated.Add("A part switch moves a part that was never added", move.Label.ToString());
                continue;
            }
            auto& bundle = front.MovedBundles[index];
            bundle = std::move(*source);
            if (move.RebasedEpoch != NTable::TEpoch::Max()) {
                bundle.Epoch = move.RebasedEpoch;
            }
            source->Load = false;
            bundles[move.Label] = &bundle;
        }
    }
}

bool TTabletBoot::TImpl::LoadBundle(ui32 table, TSwitch::TBundle& bundle) {
    TVector<TIntrusivePtr<TPrivateCollection>> collections;
    for (auto& largeGlobId : bundle.LargeGlobIds) {
        if (largeGlobId.Group == NPageCollection::TLargeGlobId::InvalidGroup) {
            // Storage groups mean nothing without BlobStorage, but a page collection refuses to be
            // built while the group is still the placeholder value.
            largeGlobId.Group = 0;
        }
        TString meta;
        if (!Store.Get(largeGlobId, meta)) {
            Repeated.Add("A page collection meta blob is missing from the input, so the rows of that"
                " part are lost", largeGlobId.Lead.ToString());
            return false;
        }
        if (meta.size() != largeGlobId.Bytes || !PlausibleMeta(meta)) {
            Repeated.Add("A page collection meta blob is damaged, so the rows of that part are lost",
                largeGlobId.Lead.ToString());
            return false;
        }
        collections.emplace_back(new TPrivateCollection(new NPageCollection::TPageCollection(
            largeGlobId, TSharedData::Copy(meta.data(), meta.size()))));
    }
    if (collections.empty()) {
        return false;
    }

    NTable::TLoader loader(std::move(collections), bundle.Legacy, bundle.Opaque, bundle.Deltas,
        bundle.Epoch);

    // Each round hands the loader everything it asked for, so a well formed part needs a handful of
    // them; the cap is only there to stop a damaged one from spinning.
    for (ui32 round = 0; round < 64; ++round) {
        auto fetch = loader.Run({.PreloadIndex = true, .PreloadData = false});
        if (!fetch) {
            DatabaseImpl->Merge(table, loader.Result());
            return true;
        }
        TVector<NSharedCache::TEvResult::TLoaded> loaded;
        loaded.reserve(fetch.Pages.size());
        for (const auto pageId : fetch.Pages) {
            const TSharedData* page = Env.GetPage(fetch.PageCollection->Label(), *fetch.PageCollection,
                pageId);
            if (!page) {
                Repeated.Add("A part cannot be read far enough to be usable, so it is dropped whole",
                    bundle.LargeGlobIds[0].Lead.ToString());
                return false;
            }
            loaded.emplace_back(pageId, NSharedCache::TSharedPageRef::MakePrivate(*page));
        }
        loader.Save(std::move(loaded));
    }

    Repeated.Add("A part keeps asking for pages and never finishes loading",
        bundle.LargeGlobIds[0].Lead.ToString());
    return false;
}

bool TTabletBoot::TImpl::LoadTxStatus(ui32 table, TSwitch::TTxStatus& txStatus) {
    TString data;
    if (!txStatus.DataId || !Store.Get(txStatus.DataId, data)) {
        Repeated.Add("A transaction status blob is missing from the input",
            txStatus.DataId.Lead.ToString());
        return false;
    }
    DatabaseImpl->Merge(table, MakeIntrusiveConst<NTable::TTxStatusPartStore>(txStatus.DataId,
        txStatus.Epoch, TSharedData::Copy(data.data(), data.size())));
    return true;
}

void TTabletBoot::TImpl::ApplySwitches() {
    for (auto& one : Switches) {
        if (!one.Loaded()) {
            continue;
        }
        auto& wrap = DatabaseImpl->Get(one.Table, false);
        if (!wrap) {
            continue; // the table was dropped later on
        }

        auto processBundles = [&](TVector<TSwitch::TBundle>& bundles) {
            for (auto& bundle : bundles) {
                if (!bundle.Load) {
                    continue;
                }
                bool ok = false;
                try {
                    ok = LoadBundle(one.Table, bundle);
                } catch (...) {
                    Repeated.Add(TStringBuilder() << "A part cannot be loaded: "
                        << CurrentExceptionMessage(),
                        bundle.LargeGlobIds ? bundle.LargeGlobIds[0].Lead.ToString() : TString("?"));
                }
                ok ? ++Stats_.BundlesLoaded : ++Stats_.BundlesDropped;
            }
        };

        processBundles(one.Bundles);
        processBundles(one.MovedBundles);

        for (auto& txStatus : one.TxStatus) {
            if (!txStatus.Load) {
                continue;
            }
            bool ok = false;
            try {
                ok = LoadTxStatus(one.Table, txStatus);
            } catch (...) {
                Repeated.Add(TStringBuilder() << "A transaction status part cannot be loaded: "
                    << CurrentExceptionMessage(), txStatus.DataId.Lead.ToString());
            }
            ok ? ++Stats_.TxStatusLoaded : ++Stats_.TxStatusDropped;
        }

        for (const auto& range : one.RemovedRowVersions) {
            try {
                wrap->RemoveRowVersions(range.Lower, range.Upper);
            } catch (...) {
            }
        }
    }
}

void TTabletBoot::TImpl::ReplayRedo() {
    for (auto& entry : RedoLog) {
        TString raw = entry.Body;
        if (!raw && entry.LargeGlobId && !Store.Get(entry.LargeGlobId, raw)) {
            ++Stats_.RedoSkipped;
            Repeated.Add("A redo log body is missing from the input, so the changes it carries are"
                " lost", entry.LargeGlobId.Lead.ToString());
            continue;
        }
        if (!raw) {
            ++Stats_.RedoSkipped;
            continue;
        }

        // An embedded body is always packed; an external one says so in its cookie.
        const bool packed = !entry.LargeGlobId
            || TCookie(entry.LargeGlobId.Lead.Cookie()).Index() == EIdx::RedoLz4;
        TString body;
        if (!Decode(packed, raw, body, "A redo log body")) {
            ++Stats_.RedoSkipped;
            continue;
        }

        try {
            DatabaseImpl->Switch(entry.Stamp).ApplyRedo(body).GrabAnnex();
            ++Stats_.RedoEntries;
        } catch (...) {
            ++Stats_.RedoSkipped;
            Repeated.Add(TStringBuilder() << "A redo log record cannot be replayed: "
                << CurrentExceptionMessage(), ToString(entry.Stamp.Raw));
        }
    }
}

// The external values a memtable refers to live in their own blobs, and until those are attached the
// cells holding them read as missing.
void TTabletBoot::TImpl::LoadAnnex() {
    for (const auto& it : DatabaseImpl->Scheme->Tables) {
        auto& wrap = DatabaseImpl->Get(it.first, false);
        if (!wrap) {
            continue;
        }
        for (const auto& mem : wrap->GetMemTables()) {
            auto* blobs = const_cast<NTable::NMem::TBlobs*>(mem->GetBlobs());
            const size_t size = blobs->Size();
            if (!size) {
                continue;
            }
            TVector<NPageCollection::TLoadedPage> pages(size);
            ui32 page = 0;
            bool complete = true;
            for (auto blob = blobs->Iterator(); blob.IsValid(); blob.Next()) {
                if (page >= size) {
                    complete = false;
                    break;
                }
                const TString* body = Store.Get(blob->GId.Logo);
                if (!body) {
                    ++Stats_.AnnexMissing;
                    Repeated.Add("A memtable external value blob is missing from the input",
                        blob->GId.Logo.ToString());
                    complete = false;
                    break;
                }
                pages[page].PageId = page;
                pages[page].Data = TSharedData::Copy(body->data(), body->size());
                ++page;
                ++Stats_.AnnexBlobs;
            }
            // The memtable takes the whole set at once, so a single missing blob costs the external
            // values of that memtable and nothing else.
            if (complete && page == size) {
                blobs->Assign(pages);
            }
        }
    }
}

bool TTabletBoot::TImpl::Run(const TTabletLogHistory& history) {
    Y_UNUSED(TabletId);

    if (!history.Ok || !history.Graph) {
        return false;
    }

    if (!Classify(history)) {
        Repeated.Flush(Issues, "warning");
        return false;
    }

    ApplyAlterLog();

    if (Scheme->Tables.empty()) {
        Issues.Error("tablet-db", "The recovered log defines no tables, so there is nothing to dump:"
            " the scheme log of this tablet is either missing from the input or was never written");
        Repeated.Flush(Issues, "warning");
        return false;
    }

    // Switches carry table snapshot edges, and those have to be known before the database is built.
    LoadSwitches();
    FoldSwitches();

    // The redo log is replayed after the parts are in place, so the database starts from a stamp above
    // its tail, the way the executor computes it.
    const NTable::TTxStamp weak = RedoLog ? NTable::TTxStamp(RedoLog.back().Stamp + 1)
        : NTable::TTxStamp(0);
    DatabaseImpl = new NTable::TDatabaseImpl(weak, Scheme, &Edges);

    ApplySwitches();
    ReplayRedo();
    LoadAnnex();

    DatabaseImpl->Rewind(Serial);
    DatabaseImpl->MergeDone();
    Database_ = MakeHolder<NTable::TDatabase>(DatabaseImpl.Release());

    Repeated.Flush(Issues, "warning");
    return true;
}

TTabletBoot::TTabletBoot(TBlobStore& store, ui64 tabletId, TIssueLog& issues)
    : Impl(MakeHolder<TImpl>(store, tabletId, issues))
{}

TTabletBoot::~TTabletBoot() = default;

bool TTabletBoot::Run(const TTabletLogHistory& history) {
    return Impl->Run(history);
}

NTable::TDatabase& TTabletBoot::Database() {
    return Impl->Database();
}

NTable::IPages& TTabletBoot::Pages() {
    return Impl->Pages();
}

const TTabletBootStats& TTabletBoot::Stats() const {
    return Impl->Stats();
}

} // namespace NKikimr::NPDiskTool
