#include "flat_part_dump.h"
#include "flat_part_iface.h"
#include "flat_part_index_iter_iface.h"
#include "flat_page_data.h"
#include "flat_page_frames.h"
#include "flat_page_blobs.h"
#include "flat_page_bloom.h"
#include "util_fmt_desc.h"
#include <util/stream/printf.h>

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_registry.h>

namespace NKikimr {
namespace NTable {

namespace {
    const NPage::TFrames::TEntry& GetFrame(const TPart &part, ui64 ref, TCellOp op)
    {
        static NPage::TFrames::TEntry None{ Max<TRowId>(), Max<ui16>(), 0, 0 };

        if (op == ELargeObj::Outer && part.Small) {
            return part.Small->Relation(ref);
        } else if (op == ELargeObj::Extern && part.Large) {
            return part.Large->Relation(ref);
        }

        return None;
    }
}

    TDump::TDump(IOut &out, IPages *env, const TReg *reg)
        : Out(out)
        , Env(env)
        , Reg(reg)
    {

    }

    TDump::~TDump() { }

    void TDump::Part(const TPart &part, ui32 depth) noexcept
    {
        Out << NFmt::Do(part) << " data " << part.DataSize() << "b" << Endl;

        if (auto *frames = part.Small.Get()) Frames(*frames, "Small");
        if (auto *frames = part.Large.Get()) Frames(*frames, "Large");
        if (auto *blobs = part.Blobs.Get())  Blobs(*blobs);
        if (auto *bloom = part.ByKey.Get())  Bloom(*bloom);

        Index(part, depth);
        BTreeIndex(part);

        if (depth > 2) {
            auto index = CreateIndexIter(&part, Env, { });
            
            for (ssize_t i = 0; ; i++) {
                auto ready = i == 0 ? index->Seek(0) : index->Next();
                if (ready != EReady::Data) {
                    if (ready == EReady::Page) {
                        Out << " | -- the rest of the index rows aren't loaded" << Endl;
                    }
                    break;
                }

                Out << Endl;

                DataPage(part, index->GetPageId());
            }
        }
    }

    void TDump::Frames(const NPage::TFrames &page, const char *tag) noexcept
    {
        Out
            << " + " << tag << " Label{" << page.Raw.size() << "b}"
            << " (" << page.Stats().Items << ", " << page.Stats().Size << "b)"
            << " " << page.Stats().Rows << "r " << page.Stats().Tags.size() << " tags"
            << Endl;
    }

    void TDump::Blobs(const NPage::TExtBlobs &page) noexcept
    {
        Out
            << " + Blobs Label{" << page.Raw.size() << "b} "
            << "(" << page.Stats().Items
            << ", " << page.Stats().Bytes << "b) refs"
            << Endl;
    }

    void TDump::Bloom(const NPage::TBloom &page) noexcept
    {
        Out
            << " + Bloom Label{" << page.Raw.size() << "b} "
            << page.Stats().Items << " bits, "
            << page.Stats().Hashes << " hashes"
            << Endl;
    }

    void TDump::Index(const TPart &part, ui32 depth) noexcept
    {
        if (!part.IndexPages.HasFlat()) {
            return;
        }

        TVector<TCell> key(Reserve(part.Scheme->Groups[0].KeyTypes.size()));

        auto indexPageId = part.IndexPages.GetFlat({});
        auto indexPage = Env->TryGetPage(&part, indexPageId, {});

        if (!indexPage) {
            Out
                << " + FlatIndex{unload}"
                << Endl
                << " |  Page     Row    Bytes  (";
            return;
        }
        
        auto index = NPage::TFlatIndex(*indexPage);
        auto label = index.Label();

        Out
            << " + FlatIndex{" << indexPageId << "}" 
            << " Label{" << (ui16)label.Type << " rev " << label.Format << ", " << label.Size << "b}"
            << " " << index->Count + (index.GetLastKeyRecord() ? 1 : 0) << " rec" << Endl
            << " |  Page     Row    Bytes  (";

        for (auto off : xrange(part.Scheme->Groups[0].KeyTypes.size())) {
            Out << (off ? ", " : "");

            TName(part.Scheme->Groups[0].KeyTypes[off].GetTypeId());
        }

        Out << ")" << Endl;

        auto printIndexKey = [&](const NPage::TFlatIndex::TRecord* record) {
            key.clear();
            for (const auto &info: part.Scheme->Groups[0].ColsKeyIdx)
                key.push_back(record->Cell(info));

            Out
                << " | " << (Printf(Out, " %4u", record->GetPageId()), " ")
                << (Printf(Out, " %6lu", record->GetRowId()), " ");

            if (auto *page = Env->TryGetPage(&part, record->GetPageId(), {})) {
                Printf(Out, " %6zub  ", page->size());
            } else {
                Out << "~none~  ";
            }

            Key(key, *part.Scheme);

            Out << Endl;
        };

        for (auto iter = index->Begin(); iter; iter++) {
            if (depth < 2 && iter.Off() >= 10) {
                Out
                    << " | -- skipped " << index->Count - iter.Off()
                    << " entries, depth level " << depth << Endl;

                break;
            }

            printIndexKey(iter.GetRecord());
        }

        if (index.GetLastKeyRecord()) {
            printIndexKey(index.GetLastKeyRecord());
        }
    }

    void TDump::BTreeIndex(const TPart &part) noexcept
    {
        if (part.IndexPages.HasBTree()) {
            auto meta = part.IndexPages.GetBTree({});
            if (meta.LevelCount) {
                BTreeIndexNode(part, meta);
            } else {
                Out
                    << " + BTreeIndex{Empty, "
                    << meta.ToString() << Endl;
            }
        }
    }

    void TDump::DataPage(const TPart &part, ui32 page) noexcept
    {
        TVector<TCell> key(Reserve(part.Scheme->Groups[0].KeyTypes.size()));

        // TODO: need to join with other column groups
        auto data = NPage::TDataPage(Env->TryGetPage(&part, page, {}));

        if (data) {
            auto label = data.Label();
            Out
                << " + Rows{" << page << "} Label{" << page << (ui16)label.Type
                << " rev " << label.Format << ", " << label.Size << "b}"
                << ", [" << data.BaseRow() << ", +" << data->Count << ")row"
                << Endl;

        } else {
            Out << " | " << page << " NOT_LOADED" << Endl;

            return;
        }

        for (auto iter = data->Begin(); iter; ++iter) {
            key.clear();
            for (const auto &info: part.Scheme->Groups[0].ColsKeyData)
                key.push_back(iter->Cell(info));

            Out << " | ERowOp " << int(iter->GetRop()) << ": ";

            Key(key, *part.Scheme);

            bool first = true;

            for (const auto &info : part.Scheme->Groups[0].Columns) {
                if (info.IsKey())
                    continue;

                const auto op = iter->GetCellOp(info);

                if (op == ECellOp::Empty)
                    continue;

                Out
                    << (std::exchange(first, false) ? " " : ", ")
                    << "{" << EOpToStr(op) << " " << info.Tag << " ";

                if (op == ELargeObj::Inline) {
                    Out
                        << DbgPrintCell(iter->Cell(info), info.TypeInfo, *Reg);
                } else {
                    const auto ref = iter->Cell(info).AsValue<ui64>();

                    TName(info.TypeInfo.GetTypeId());

                    const auto frame = GetFrame(part, ref, op);
                    const auto blob = Env->Locate(&part, ref, op);

                    Out << " ELargeObj{" << int(ELargeObj(op)) << ", " << ref << ": ";

                    if (auto bytes = frame.Size) {
                        Out << "frm " << bytes << "b";
                    } else {
                        Out << "nof";
                    }

                    if (auto bytes = (blob ? blob.Page->size() : 0)) {
                        Out << " raw " << bytes << "b";
                    } else {
                        Out << " -";
                    }

                    if (frame.Size && blob && frame.Size != blob.Page->size()) {
                        Out << " **"; /* error indicator */
                    }

                    Out << "}";
                }

                Out << "}";
            }

            Out << Endl;
        }
    }

    void TDump::TName(ui32 num) noexcept
    {
        const auto &type = Reg->GetType(num);

        if (type.IsKnownType()) {
            Out << type->GetName();
        } else {
            Out << "Type?" << num;
        }
    }

    void TDump::Key(TCellsRef key, const TPartScheme &scheme) noexcept
    {
        Out << "(";

        for (auto off : xrange(key.size())) {
            TString str;

            DbgPrintValue(str, key[off], scheme.Groups[0].KeyTypes[off]);

            Out << (off ? ", " : "") << str;
        }

        Out << ")";
    }

    void TDump::BTreeIndexNode(const TPart &part, NPage::TBtreeIndexNode::TChild meta, ui32 level) noexcept
    {
        TVector<TCell> key(Reserve(part.Scheme->Groups[0].KeyTypes.size()));

        TString intend;
        for (size_t i = 0; i < level; i++) {
            intend += " |";
        }

        auto dumpChild = [&] (NPage::TBtreeIndexNode::TChild child) {
            if (part.GetPageType(child.GetPageId(), {}) == EPage::BTreeIndex) {
                BTreeIndexNode(part, child, level + 1);
            } else {
                Out << intend << " | " << child.ToString() << Endl;
            }
        };

        auto page = Env->TryGetPage(&part, meta.GetPageId(), {});
        if (!page) {
            Out << intend << " | -- the rest of the index pages aren't loaded" << Endl;
            return;
        }

        auto node = NPage::TBtreeIndexNode(*page);

        auto label = node.Label();

        Out
            << intend
            << " + BTreeIndex{" << meta.ToString() << "}"
            << " Label{" << (ui16)label.Type << " rev " << label.Format << ", " << label.Size << "b}"
            << Endl;

        dumpChild(node.GetChild(0));

        for (NPage::TRecIdx i : xrange(node.GetKeysCount())) {
            Out << intend << " | > ";

            key.clear();
            auto cells = node.GetKeyCellsIter(i, part.Scheme->Groups[0].ColsKeyIdx);
            for (TPos pos : xrange(cells.Count())) {
                Y_UNUSED(pos);
                key.push_back(cells.Next());
            }

            Key(key, *part.Scheme);
            Out << Endl;
            dumpChild(node.GetChild(i + 1));
        }

        Out << Endl;
    }

}
}
