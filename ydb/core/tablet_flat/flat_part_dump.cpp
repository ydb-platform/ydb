#include "flat_part_dump.h"
#include "flat_part_iface.h"
#include "flat_page_index.h"
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

        if (auto *frames = part.Small.Get()) Dump(*frames, "Small");
        if (auto *frames = part.Large.Get()) Dump(*frames, "Large");
        if (auto *blobs = part.Blobs.Get())  Dump(*blobs);
        if (auto *bloom = part.ByKey.Get())  Dump(*bloom);

        Index(part, depth);

        if (depth > 2) {
            for (auto iter = part.Index->Begin(); iter; ++iter) {
                Out << Endl;

                DataPage(part, iter->GetPageId());
            }
        }
    }

    void TDump::Dump(const NPage::TFrames &page, const char *tag) noexcept
    {
        Out
            << " + " << tag << " Label{" << page.Raw.size() << "b}"
            << " (" << page.Stats().Items << ", " << page.Stats().Size << "b)"
            << " " << page.Stats().Rows << "r " << page.Stats().Tags.size() << " tags"
            << Endl;
    }

    void TDump::Dump(const NPage::TExtBlobs &page) noexcept
    {
        Out
            << " + Blobs Label{" << page.Raw.size() << "b} "
            << "(" << page.Stats().Items
            << ", " << page.Stats().Bytes << "b) refs"
            << Endl;
    }

    void TDump::Dump(const NPage::TBloom &page) noexcept
    {
        Out
            << " + Bloom Label{" << page.Raw.size() << "b} "
            << page.Stats().Items << " bits, "
            << page.Stats().Hashes << " hashes"
            << Endl;
    }

    void TDump::Index(const TPart &part, ui32 depth) noexcept
    {
        Key.reserve(part.Scheme->Groups[0].KeyTypes.size());

        auto label = part.Index.Label();

        const auto items = (part.Index->End() - part.Index->Begin());

        Out
            << " + Index{" << (ui16)label.Type << " rev "
            << label.Format << ", " << label.Size << "b}"
            << " " << items << " rec" << Endl
            << " |  Page     Row    Bytes  (";

        for (auto off : xrange(part.Scheme->Groups[0].KeyTypes.size())) {
            Out << (off ? ", " : "");

            TName(part.Scheme->Groups[0].KeyTypes[off].GetTypeId());
        }

        Out << ")" << Endl;

        ssize_t seen = 0;

        for (auto iter = part.Index->Begin(); iter; ++iter) {
            Key.clear();

            if (depth < 2 && (seen += 1) > 10) {
                Out
                    << " | -- skipped " << (items - Min(items, seen - 1))
                    << " entries, depth level " << depth << Endl;

                break;
            }

            for (const auto &info: part.Scheme->Groups[0].ColsKeyIdx)
                Key.push_back(iter->Cell(info));

            Out
                << " | " << (Printf(Out, " %4u", iter->GetPageId()), " ")
                << (Printf(Out, " %6lu", iter->GetRowId()), " ");

            if (auto *page = Env->TryGetPage(&part, iter->GetPageId())) {
                Printf(Out, " %6zub  ", page->size());
            } else {
                Out << "~none~  ";
            }

            DumpKey(*part.Scheme);

            Out << Endl;
        }
    }

    void TDump::DataPage(const TPart &part, ui32 page) noexcept
    {
        // TODO: need to join with other column groups
        auto data = NPage::TDataPage(Env->TryGetPage(&part, page));

        if (data) {
            auto label = data.Label();
            Out
                << " + Rows{" << page << "} Label{" << page << (ui16)label.Type
                << " rev " << label.Format << ", " << label.Size << "b}"
                << ", [" << data.BaseRow() << ", +" << data->Records << ")row"
                << Endl;

        } else {
            Out << " | " << page << " NOT_LOADED" << Endl;

            return;
        }

        for (auto iter = data->Begin(); iter; ++iter) {
            Key.clear();
            for (const auto &info: part.Scheme->Groups[0].ColsKeyData)
                Key.push_back(iter->Cell(info));

            Out << " | ERowOp " << int(iter->GetRop()) << ": ";

            DumpKey(*part.Scheme);

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

    void TDump::DumpKey(const TPartScheme &scheme) noexcept
    {
        Out << "(";

        for (auto off : xrange(Key.size())) {
            TString str;

            DbgPrintValue(str, Key[off], scheme.Groups[0].KeyTypes[off]);

            Out << (off ? ", " : "") << str;
        }

        Out << ")";
    }

}
}
