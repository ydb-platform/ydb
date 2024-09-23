#include "flat_page_label.h"
#include "flat_part_iface.h"
#include "flat_page_data.h"
#include "flat_page_flat_index.h"

#include <ydb/core/util/pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet_flat/protos/flat_table_part.pb.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/map.h>

namespace NKikimr {
namespace NTable {

TPartScheme::TPartScheme(TArrayRef<const TColInfo> cols)
{
    ui32 maxGroup = 0;
    for (auto& col : cols) {
        maxGroup = Max(maxGroup, col.Group);
    }

    ui32 pos = 0;
    Groups.resize(size_t(maxGroup) + 1);
    for (auto& col : cols) {
        Groups[col.Group].Columns.push_back(col);
        Groups[col.Group].Columns.back().Pos = pos++;
    }

    FillKeySlots();
    FillHistoricSlots();
}

TIntrusiveConstPtr<TPartScheme> TPartScheme::Parse(TArrayRef<const char> raw, bool labeled)
{
    if (labeled) {
        /* New styled scheme blob prepended with generic TLabel data */

        auto got = NPage::TLabelWrapper().Read(raw, NPage::EPage::Schem2);

        // Version 1 may have non-zero group columns
        Y_ABORT_UNLESS(got.Version == 0 || got.Version == 1, "Unknown EPage::Schem2 version");

        raw = got.Page;
    }

    TProtoBox<NProto::TPartScheme> proto(raw);

    TMap<TTag, ui32> byTag;
    TVector<TColInfo> cols;

    for (size_t i = 0; i < proto.ColumnsSize(); i++) {
        auto &one = proto.GetColumns(i);

        cols.emplace_back();
        cols.back().Tag = one.GetTag();
        cols.back().TypeInfo = NScheme::TypeInfoModFromProtoColumnType(one.GetType(),
            one.HasTypeInfo() ? &one.GetTypeInfo() : nullptr).TypeInfo;
        cols.back().Pos = cols.size() - 1;
        cols.back().Group = one.GetGroup();

        if (one.HasKey())
            cols.back().Key = one.GetKey();

        byTag[one.GetTag()] = cols.back().Pos;
    }

    /* Compatability with legacy schemes */
    for (size_t pos = 0; pos < proto.KeyTagsSize(); pos++) {
        auto it = byTag.find(proto.GetKeyTags(pos));

        Y_ABORT_UNLESS(it != byTag.end(), "Cannot find key tag plain scheme");

        cols[it->second].Key = pos;
    }

    return new TPartScheme(cols);
}

void TPartScheme::FillKeySlots()
{
    for (auto& group : Groups) {
        InitGroup(group);

        for (const auto& col : group.Columns) {
            AllColumns.push_back(col);
        }
    }

    auto byPos = NTable::TColInfo::TByPos();

    std::sort(AllColumns.begin(), AllColumns.end(), byPos);

    for (auto& col : AllColumns) {
        Tag2DataInfo[col.Tag] = &col;
    }
}

void TPartScheme::FillHistoricSlots()
{
    // Synthetic (rowid, step, txid) key used during history searches
    TStackVec<NScheme::TTypeInfoOrder, 3> types;
    types.emplace_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), NScheme::EOrder::Ascending);
    types.emplace_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), NScheme::EOrder::Descending);
    types.emplace_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), NScheme::EOrder::Descending);
    TStackVec<TCell, 3> defs;
    defs.resize(3);
    HistoryKeys = TKeyCellDefaults::Make(types, defs);

    // Synthetic (rowid, step, txid) key for the lead group of historic data
    // Note that Tag/Pos are left unspecified, they should never be used
    for (ui32 keyIdx = 0; keyIdx < HistoryKeys->Types.size(); ++keyIdx) {
        auto& col = HistoryGroup.Columns.emplace_back();
        col.Key = keyIdx;
        col.TypeInfo = HistoryKeys->Types[keyIdx].ToTypeInfo();
    }

    // All non-key columns go after synthetic key
    for (auto& col : Groups[0].Columns) {
        if (!col.IsKey()) {
            HistoryGroup.Columns.push_back(col);
        }
    }

    InitGroup(HistoryGroup);

    // The lead group of historic data has a different layout
    HistoryColumns = AllColumns;
    for (const auto& col : HistoryGroup.Columns) {
        if (!col.IsKey()) {
            HistoryColumns[col.Pos] = col;
        }
    }

    // Remove incorrect key columns from history
    for (auto& col : HistoryColumns) {
        if (col.IsKey()) {
            col = { };
        }
    }
}

void TPartScheme::InitGroup(TGroupInfo& group)
{
    using namespace NPage;

    group.FixedSize = InitInfo(group.Columns, TPgSizeOf<TDataPage::TItem>::Value);

    for (auto& col : group.Columns) {
        if (col.IsKey()) {
            Y_ABORT_UNLESS(col.Group == 0, "Key columns must be in the main column group");

            group.ColsKeyData.push_back(col);
        }
    }

    if (group.ColsKeyData) {
        auto byKey = NTable::TColInfo::TByKey();

        std::sort(group.ColsKeyData.begin(), group.ColsKeyData.end(), byKey);

        for (auto& col : group.ColsKeyData) {
            group.KeyTypes.push_back(col.TypeInfo);
        }

        group.ColsKeyIdx = group.ColsKeyData;
        group.IdxRecFixedSize = InitInfo(group.ColsKeyIdx, TPgSizeOf<TFlatIndex::TItem>::Value);
    } else {
        group.IdxRecFixedSize = 0;
    }
}

size_t TPartScheme::InitInfo(TVector<TColumn>& cols, TPgSize headerSize)
{
    size_t offset = 0;

    for (auto &col: cols) {
        const ui32 fixed = NScheme::GetFixedSize(col.TypeInfo);

        col.Offset = offset;
        col.IsFixed = fixed > 0;
        col.FixedSize = fixed > 0 ? fixed : sizeof(NPage::TDataRef);

        offset += col.FixedSize + headerSize;
    }

    return offset;
}

TSharedData TPartScheme::Serialize() const
{
    NProto::TPartScheme proto;

    for (const auto& col : AllColumns) {
        auto* pb = proto.AddColumns();
        pb->SetTag(col.Tag);
        auto protoType = NScheme::ProtoColumnTypeFromTypeInfoMod(col.TypeInfo, "");
        pb->SetType(protoType.TypeId);
        if (protoType.TypeInfo) {
            *pb->MutableTypeInfo() = *protoType.TypeInfo;
        }
        pb->SetGroup(col.Group);

        if (col.IsKey()) {
            pb->SetKey(col.Key);
        }
    }

    TStringStream ss;
    proto.SerializeToArcadiaStream(&ss);

    return NPage::TLabelWrapper::Wrap(ss.Str(), EPage::Schem2, Groups.size() > 1 ? 1 : 0);
}

}}
