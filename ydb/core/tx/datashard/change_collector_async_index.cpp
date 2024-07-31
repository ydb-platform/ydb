#include "change_collector_async_index.h"
#include "datashard_impl.h"
#include "datashard_user_db.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;
using namespace NTable;

class TCachedTagsBuilder {
    using TCachedTags = TAsyncIndexChangeCollector::TCachedTags;

public:
    void AddIndexTags(const TVector<TTag>& tags) {
        IndexTags.insert(tags.begin(), tags.end());
    }

    void AddDataTags(const TVector<TTag>& tags) {
        DataTags.insert(tags.begin(), tags.end());
    }

    TCachedTags Build() {
        TVector<TTag> tags(Reserve(IndexTags.size() + DataTags.size()));

        for (const auto tag : IndexTags) {
            tags.push_back(tag);
        }

        for (const auto tag : DataTags) {
            if (!IndexTags.contains(tag)) {
                tags.push_back(tag);
            }
        }

        Y_ABORT_UNLESS(!tags.empty());
        Y_ABORT_UNLESS(!IndexTags.empty());

        return TCachedTags(std::move(tags), std::make_pair(0, IndexTags.size() - 1));
    }

private:
    THashSet<TTag> IndexTags;
    THashSet<TTag> DataTags;
};

template <typename C, typename E>
static THashMap<TTag, TPos> MakeTagToPos(const C& container, E extractor) {
    THashMap<TTag, TPos> tagToPos;

    for (ui32 i = 0; i < container.size(); ++i) {
        const auto tag = extractor(container.at(i));
        Y_DEBUG_ABORT_UNLESS(!tagToPos.contains(tag));
        tagToPos.emplace(tag, i);
    }

    return tagToPos;
}

void TAsyncIndexChangeCollector::OnRestart() {
    TBaseChangeCollector::OnRestart();
}

bool TAsyncIndexChangeCollector::NeedToReadKeys() const {
    return true;
}

bool TAsyncIndexChangeCollector::Collect(const TTableId& tableId, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TUpdateOp> updates)
{
    Y_VERIFY_S(Self->IsUserTable(tableId), "Unknown table: " << tableId);

    auto userTable = Self->GetUserTables().at(tableId.PathId.LocalPathId);
    Y_VERIFY_S(key.size() == userTable->KeyColumnIds.size(), "Count doesn't match"
        << ": key# " << key.size()
        << ", tags# " << userTable->KeyColumnIds.size());

    switch (rop) {
    case ERowOp::Upsert:
    case ERowOp::Erase:
    case ERowOp::Reset:
        break;
    default:
        Y_FAIL_S("Unsupported row op: " << static_cast<ui8>(rop));
    }

    const auto tagsToSelect = GetTagsToSelect(tableId, rop);

    TRowState row;
    const auto ready = UserDb.SelectRow(tableId, key, tagsToSelect, row);

    if (ready == EReady::Page) {
        return false;
    }

    const bool generateDeletions = (ready != EReady::Gone);
    const bool generateUpdates = (rop != ERowOp::Erase);

    if (!generateDeletions && !generateUpdates) {
        return true;
    }

    const auto tagToPos = MakeTagToPos(tagsToSelect, [](const auto tag) { return tag; });
    const auto updatedTagToPos = MakeTagToPos(updates, [](const TUpdateOp& op) { return op.Tag; });

    userTable->ForEachAsyncIndex([&](const auto& pathId, const TUserTable::TTableIndex& index) {
        if (generateDeletions) {
            bool needDeletion = rop == ERowOp::Erase || rop == ERowOp::Reset;

            for (const auto tag : index.KeyColumnIds) {
                if (updatedTagToPos.contains(tag)) {
                    needDeletion = true;
                }

                Y_ABORT_UNLESS(tagToPos.contains(tag));
                Y_ABORT_UNLESS(userTable->Columns.contains(tag));
                AddCellValue(KeyVals, tag, row.Get(tagToPos.at(tag)), userTable->Columns.at(tag).Type);
                KeyTagsSeen.insert(tag);
            }

            for (TPos pos = 0; pos < userTable->KeyColumnIds.size(); ++pos) {
                const auto& tag = userTable->KeyColumnIds.at(pos);
                if (!KeyTagsSeen.contains(tag)) {
                    AddRawValue(KeyVals, tag, key.at(pos));
                }
            }

            if (needDeletion) {
                Persist(tableId, pathId, ERowOp::Erase, KeyVals, {});
            }

            Clear();
        }

        if (generateUpdates) {
            bool needUpdate = !generateDeletions || rop == ERowOp::Reset;

            for (const auto tag : index.KeyColumnIds) {
                if (updatedTagToPos.contains(tag)) {
                    needUpdate = true;
                    AddValue(KeyVals, updates.at(updatedTagToPos.at(tag)));
                    KeyTagsSeen.insert(tag);
                } else {
                    Y_ABORT_UNLESS(userTable->Columns.contains(tag));
                    const auto& column = userTable->Columns.at(tag);

                    if (rop == ERowOp::Reset && !column.IsKey) {
                        AddNullValue(KeyVals, tag, column.Type);
                        KeyTagsSeen.insert(tag);
                    } else {
                        Y_ABORT_UNLESS(tagToPos.contains(tag));
                        AddCellValue(KeyVals, tag, row.Get(tagToPos.at(tag)), column.Type);
                        KeyTagsSeen.insert(tag);
                    }
                }
            }

            for (TPos pos = 0; pos < userTable->KeyColumnIds.size(); ++pos) {
                const auto& tag = userTable->KeyColumnIds.at(pos);
                if (!KeyTagsSeen.contains(tag)) {
                    AddRawValue(KeyVals, tag, key.at(pos));
                }
            }

            for (const auto tag : index.DataColumnIds) {
                if (updatedTagToPos.contains(tag)) {
                    needUpdate = true;
                    AddValue(DataVals, updates.at(updatedTagToPos.at(tag)));
                } else {
                    Y_ABORT_UNLESS(userTable->Columns.contains(tag));
                    const auto& column = userTable->Columns.at(tag);

                    if (rop == ERowOp::Reset && !column.IsKey) {
                        AddNullValue(DataVals, tag, column.Type);
                    } else {
                        Y_ABORT_UNLESS(tagToPos.contains(tag));
                        AddCellValue(DataVals, tag, row.Get(tagToPos.at(tag)), column.Type);
                    }
                }
            }

            if (needUpdate) {
                Persist(tableId, pathId, ERowOp::Upsert, KeyVals, DataVals);
            }

            Clear();
        }
    });

    return true;
}

auto TAsyncIndexChangeCollector::CacheTags(const TTableId& tableId) const {
    Y_ABORT_UNLESS(Self->GetUserTables().contains(tableId.PathId.LocalPathId));
    auto userTable = Self->GetUserTables().at(tableId.PathId.LocalPathId);

    TCachedTagsBuilder builder;

    userTable->ForEachAsyncIndex([&](const auto&, const TUserTable::TTableIndex& index) {
        builder.AddIndexTags(index.KeyColumnIds);
        builder.AddDataTags(index.DataColumnIds);
    });

    return CachedTags.emplace(tableId, builder.Build()).first;
}

TArrayRef<TTag> TAsyncIndexChangeCollector::GetTagsToSelect(const TTableId& tableId, ERowOp rop) const {
    auto it = CachedTags.find(tableId);
    if (it == CachedTags.end()) {
        it = CacheTags(tableId);
    }

    switch (rop) {
    case ERowOp::Upsert:
        return it->second.Columns;
    case ERowOp::Erase:
    case ERowOp::Reset:
        return it->second.IndexColumns;
    default:
        Y_ABORT("unreachable");
    }
}

void TAsyncIndexChangeCollector::AddValue(TVector<TUpdateOp>& out, const TUpdateOp& update) {
    Y_VERIFY_S(update.Op == ECellOp::Set, "Unexpected op: " << update.Op.Raw());
    out.push_back(update);
}

void TAsyncIndexChangeCollector::AddRawValue(TVector<TUpdateOp>& out, TTag tag, const TRawTypeValue& value) {
    AddValue(out, TUpdateOp(tag, ECellOp::Set, value));
}

void TAsyncIndexChangeCollector::AddCellValue(TVector<TUpdateOp>& out, TTag tag, const TCell& cell, NScheme::TTypeInfo type) {
    AddRawValue(out, tag, TRawTypeValue(cell.AsRef(), type));
}

void TAsyncIndexChangeCollector::AddNullValue(TVector<TUpdateOp>& out, TTag tag, NScheme::TTypeInfo type) {
    AddCellValue(out, tag, {}, type);
}

void TAsyncIndexChangeCollector::Persist(const TTableId& tableId, const TPathId& pathId, ERowOp rop,
        TArrayRef<const TUpdateOp> keyVals, TArrayRef<const TUpdateOp> dataVals)
{
    TVector<TRawTypeValue> key(Reserve(keyVals.size()));
    TVector<TTag> keyTags(Reserve(keyVals.size()));
    for (const auto& v : keyVals) {
        key.push_back(v.Value);
        keyTags.push_back(v.Tag);
    }

    Persist(tableId, pathId, rop, key, keyTags, dataVals);
}

void TAsyncIndexChangeCollector::Persist(const TTableId& tableId, const TPathId& pathId, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TTag> keyTags, TArrayRef<const TUpdateOp> updates)
{
    NKikimrChangeExchange::TDataChange body;
    Serialize(body, rop, key, keyTags, updates);
    Sink.AddChange(tableId, pathId, TChangeRecord::EKind::AsyncIndex, body);
}

void TAsyncIndexChangeCollector::Clear() {
    KeyTagsSeen.clear();
    KeyVals.clear();
    DataVals.clear();
}

} // NDataShard
} // NKikimr
