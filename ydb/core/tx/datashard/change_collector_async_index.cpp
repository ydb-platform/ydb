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

        Y_VERIFY(!tags.empty());
        Y_VERIFY(!IndexTags.empty());

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
        Y_VERIFY_DEBUG(!tagToPos.contains(tag));
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

    for (const auto& [pathId, index] : userTable->Indexes) {
        if (index.Type != TUserTable::TTableIndex::EIndexType::EIndexTypeGlobalAsync) {
            continue;
        }

        if (generateDeletions) {
            bool needDeletion = false;

            for (const auto tag : index.KeyColumnIds) {
                if (updatedTagToPos.contains(tag) || rop == ERowOp::Erase || rop == ERowOp::Reset) {
                    needDeletion = true;
                }

                Y_VERIFY(tagToPos.contains(tag));
                Y_VERIFY(userTable->Columns.contains(tag));
                FillKeyFromRowState(tag, tagToPos.at(tag), row, userTable->Columns.at(tag).Type);
            }

            for (TPos pos = 0; pos < userTable->KeyColumnIds.size(); ++pos) {
                FillKeyFromKey(userTable->KeyColumnIds.at(pos), pos, key);
            }

            if (needDeletion) {
                Persist(tableId, pathId, ERowOp::Erase, IndexKeyVals, IndexKeyTags, {});
            }

            Clear();
        }

        if (generateUpdates) {
            bool needUpdate = !generateDeletions || rop == ERowOp::Reset;

            for (const auto tag : index.KeyColumnIds) {
                if (updatedTagToPos.contains(tag)) {
                    needUpdate = true;
                    FillKeyFromUpdate(tag, updatedTagToPos.at(tag), updates);
                } else {
                    Y_VERIFY(userTable->Columns.contains(tag));
                    const auto& column = userTable->Columns.at(tag);

                    if (rop == ERowOp::Reset && !column.IsKey) {
                        FillKeyWithNull(tag, column.Type);
                    } else {
                        Y_VERIFY(tagToPos.contains(tag));
                        FillKeyFromRowState(tag, tagToPos.at(tag), row, column.Type);
                    }
                }
            }

            for (TPos pos = 0; pos < userTable->KeyColumnIds.size(); ++pos) {
                FillKeyFromKey(userTable->KeyColumnIds.at(pos), pos, key);
            }

            for (const auto tag : index.DataColumnIds) {
                if (updatedTagToPos.contains(tag)) {
                    needUpdate = true;
                    FillDataFromUpdate(tag, updatedTagToPos.at(tag), updates);
                } else {
                    Y_VERIFY(userTable->Columns.contains(tag));
                    const auto& column = userTable->Columns.at(tag);

                    if (rop == ERowOp::Reset && !column.IsKey) {
                        FillDataWithNull(tag, column.Type);
                    } else {
                        Y_VERIFY(tagToPos.contains(tag));
                        FillDataFromRowState(tag, tagToPos.at(tag), row, column.Type);
                    }
                }
            }

            if (needUpdate) {
                Persist(tableId, pathId, ERowOp::Upsert, IndexKeyVals, IndexKeyTags, IndexDataVals);
            }

            Clear();
        }
    }

    return true;
}

auto TAsyncIndexChangeCollector::CacheTags(const TTableId& tableId) const {
    Y_VERIFY(Self->GetUserTables().contains(tableId.PathId.LocalPathId));
    auto userTable = Self->GetUserTables().at(tableId.PathId.LocalPathId);

    TCachedTagsBuilder builder;

    for (const auto& [_, index] : userTable->Indexes) {
        if (index.Type != TUserTable::TTableIndex::EIndexType::EIndexTypeGlobalAsync) {
            continue;
        }

        builder.AddIndexTags(index.KeyColumnIds);
        builder.AddDataTags(index.DataColumnIds);
    }

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
        Y_FAIL("unreachable");
    }
}

void TAsyncIndexChangeCollector::FillKeyFromRowState(TTag tag, TPos pos, const TRowState& rowState, NScheme::TTypeInfo type) {
    Y_VERIFY(pos < rowState.Size());

    IndexKeyVals.emplace_back(rowState.Get(pos).AsRef(), type);
    IndexKeyTags.emplace_back(tag);
    TagsSeen.insert(tag);
}

void TAsyncIndexChangeCollector::FillKeyFromKey(TTag tag, TPos pos, TArrayRef<const TRawTypeValue> key) {
    Y_VERIFY(pos < key.size());

    if (TagsSeen.contains(tag)) {
        return;
    }

    IndexKeyVals.emplace_back(key.at(pos));
    IndexKeyTags.emplace_back(tag);
}

void TAsyncIndexChangeCollector::FillKeyFromUpdate(TTag tag, TPos pos, TArrayRef<const TUpdateOp> updates) {
    Y_VERIFY(pos < updates.size());

    const auto& update = updates.at(pos);
    Y_VERIFY_S(update.Op == ECellOp::Set, "Unexpected op: " << update.Op.Raw());

    IndexKeyVals.emplace_back(update.Value);
    IndexKeyTags.emplace_back(tag);
    TagsSeen.insert(tag);
}

void TAsyncIndexChangeCollector::FillKeyWithNull(TTag tag, NScheme::TTypeInfo type) {
    IndexKeyVals.emplace_back(TRawTypeValue({}, type));
    IndexKeyTags.emplace_back(tag);
    TagsSeen.insert(tag);
}

void TAsyncIndexChangeCollector::FillDataFromRowState(TTag tag, TPos pos, const TRowState& rowState, NScheme::TTypeInfo type) {
    Y_VERIFY(pos < rowState.Size());
    IndexDataVals.emplace_back(tag, ECellOp::Set, TRawTypeValue(rowState.Get(pos).AsRef(), type));
}

void TAsyncIndexChangeCollector::FillDataFromUpdate(TTag tag, TPos pos, TArrayRef<const TUpdateOp> updates) {
    Y_VERIFY(pos < updates.size());

    const auto& update = updates.at(pos);
    Y_VERIFY_S(update.Op == ECellOp::Set, "Unexpected op: " << update.Op.Raw());

    IndexDataVals.emplace_back(tag, ECellOp::Set, update.Value);
}

void TAsyncIndexChangeCollector::FillDataWithNull(TTag tag, NScheme::TTypeInfo type) {
    IndexDataVals.emplace_back(tag, ECellOp::Set, TRawTypeValue({}, type));
}

void TAsyncIndexChangeCollector::Persist(const TTableId& tableId, const TPathId& pathId, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TTag> keyTags, TArrayRef<const TUpdateOp> updates)
{
    NKikimrChangeExchange::TChangeRecord::TDataChange body;
    Serialize(body, rop, key, keyTags, updates);
    Sink.AddChange(tableId, pathId, TChangeRecord::EKind::AsyncIndex, body);
}

void TAsyncIndexChangeCollector::Clear() {
    TagsSeen.clear();
    IndexKeyTags.clear();
    IndexKeyVals.clear();
    IndexDataVals.clear();
}

} // NDataShard
} // NKikimr
