#include "change_collector_cdc_stream.h"
#include "datashard_impl.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;
using namespace NTable;

namespace {

    auto MakeValueTags(const TMap<TTag, TUserTable::TUserColumn>& columns) {
        TVector<TTag> result;

        for (const auto& [tag, column] : columns) {
            if (!column.IsKey) {
                result.push_back(tag);
            }
        }

        return result;
    }

    auto MakeTagToPos(TArrayRef<const TTag> tags) {
        THashMap<TTag, TPos> result;

        for (TPos pos = 0; pos < tags.size(); ++pos) {
            result.emplace(tags.at(pos), pos);
        }

        return result;
    }

    auto MappedUpdates(TArrayRef<const TUpdateOp> updates) {
        THashMap<TTag, TUpdateOp> result;

        for (const auto& op : updates) {
            result.emplace(op.Tag, op);
        }

        return result;
    }

    const TRowState* NullIfErased(const TRowState* state) {
        switch (state->GetRowState()) {
        case ERowOp::Upsert:
        case ERowOp::Reset:
            return state;
        case ERowOp::Erase:
            return nullptr;
        default:
            Y_FAIL_S("Unexpected row op: " << static_cast<ui8>(state->GetRowState()));
        }
    }

} // anonymous

bool TCdcStreamChangeCollector::NeedToReadKeys() const {
    if (CachedNeedToReadKeys) {
        return *CachedNeedToReadKeys;
    }

    for (const auto& [_, tableInfo] : Self->GetUserTables()) {
        for (const auto& [_, streamInfo] : tableInfo->CdcStreams) {
            switch (streamInfo.Mode) {
            case NKikimrSchemeOp::ECdcStreamModeKeysOnly:
            case NKikimrSchemeOp::ECdcStreamModeUpdate:
                CachedNeedToReadKeys = false;
                break;
            case NKikimrSchemeOp::ECdcStreamModeNewImage:
            case NKikimrSchemeOp::ECdcStreamModeOldImage:
            case NKikimrSchemeOp::ECdcStreamModeNewAndOldImages:
                CachedNeedToReadKeys = true;
                break;
            default:
                Y_FAIL_S("Invalid stream mode: " << static_cast<ui32>(streamInfo.Mode));
            }
        }
    }

    Y_VERIFY(CachedNeedToReadKeys);
    return *CachedNeedToReadKeys;
}

void TCdcStreamChangeCollector::SetReadVersion(const TRowVersion& readVersion) {
    ReadVersion = readVersion;
}

bool TCdcStreamChangeCollector::Collect(const TTableId& tableId, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TUpdateOp> updates)
{
    Y_VERIFY_S(Self->IsUserTable(tableId), "Unknown table: " << tableId);
    const auto localTableId = Self->GetLocalTableId(tableId);

    auto userTable = Self->GetUserTables().at(tableId.PathId.LocalPathId);
    const auto& keyTags = userTable->KeyColumnIds;

    Y_VERIFY_S(key.size() == keyTags.size(), "Count doesn't match"
        << ": key# " << key.size()
        << ", tags# " << keyTags.size());

    switch (rop) {
    case ERowOp::Upsert:
    case ERowOp::Erase:
    case ERowOp::Reset:
        break;
    default:
        Y_FAIL_S("Unsupported row op: " << static_cast<ui8>(rop));
    }

    bool read = false;

    for (const auto& [pathId, stream] : userTable->CdcStreams) {
        switch (stream.Mode) {
        case NKikimrSchemeOp::ECdcStreamModeKeysOnly:
            Persist(tableId, pathId, rop, key, keyTags, {});
            break;
        case NKikimrSchemeOp::ECdcStreamModeUpdate:
            Persist(tableId, pathId, rop, key, keyTags, updates);
            break;
        case NKikimrSchemeOp::ECdcStreamModeNewImage:
        case NKikimrSchemeOp::ECdcStreamModeOldImage:
        case NKikimrSchemeOp::ECdcStreamModeNewAndOldImages: {
            const auto valueTags = MakeValueTags(userTable->Columns);
            const auto oldState = GetCurrentState(localTableId, key, keyTags, valueTags);

            if (!oldState) {
                return false;
            }

            if (stream.Mode == NKikimrSchemeOp::ECdcStreamModeOldImage) {
                Persist(tableId, pathId, rop, key, keyTags, NullIfErased(&*oldState), nullptr, valueTags);
            } else {
                const auto newState = PatchState(*oldState, rop, MakeTagToPos(valueTags), MappedUpdates(updates));

                if (stream.Mode == NKikimrSchemeOp::ECdcStreamModeNewImage) {
                    Persist(tableId, pathId, rop, key, keyTags, nullptr, NullIfErased(&newState), valueTags);
                } else {
                    Persist(tableId, pathId, rop, key, keyTags, NullIfErased(&*oldState), NullIfErased(&newState), valueTags);
                }
            }

            read = true;
            break;
        }
        default:
            Y_FAIL_S("Invalid stream mode: " << static_cast<ui32>(stream.Mode));
        }
    }

    if (read) {
        RowsCache.UpdateCachedRow(localTableId, rop, key, updates);
    }

    return true;
}

TMaybe<TRowState> TCdcStreamChangeCollector::GetCurrentState(ui32 tid, TArrayRef<const TRawTypeValue> key,
        TArrayRef<const TTag> keyTags, TArrayRef<const TTag> valueTags)
{
    TRowState row;
    const auto ready = RowsCache.SelectRow(Db, tid, key, MakeTagToPos(keyTags), valueTags, row, ReadVersion);

    if (ready == EReady::Page) {
        return Nothing();
    }

    return row;
}

TRowState TCdcStreamChangeCollector::PatchState(const TRowState& oldState, ERowOp rop,
        const THashMap<TTag, TPos>& tagToPos, const THashMap<TTag, TUpdateOp>& updates)
{
    TRowState newState;

    newState.Init(tagToPos.size());
    newState.Touch(rop);

    switch (rop) {
    case ERowOp::Upsert:
    case ERowOp::Reset:
        for (const auto [tag, pos] : tagToPos) {
            auto it = updates.find(tag);
            if (it != updates.end()) {
                newState.Set(pos, it->second.Op, it->second.AsCell());
            } else if (rop == ERowOp::Upsert) {
                newState.Set(pos, oldState.GetCellOp(pos), oldState.Get(pos));
            } else {
                newState.Set(pos, ECellOp::Null, TCell());
            }
        }
        break;
    case ERowOp::Erase:
        break;
    default:
        Y_FAIL("unreachable");
    }

    Y_VERIFY(newState.IsFinalized());
    return newState;
}

void TCdcStreamChangeCollector::Persist(const TTableId& tableId, const TPathId& pathId, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TTag> keyTags, TArrayRef<const TUpdateOp> updates)
{
    NKikimrChangeExchange::TChangeRecord::TDataChange body;
    Serialize(body, rop, key, keyTags, updates);
    TBaseChangeCollector::Persist(tableId, pathId, TChangeRecord::EKind::CdcDataChange, body);
}

void TCdcStreamChangeCollector::Persist(const TTableId& tableId, const TPathId& pathId, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TTag> keyTags,
        const TRowState* oldState, const TRowState* newState, TArrayRef<const TTag> valueTags)
{
    if (!oldState && !newState) {
        return;
    }

    NKikimrChangeExchange::TChangeRecord::TDataChange body;
    Serialize(body, rop, key, keyTags, oldState, newState, valueTags);
    TBaseChangeCollector::Persist(tableId, pathId, TChangeRecord::EKind::CdcDataChange, body);
}

} // NDataShard
} // NKikimr
