#include "change_collector_cdc_stream.h"
#include "datashard_impl.h"
#include "datashard_user_db.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;
using namespace NTable;

namespace {

    auto MakeKeyCells(TArrayRef<const TRawTypeValue> key) {
        TVector<TCell> result(Reserve(key.size()));

        for (TPos pos = 0; pos < key.size(); ++pos) {
            result.emplace_back(key.at(pos).AsRef());
        }

        return result;
    }

    auto MakeValueTags(const TMap<TTag, TUserTable::TUserColumn>& columns) {
        TVector<TTag> result(Reserve(columns.size() - 1));

        for (const auto& [tag, column] : columns) {
            if (!column.IsKey) {
                result.push_back(tag);
            }
        }

        return result;
    }

    auto MakeValueTypes(const TMap<TTag, TUserTable::TUserColumn>& columns) {
        TVector<NScheme::TTypeInfo> result(Reserve(columns.size() - 1));

        for (const auto& [_, column] : columns) {
            if (!column.IsKey) {
                result.push_back(column.Type);
            }
        }

        return result;
    }

    auto MakeUpdates(TArrayRef<const TCell> cells, TArrayRef<const TTag> tags, TArrayRef<const NScheme::TTypeInfo> types) {
        TVector<TUpdateOp> result(Reserve(cells.size()));

        Y_ABORT_UNLESS(cells.size() == tags.size());
        Y_ABORT_UNLESS(cells.size() == types.size());

        for (TPos pos = 0; pos < cells.size(); ++pos) {
            result.emplace_back(tags.at(pos), ECellOp::Set, TRawTypeValue(cells.at(pos).AsRef(), types.at(pos)));
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
        case ERowOp::Absent:
        case ERowOp::Erase:
            return nullptr;
        default:
            Y_FAIL_S("Unexpected row op: " << static_cast<int>(state->GetRowState()));
        }
    }

    struct TVersionContext {
        IBaseChangeCollectorSink& Sink;
        IBaseChangeCollectorSink::TVersionState SavedState;

        TVersionContext(IBaseChangeCollectorSink& sink, const TRowVersion& replace)
            : Sink(sink)
            , SavedState(sink.GetVersionState())
        {
            Sink.SetVersionState({
                .WriteVersion = replace,
                .WriteTxId = 0,
            });
        }

        ~TVersionContext() {
            Sink.SetVersionState(SavedState);
        }
    };

} // anonymous

void TCdcStreamChangeCollector::OnRestart() {
    TBaseChangeCollector::OnRestart();
}

bool TCdcStreamChangeCollector::NeedToReadKeys() const {
    if (CachedNeedToReadKeys) {
        return *CachedNeedToReadKeys;
    }

    bool value = false;
    for (const auto& [_, tableInfo] : Self->GetUserTables()) {
        for (const auto& [_, streamInfo] : tableInfo->CdcStreams) {
            if (streamInfo.State == NKikimrSchemeOp::ECdcStreamStateDisabled) {
                continue;
            }

            switch (streamInfo.Mode) {
            case NKikimrSchemeOp::ECdcStreamModeKeysOnly:
            case NKikimrSchemeOp::ECdcStreamModeUpdate:
                break;
            case NKikimrSchemeOp::ECdcStreamModeNewImage:
            case NKikimrSchemeOp::ECdcStreamModeOldImage:
            case NKikimrSchemeOp::ECdcStreamModeNewAndOldImages:
                value = true;
                break;
            default:
                Y_FAIL_S("Invalid stream mode: " << static_cast<ui32>(streamInfo.Mode));
            }
        }
    }

    CachedNeedToReadKeys = value;
    return *CachedNeedToReadKeys;
}

bool TCdcStreamChangeCollector::Collect(const TTableId& tableId, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TUpdateOp> updates)
{
    Y_VERIFY_S(Self->IsUserTable(tableId), "Unknown table: " << tableId);

    auto userTable = Self->GetUserTables().at(tableId.PathId.LocalPathId);
    const auto& keyTags = userTable->KeyColumnIds;
    const auto& keyTypes = userTable->KeyColumnTypes;
    const auto valueTags = MakeValueTags(userTable->Columns);
    const auto valueTypes = MakeValueTypes(userTable->Columns);

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

    for (const auto& [pathId, stream] : userTable->CdcStreams) {
        TMaybe<TRowState> initialState;
        TMaybe<TRowVersion> snapshotVersion;

        switch (stream.State) {
        case NKikimrSchemeOp::ECdcStreamStateDisabled:
            continue; // do not generate change record at all
        case NKikimrSchemeOp::ECdcStreamStateScan:
            if (const auto* info = Self->GetCdcStreamScanManager().Get(pathId)) {
                snapshotVersion = info->SnapshotVersion;

                TSelectStats stats;
                const auto stateAtSnapshot = GetState(tableId, key, valueTags, stats, info->SnapshotVersion);
                if (!stateAtSnapshot) {
                    return false;
                }

                const auto state = stateAtSnapshot->GetRowState();
                const bool presentInSnapshot = (state == ERowOp::Upsert || state == ERowOp::Reset);

                if (!presentInSnapshot || stats.InvisibleRowSkips) {
                    // just generate change record
                } else if (!info->LastKey) {
                    initialState = stateAtSnapshot;
                } else {
                    const auto& lastKeyCells = info->LastKey->GetCells();
                    const auto keyCells = MakeKeyCells(key);

                    Y_ABORT_UNLESS(keyCells.size() == lastKeyCells.size());
                    Y_ABORT_UNLESS(keyCells.size() == keyTypes.size());

                    const int cmp = CompareTypedCellVectors(keyCells.data(), lastKeyCells.data(), keyTypes.data(), keyCells.size());
                    if (cmp > 0) {
                        initialState = stateAtSnapshot;
                    }
                }
            } else {
                // nop, scan is completed
            }
            break;
        default:
            break;
        }

        if (initialState) {
            Y_ABORT_UNLESS(snapshotVersion.Defined());
            TVersionContext ctx(Sink, *snapshotVersion);

            switch (stream.Mode) {
            case NKikimrSchemeOp::ECdcStreamModeKeysOnly:
                Persist(tableId, pathId, ERowOp::Upsert, key, keyTags, {});
                break;
            case NKikimrSchemeOp::ECdcStreamModeUpdate:
                Persist(tableId, pathId, ERowOp::Upsert, key, keyTags, MakeUpdates(**initialState, valueTags, valueTypes));
                break;
            case NKikimrSchemeOp::ECdcStreamModeRestoreIncrBackup: {
                Y_FAIL_S("Invariant violation: source table must be locked before restore.");
                break;
            }
            case NKikimrSchemeOp::ECdcStreamModeNewImage:
            case NKikimrSchemeOp::ECdcStreamModeNewAndOldImages:
                Persist(tableId, pathId, ERowOp::Upsert, key, keyTags, nullptr, &*initialState, valueTags);
                break;
            case NKikimrSchemeOp::ECdcStreamModeOldImage:
                Persist(tableId, pathId, ERowOp::Upsert, key, keyTags, &*initialState, nullptr, valueTags);
                break;
            default:
                Y_FAIL_S("Invalid stream mode: " << static_cast<ui32>(stream.Mode));
            }
        }

        switch (stream.Mode) {
        case NKikimrSchemeOp::ECdcStreamModeKeysOnly:
            Persist(tableId, pathId, rop, key, keyTags, {});
            break;
        case NKikimrSchemeOp::ECdcStreamModeUpdate:
            Persist(tableId, pathId, rop, key, keyTags, updates);
            break;
        case NKikimrSchemeOp::ECdcStreamModeRestoreIncrBackup:
            Y_FAIL_S("Invariant violation: source table must be locked before restore.");
        case NKikimrSchemeOp::ECdcStreamModeNewImage:
        case NKikimrSchemeOp::ECdcStreamModeOldImage:
        case NKikimrSchemeOp::ECdcStreamModeNewAndOldImages:
            if (const auto oldState = GetState(tableId, key, valueTags)) {
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
            } else {
                return false;
            }
            break;
        default:
            Y_FAIL_S("Invalid stream mode: " << static_cast<ui32>(stream.Mode));
        }
    }

    return true;
}

TMaybe<TRowState> TCdcStreamChangeCollector::GetState(const TTableId& tableId, TArrayRef<const TRawTypeValue> key,
        TArrayRef<const TTag> valueTags, TSelectStats& stats, const TMaybe<TRowVersion>& readVersion)
{
    TRowState row;
    const auto ready = UserDb.SelectRow(tableId, key, valueTags, row, stats, readVersion);

    if (ready == EReady::Page) {
        return Nothing();
    }

    return row;
}

TMaybe<TRowState> TCdcStreamChangeCollector::GetState(const TTableId& tableId, TArrayRef<const TRawTypeValue> key,
        TArrayRef<const TTag> valueTags, const TMaybe<TRowVersion>& readVersion)
{
    TSelectStats stats;
    return GetState(tableId, key, valueTags, stats, readVersion);
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
                // Copy value from the old state, this also handles schema default values
                newState.Set(pos, ECellOp::Set, oldState.Get(pos));
            } else {
                // FIXME: reset fills columns with schema defaults, which are currently always null
                newState.Set(pos, ECellOp::Null, TCell());
            }
        }
        break;
    case ERowOp::Erase:
        break;
    default:
        Y_ABORT("unreachable");
    }

    Y_ABORT_UNLESS(newState.IsFinalized());
    return newState;
}

void TCdcStreamChangeCollector::Persist(const TTableId& tableId, const TPathId& pathId, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TTag> keyTags, TArrayRef<const TUpdateOp> updates)
{
    NKikimrChangeExchange::TDataChange body;
    Serialize(body, rop, key, keyTags, updates);
    Sink.AddChange(tableId, pathId, TChangeRecord::EKind::CdcDataChange, body);
}

void TCdcStreamChangeCollector::Persist(const TTableId& tableId, const TPathId& pathId, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TTag> keyTags,
        const TRowState* oldState, const TRowState* newState, TArrayRef<const TTag> valueTags)
{
    NKikimrChangeExchange::TDataChange body;
    Serialize(body, rop, key, keyTags, oldState, newState, valueTags);
    Sink.AddChange(tableId, pathId, TChangeRecord::EKind::CdcDataChange, body);
}

} // NDataShard
} // NKikimr
