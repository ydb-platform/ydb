#include "json_change_record.h"

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/io_formats/cell_maker/cell_maker.h>

#include <util/memory/pool.h>

namespace NKikimr::NReplication::NService {

ui64 TChangeRecord::GetGroup() const {
    return 0;
}

ui64 TChangeRecord::GetStep() const {
    if (const auto* step = JsonBody.GetValueByPath("ts.[0]")) {
        return step->GetUIntegerRobust();
    }

    return 0;
}

ui64 TChangeRecord::GetTxId() const {
    if (const auto* txId = JsonBody.GetValueByPath("ts.[1]")) {
        return txId->GetUIntegerRobust();
    }

    return 0;
}

NChangeExchange::IChangeRecord::EKind TChangeRecord::GetKind() const {
    return JsonBody.Has("resolved")
        ? EKind::CdcHeartbeat
        : EKind::CdcDataChange;
}

void TChangeRecord::Serialize(NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& record) const {
    record.SetSourceOffset(GetOrder());
    // TODO: fill WriteTxId

    TMemoryPool pool(256);
    TString error;

    if (JsonBody.Has("key") && JsonBody["key"].IsArray()) {
        const auto& key = JsonBody["key"].GetArray();
        Y_ABORT_UNLESS(key.size() == Schema->KeyColumns.size());

        TVector<TCell> cells(key.size());
        for (ui32 i = 0; i < key.size(); ++i) {
            auto res = NFormats::MakeCell(cells[i], key[i], Schema->KeyColumns[i], pool, error);
            Y_ABORT_UNLESS(res);
        }

        record.SetKey(TSerializedCellVec::Serialize(cells));
    } else {
        Y_ABORT("Malformed json record");
    }

    if (JsonBody.Has("update") && JsonBody["update"].IsMap()) {
        const auto& update = JsonBody["update"].GetMap();
        auto& upsert = *record.MutableUpsert();

        TVector<TCell> cells(::Reserve(update.size()));
        for (const auto& [column, value] : update) {
            auto it = Schema->ValueColumns.find(column);
            Y_ABORT_UNLESS(it != Schema->ValueColumns.end());

            upsert.AddTags(it->second.Tag);
            auto res = NFormats::MakeCell(cells.emplace_back(), value, it->second.Type, pool, error);
            Y_ABORT_UNLESS(res);
        }

        upsert.SetData(TSerializedCellVec::Serialize(cells));
    } else if (JsonBody.Has("erase")) {
        record.MutableErase();
    } else {
        Y_ABORT("Malformed json record");
    }
}

}
