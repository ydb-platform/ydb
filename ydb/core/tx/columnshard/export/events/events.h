#pragma once
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/export/common/identifier.h>
#include <ydb/core/tx/columnshard/export/session/cursor.h>

namespace NKikimr::NOlap::NExport::NEvents {

struct TEvExportWritingFinished: public TEventLocal<TEvExportWritingFinished, NColumnShard::TEvPrivate::EvExportWritingFinished> {
};

struct TEvExportWritingFailed: public TEventLocal<TEvExportWritingFailed, NColumnShard::TEvPrivate::EvExportWritingFailed> {
};

struct TEvExportCursorSaved: public TEventLocal<TEvExportCursorSaved, NColumnShard::TEvPrivate::EvExportCursorSaved> {
};

class TEvExportSaveCursor: public TEventLocal<TEvExportSaveCursor, NColumnShard::TEvPrivate::EvExportSaveCursor> {
private:
    TIdentifier Identifier;
    TCursor Cursor;
public:
    const TIdentifier& GetIdentifier() const {
        return Identifier;
    }

    TCursor DetachCursor() {
        return std::move(Cursor);
    }

    TEvExportSaveCursor(const TIdentifier& id, const TCursor& cursor)
        : Identifier(id)
        , Cursor(cursor) {

    }
};

}