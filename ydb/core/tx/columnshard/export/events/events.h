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

}