#pragma once

#include "schemeshard_private.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <variant>

namespace NKikimr::NSchemeShard::TEvPrivate {

struct TEvImportSchemeQueryResult: public TEventLocal<TEvImportSchemeQueryResult, EvImportSchemeQueryResult> {
    const ui64 ImportId;
    const ui32 ItemIdx;
    const Ydb::StatusIds::StatusCode Status;
    const std::variant<TString, NKikimrSchemeOp::TModifyScheme> Result;

    TEvImportSchemeQueryResult(ui64 id, ui32 itemIdx, Ydb::StatusIds::StatusCode status, TString&& error)
        : ImportId(id)
        , ItemIdx(itemIdx)
        , Status(status)
        , Result(std::move(error))
    {}

    TEvImportSchemeQueryResult(ui64 id, ui32 itemIdx, Ydb::StatusIds::StatusCode status, NKikimrSchemeOp::TModifyScheme&& preparedQuery)
        : ImportId(id)
        , ItemIdx(itemIdx)
        , Status(status)
        , Result(std::move(preparedQuery))
    {}
};

} // namespace NKikimr::NSchemeShard::TEvPrivate
