#pragma once

#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/services/sql/parse/parser.h>

namespace NKikimr::NSql {

struct TResolvedType {
    TString Name;
    TString SqlState;
    TString Message;

    bool Ok() const {
        return SqlState.empty();
    }
};

// Unquoted PostgreSQL names map to a YDB type. Quoted names are YDB types.
// date has no Date64 type, so it is rejected and must not be proposed.
TResolvedType ResolveColumnType(const TPgType& type);

// Fills DefaultFromLiteral. Returns false when the literal cannot be stored.
bool FillDefaultLiteral(Ydb::TypedValue& value, const TString& ydbType, const TLiteral& literal, TString& message);

} // namespace NKikimr::NSql
