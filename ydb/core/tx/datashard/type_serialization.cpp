#include "type_serialization.h"

#include <ydb/library/dynumber/dynumber.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <library/cpp/string_utils/quote/quote.h>

#include <util/string/builder.h>

namespace NKikimr::NDataShard {

TString DecimalToString(const std::pair<ui64, i64>& loHi, const NScheme::TTypeInfo& typeInfo) {
    using namespace NYql::NDecimal;

    TInt128 val = FromHalfs(loHi.first, loHi.second);
    return ToString(val, typeInfo.GetDecimalType().GetPrecision(), typeInfo.GetDecimalType().GetScale());
}

TString DyNumberToString(TStringBuf data) {
    TString result;
    TStringOutput out(result);
    TStringBuilder err;

    bool success = DyNumberToStream(data, out, err);
    Y_ABORT_UNLESS(success);

    return result;
}

TString PgToString(TStringBuf data, const NScheme::TTypeInfo& typeInfo) {
    const NPg::TConvertResult& pgResult = NPg::PgNativeTextFromNativeBinary(data, typeInfo.GetPgTypeDesc());
    Y_ABORT_UNLESS(pgResult.Error.Empty());
    return pgResult.Str;
}

bool DecimalToStream(const std::pair<ui64, i64>& loHi, IOutputStream& out, TString& err, const NScheme::TTypeInfo& typeInfo) {
    Y_UNUSED(err);
    using namespace NYql::NDecimal;

    TInt128 val = FromHalfs(loHi.first, loHi.second);
    out << ToString(val, typeInfo.GetDecimalType().GetPrecision(), typeInfo.GetDecimalType().GetScale());
    return true;
}

bool DyNumberToStream(TStringBuf data, IOutputStream& out, TString& err) {
    auto result = NDyNumber::DyNumberToString(data);
    if (!result.Defined()) {
        err = "Invalid DyNumber binary representation";
        return false;
    }
    out << *result;
    return true;
}

bool PgToStream(TStringBuf data, const NScheme::TTypeInfo& typeInfo, IOutputStream& out, TString& err) {
    const NPg::TConvertResult& pgResult = NPg::PgNativeTextFromNativeBinary(data, typeInfo.GetPgTypeDesc());
    if (pgResult.Error) {
        err = *pgResult.Error;
        return false;
    }
    out << '"' << CGIEscapeRet(pgResult.Str) << '"';
    return true;
}

bool UuidToStream(const std::pair<ui64, ui64>& loHi, IOutputStream& out, TString& err) {
    Y_UNUSED(err);

    NYdb::TUuidValue uuid(loHi.first, loHi.second);
    
    out << uuid.ToString();
    
    return true;
}

} // NKikimr::NDataShard
