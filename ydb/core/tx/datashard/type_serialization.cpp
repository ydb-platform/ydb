#include "type_serialization.h"

#include <yql/essentials/types/dynumber/dynumber.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <util/string/builder.h>

namespace NKikimr::NDataShard {

TString DecimalToString(const std::pair<ui64, i64>& loHi, const NScheme::TTypeInfo& typeInfo) {
    using namespace NYql::NDecimal;

    TInt128 val = FromHalfs(loHi.first, loHi.second);
    const char* result = ToString(val, MaxPrecision /*typeInfo.GetDecimalType().GetPrecision()*/, typeInfo.GetDecimalType().GetScale());
    Y_ENSURE(result);

    return result;
}

TString DyNumberToString(TStringBuf data) {
    TString result;
    TStringOutput out(result);
    TStringBuilder err;

    bool success = DyNumberToStream(data, out, err);
    Y_ENSURE(success);

    return result;
}

bool DecimalToStream(const std::pair<ui64, i64>& loHi, IOutputStream& out, TString& err, const NScheme::TTypeInfo& typeInfo) {
    using namespace NYql::NDecimal;

    TInt128 val = FromHalfs(loHi.first, loHi.second);
    const char* result = ToString(val, MaxPrecision /*typeInfo.GetDecimalType().GetPrecision()*/, typeInfo.GetDecimalType().GetScale());
    if (!result) [[unlikely]] {
        err = "Invalid Decimal binary representation";
        return false;
    }
    out << result;
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

bool UuidToStream(const std::pair<ui64, ui64>& loHi, IOutputStream& out, TString& err) {
    Y_UNUSED(err);

    NYdb::TUuidValue uuid(loHi.first, loHi.second);
    
    out << uuid.ToString();
    
    return true;
}

} // NKikimr::NDataShard
