#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>
#include <ydb/public/lib/value/value.h>

namespace NKikimr {
namespace NClient {

TString TValue::GetPgText() const {
    Y_ASSERT(Type.GetKind() == NKikimrMiniKQL::ETypeKind::Pg);
    if (Value.HasNullFlagValue()) {
        return TString("null");
    }
    if (Value.HasText()) {
        return Value.GetText();
    }
    auto pgType = Type.GetPg();
    auto convertResult = NPg::PgNativeTextFromNativeBinary(Value.GetBytes(), NPg::TypeDescFromPgTypeId(pgType.Getoid()));
    Y_ENSURE(!convertResult.Error, convertResult.Error);
    return convertResult.Str;
}

}
}
