#include "scheme_type_registry.h"
#include "scheme_types.h"
#include "scheme_types_defs.h"

#include "scheme_decimal_type.h"
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>

#include <util/digest/murmur.h>
#include <util/generic/algorithm.h>
#include <util/string/printf.h>

#define REGISTER_TYPE(name, size, ...) RegisterType<T##name>();

namespace NKikimr {
namespace NScheme {

TTypeRegistry::TTypeRegistry()
{
    // move to 'init defaults?'
    RegisterType<TInt8>();
    RegisterType<TUint8>();
    RegisterType<TInt16>();
    RegisterType<TUint16>();
    RegisterType<TInt32>();
    RegisterType<TUint32>();
    RegisterType<TInt64>();
    RegisterType<TUint64>();
    RegisterType<TDouble>();
    RegisterType<TFloat>();
    RegisterType<TBool>();
    RegisterType<TPairUi64Ui64>();
    RegisterType<TString>();
    RegisterType<TSmallBoundedString>();
    RegisterType<TLargeBoundedString>();
    RegisterType<TUtf8>();
    RegisterType<TYson>();
    RegisterType<TJson>();
    RegisterType<TJsonDocument>();
    RegisterType<TDate>();
    RegisterType<TDatetime>();
    RegisterType<TTimestamp>();
    RegisterType<TInterval>();
    RegisterType<TDyNumber>();
    RegisterType<TUuid>();
    RegisterType<TDate32>();
    RegisterType<TDatetime64>();
    RegisterType<TTimestamp64>();
    RegisterType<TInterval64>();
}

void TTypeRegistry::CalculateMetadataEtag() {
}

bool TTypeRegistry::GetTypeInfo(const TStringBuf& typeName, const TStringBuf& columnName, NScheme::TTypeInfo &typeInfo, ::TString& errorStr) const {
    if (const NScheme::IType* type = GetType(typeName)) {
        // Only allow YQL types
        if (!NScheme::NTypeIds::IsYqlType(type->GetTypeId())) {
            errorStr = Sprintf("Type '%s' specified for column '%s' is no longer supported", typeName.data(), columnName.data());
            return false;
        }
        typeInfo = NScheme::TTypeInfo(type->GetTypeId());
    } else if (const auto decimalType = NScheme::TDecimalType::ParseTypeName(typeName)) {
        typeInfo = NScheme::TTypeInfo(NScheme::NTypeIds::Decimal, *decimalType);
    } else if (const auto pgTypeDesc = NPg::TypeDescFromPgTypeName(typeName)) {
        typeInfo = NScheme::TTypeInfo(NScheme::NTypeIds::Pg, pgTypeDesc);
    } else {
        errorStr = Sprintf("Type '%s' specified for column '%s' is not supported by storage", typeName.data(), columnName.data());
        return false;
    }
    return true;
}

} // namespace NScheme
} // namespace NKikimr
