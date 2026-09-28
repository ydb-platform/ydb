#include "flat_cxx_database.h"

namespace NKikimr::NIceDb::NPrivate {

TString StripNamespace(const TString& typeName) {
    return typeName.substr(typeName.rfind(':') + 1);
}

void MaterializeColumn(TToughDb& database, ui32 tableId, const std::type_info& columnTypeInfo, TGetNameFunc getName,
    ui32 columnId, NScheme::TTypeId columnType, bool isNotNull, bool isSensitive, bool isSetNotNullInProgress)
{
    database.Alter().AddColumn(tableId, getName(TypeName(columnTypeInfo)), columnId, columnType, isNotNull, isSensitive, { }, isSetNotNullInProgress);
}

bool MaterializeTable(TToughDb& database, ui32 tableId, const std::type_info& tableTypeInfo, EMaterializationMode mode) {
    switch (mode) {
    case EMaterializationMode::All:
        break;
    case EMaterializationMode::Existing:
        if (!database.GetScheme().GetTableInfo(tableId)) {
            return false;
        }
        break;
    case EMaterializationMode::NonExisting:
        if (database.GetScheme().GetTableInfo(tableId)) {
            return false;
        }
        break;
    }

    database.Alter().AddTable(StripNamespace(TypeName(tableTypeInfo)), tableId);
    return true;
}

} // namespace NKikimr::NIceDb::NPrivate
