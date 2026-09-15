#include "yql_type_parser.h"

#include <library/cpp/yson/node/node_io.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>

namespace NYql::NCommon {

void TYqlTypeYsonSaverBase::SaveTypeHeader(TStringBuf name) {
    Writer_.OnBeginList();
    Writer_.OnListItem();
    Writer_.OnStringScalar(name);
}

#define SAVE_TYPE_IMPL(type)                   \
    void TYqlTypeYsonSaverBase::Save##type() { \
        SaveTypeHeader(#type);                 \
        Writer_.OnEndList();                   \
    }

SAVE_TYPE_IMPL(Type)
SAVE_TYPE_IMPL(VoidType)
SAVE_TYPE_IMPL(NullType)
SAVE_TYPE_IMPL(UnitType)
SAVE_TYPE_IMPL(UniversalType)
SAVE_TYPE_IMPL(UniversalStructType)
SAVE_TYPE_IMPL(GenericType)
SAVE_TYPE_IMPL(EmptyListType)
SAVE_TYPE_IMPL(EmptyDictType)

#undef SAVE_TYPE_IMPL

void TYqlTypeYsonSaverBase::SaveDataType(const TStringBuf& dataType) {
    SaveTypeHeader("DataType");
    Writer_.OnListItem();
    Writer_.OnStringScalar(dataType);
    Writer_.OnEndList();
}

void TYqlTypeYsonSaverBase::SavePgType(const TStringBuf& pgType) {
    SaveTypeHeader("PgType");
    Writer_.OnListItem();
    Writer_.OnStringScalar(pgType);
    if (ExtendedForm_) {
        Writer_.OnListItem();
        const auto& desc = NYql::NPg::LookupType(TString(pgType));
        char cat = desc.Category;
        if (desc.ArrayTypeId == desc.TypeId) {
            cat = NYql::NPg::LookupType(desc.ElementTypeId).Category;
        }

        Writer_.OnStringScalar(TStringBuf(&cat, 1));
    }

    Writer_.OnEndList();
}

void TYqlTypeYsonSaverBase::SaveDataTypeParams(const TStringBuf& dataType, const TStringBuf& paramOne, const TStringBuf& paramTwo) {
    SaveTypeHeader("DataType");
    Writer_.OnListItem();
    Writer_.OnStringScalar(dataType);
    Writer_.OnListItem();
    Writer_.OnStringScalar(paramOne);
    Writer_.OnListItem();
    Writer_.OnStringScalar(paramTwo);
    Writer_.OnEndList();
}

void TYqlTypeYsonSaverBase::SaveResourceType(const TStringBuf& tag) {
    SaveTypeHeader("ResourceType");
    Writer_.OnListItem();
    Writer_.OnStringScalar(tag);
    Writer_.OnEndList();
}

bool ParseYson(NYT::TNode& res, const TStringBuf yson, IOutputStream& err) {
    try {
        res = NYT::NodeFromYsonString(yson);
    } catch (const yexception& e) {
        err << "Failed to parse scheme from YSON: " << e.what();
        return false;
    }
    return true;
}

} // namespace NYql::NCommon
