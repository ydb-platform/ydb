#include "yql_type_parser.h"

#include <library/cpp/yson/node/node_io.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

namespace NYql {
namespace NCommon {

void TYqlTypeYsonSaverBase::SaveTypeHeader(TStringBuf name) {
    Writer.OnBeginList();
    Writer.OnListItem();
    Writer.OnStringScalar(name);
}

#define SAVE_TYPE_IMPL(type) \
void TYqlTypeYsonSaverBase::Save ## type() { \
    SaveTypeHeader(#type); \
    Writer.OnEndList(); \
}

SAVE_TYPE_IMPL(Type)
SAVE_TYPE_IMPL(VoidType)
SAVE_TYPE_IMPL(NullType)
SAVE_TYPE_IMPL(UnitType)
SAVE_TYPE_IMPL(GenericType)
SAVE_TYPE_IMPL(EmptyListType)
SAVE_TYPE_IMPL(EmptyDictType)

#undef SAVE_TYPE_IMPL

void TYqlTypeYsonSaverBase::SaveDataType(const TStringBuf& dataType) {
    SaveTypeHeader("DataType");
    Writer.OnListItem();
    Writer.OnStringScalar(dataType);
    Writer.OnEndList();
}

void TYqlTypeYsonSaverBase::SavePgType(const TStringBuf& pgType) {
    SaveTypeHeader("PgType");
    Writer.OnListItem();
    Writer.OnStringScalar(pgType);
    if (ExtendedForm) {
        Writer.OnListItem();
        const auto& desc = NYql::NPg::LookupType(TString(pgType));
        char cat = desc.Category;
        if (desc.ArrayTypeId == desc.TypeId) {
            cat = NYql::NPg::LookupType(desc.ElementTypeId).Category;
        }
        
        Writer.OnStringScalar(TStringBuf(&cat, 1));
    }
    
    Writer.OnEndList();
}

void TYqlTypeYsonSaverBase::SaveDataTypeParams(const TStringBuf& dataType, const TStringBuf& paramOne, const TStringBuf& paramTwo) {
    SaveTypeHeader("DataType");
    Writer.OnListItem();
    Writer.OnStringScalar(dataType);
    Writer.OnListItem();
    Writer.OnStringScalar(paramOne);
    Writer.OnListItem();
    Writer.OnStringScalar(paramTwo);
    Writer.OnEndList();
}

void TYqlTypeYsonSaverBase::SaveResourceType(const TStringBuf& tag) {
    SaveTypeHeader("ResourceType");
    Writer.OnListItem();
    Writer.OnStringScalar(tag);
    Writer.OnEndList();
}

bool ParseYson(NYT::TNode& res, const TStringBuf yson, IOutputStream& err) {
    try {
        res = NYT::NodeFromYsonString(yson);
    }
    catch (const yexception& e) {
        err << "Failed to parse scheme from YSON: " << e.what();
        return false;
    }
    return true;
}

} // namespace NCommon
} // namespace NYql
