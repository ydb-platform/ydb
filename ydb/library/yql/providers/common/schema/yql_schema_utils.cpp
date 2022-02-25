#include "yql_schema_utils.h"

#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql {
namespace NCommon {

TVector<TString> ExtractColumnOrderFromYsonStructType(const NYT::TNode& node) {
    if (!node.IsList() || node.Size() < 1 || !node[0].IsString()) {
        YQL_ENSURE(!"Invalid type scheme");
    }

    auto typeName = node[0].AsString();
    YQL_ENSURE(typeName == "StructType");

    if (node.Size() != 2 || !node[1].IsList()) {
        YQL_ENSURE(!"Invalid struct type scheme");
    }

    TVector<TString> columns;
    for (auto& member : node[1].AsList()) {
        if (!member.IsList() || member.Size() != 2 || !member[0].IsString()) {
            YQL_ENSURE(!"Invalid struct type scheme");
        }

        columns.push_back(member[0].AsString());
    }

    return columns;
}

bool EqualsYsonTypesIgnoreStructOrder(const NYT::TNode& left, const NYT::TNode& right) {
    auto typeName = left[0].AsString();
    if (typeName != right[0].AsString()) {
        return false;
    }
    if (left.Size() != right.Size()) {
        return false;
    }
    if (typeName == "VoidType") {
        return true;
    } else if (typeName == "NullType") {
        return true;
    } else if (typeName == "UnitType") {
        return true;
    } else if (typeName == "GenericType") {
        return true;
    } else if (typeName == "EmptyListType") {
        return true;
    } else if (typeName == "EmptyDictType") {
        return true;
    } else if (typeName == "DataType" || typeName == "PgType") {
        return left == right;
    } else if (typeName == "ResourceType") {
        return left[1].AsString() == right[1].AsString();
    } else if (typeName == "TaggedType") {
        return left[1].AsString() == right[1].AsString()
            && EqualsYsonTypesIgnoreStructOrder(left[2], right[2]);
    } else if (typeName == "ErrorType") {
        return left[1].AsInt64() == right[1].AsInt64() && left[2].AsInt64() == right[2].AsInt64()
            && left[3].AsString() == right[3].AsString() && left[4].AsString() == right[4].AsString();
    } else if (typeName == "StructType") {
        if (left[1].Size() != right[1].Size()) {
            return false;
        }
        THashMap<TString, size_t> members;
        for (size_t i = 0; i < right[1].Size(); ++i) {
            members.emplace(right[1][i][0].AsString(), i);
        }
        for (auto& item : left[1].AsList()) {
            auto name = item[0].AsString();
            auto it = members.find(name);
            if (it == members.end()) {
                return false;
            }
            if (!EqualsYsonTypesIgnoreStructOrder(item[1], right[1][it->second][1])) {
                return false;
            }
        }
        return true;
    } else if (typeName == "ListType") {
        return EqualsYsonTypesIgnoreStructOrder(left[1], right[1]);
    } else if (typeName == "StreamType") {
        return EqualsYsonTypesIgnoreStructOrder(left[1], right[1]);
    } else if (typeName == "OptionalType") {
        return EqualsYsonTypesIgnoreStructOrder(left[1], right[1]);
    } else if (typeName == "TupleType") {
        for (size_t i = 0; i < left[1].Size(); ++i) {
            if (!EqualsYsonTypesIgnoreStructOrder(left[1][i], right[1][i])) {
                return false;
            }
        }
        return true;
    } else if (typeName == "DictType") {
        return EqualsYsonTypesIgnoreStructOrder(left[1], right[1])
            && EqualsYsonTypesIgnoreStructOrder(left[2], right[2]);
    } else if (typeName == "CallableType") {
        if (left[1].Size() != right[1].Size() || left[2].Size() != right[2].Size() || left[3].Size() != right[3].Size()) {
            return false;
        }
        if (left[1].Size() > 0) {
            if (left[1][0].AsUint64() != right[1][0].AsUint64()) {
                return false;
            }
            if (left[1].Size() > 1) {
                if (left[1][1].AsString() != right[1][1].AsString()) {
                    return false;
                }
            }
        }

        if (!EqualsYsonTypesIgnoreStructOrder(left[2][0], right[2][0])) {
            return false;
        }

        for (size_t i = 0; i < left[3].Size(); ++i) {
            if (!EqualsYsonTypesIgnoreStructOrder(left[3][i][0], right[3][i][0])) {
                return false;
            }
            for (size_t a = 1; a < left[3][i].Size(); ++a) {
                if (left[3][i][a] != right[3][i][a]) {
                    return false;
                }
            }
        }
        return true;
    } else if (typeName == "VariantType") {
        return EqualsYsonTypesIgnoreStructOrder(left[1], right[1]);
    }
    return false;
}

} // namespace NCommon
} // namespace NYql
