#include "factory.h"

#include "array.h"
#include "date.h"
#include "enum.h"
#include "nullable.h"
#include "numeric.h"
#include "string.h"
#include "tuple.h"

#include <library/cpp/clickhouse/client/types/type_parser.h>

namespace NClickHouse {
    namespace {
        TColumnRef CreateTerminalColumn(const TTypeAst& ast) {
            if (ast.Name == "UInt8")
                return TColumnUInt8::Create();
            if (ast.Name == "UInt16")
                return TColumnUInt16::Create();
            if (ast.Name == "UInt32")
                return TColumnUInt32::Create();
            if (ast.Name == "UInt64")
                return TColumnUInt64::Create();

            if (ast.Name == "Int8")
                return TColumnInt8::Create();
            if (ast.Name == "Int16")
                return TColumnInt16::Create();
            if (ast.Name == "Int32")
                return TColumnInt32::Create();
            if (ast.Name == "Int64")
                return TColumnInt64::Create();

            if (ast.Name == "Float32")
                return TColumnFloat32::Create();
            if (ast.Name == "Float64")
                return TColumnFloat64::Create();

            if (ast.Name == "String")
                return TColumnString::Create();
            if (ast.Name == "FixedString")
                return TColumnFixedString::Create(ast.Elements.front().Value);

            if (ast.Name == "DateTime")
                return TColumnDateTime::Create();
            if (ast.Name == "Date")
                return TColumnDate::Create();

            return nullptr;
        }

        TColumnRef CreateColumnFromAst(const TTypeAst& ast) {
            switch (ast.Meta) {
                case TTypeAst::Array: {
                    return TColumnArray::Create(
                        CreateColumnFromAst(ast.Elements.front()));
                }

                case TTypeAst::Nullable: {
                    return TColumnNullable::Create(
                        CreateColumnFromAst(ast.Elements.front()));
                }

                case TTypeAst::Terminal: {
                    return CreateTerminalColumn(ast);
                }

                case TTypeAst::Tuple: {
                    TVector<TColumnRef> columns;

                    for (const auto& elem : ast.Elements) {
                        if (auto col = CreateColumnFromAst(elem)) {
                            columns.push_back(col);
                        } else {
                            return nullptr;
                        }
                    }

                    return TColumnTuple::Create(columns);
                }

                case TTypeAst::Enum: {
                    TVector<TEnumItem> enum_items;

                    for (const auto& elem : ast.Elements) {
                        TString name(elem.Name);
                        i16 value = elem.Value;
                        enum_items.push_back({name, value});
                    }

                    if (ast.Name == "Enum8") {
                        return TColumnEnum8::Create(enum_items);
                    } else {
                        return TColumnEnum16::Create(enum_items);
                    }
                }

                case TTypeAst::Null:
                case TTypeAst::Number:
                    break;
            }

            return nullptr;
        }

    }

    TColumnRef CreateColumnByType(const TString& type_name) {
        TTypeAst ast;

        if (TTypeParser(type_name).Parse(&ast)) {
            return CreateColumnFromAst(ast);
        }

        return nullptr;
    }

}
