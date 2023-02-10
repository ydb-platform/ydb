#define INCLUDE_YDB_INTERNAL_H
#include "helpers.h"
#include <ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers/handlers.h>
#include <util/string/builder.h>

namespace NYdb {

bool TypesEqual(const Ydb::Type& t1, const Ydb::Type& t2) {
    if (t1.type_case() != t2.type_case()) {
        return false;
    }

    switch (t1.type_case()) {
        case Ydb::Type::kTypeId:
            return t1.type_id() == t2.type_id();
        case Ydb::Type::kDecimalType:
            return t1.decimal_type().precision() == t2.decimal_type().precision()
                && t1.decimal_type().scale() == t2.decimal_type().scale();
        case Ydb::Type::kPgType:
            return t1.pg_type().type_name() == t2.pg_type().type_name()
                && t1.pg_type().type_modifier() == t2.pg_type().type_modifier();
        case Ydb::Type::kOptionalType:
            return TypesEqual(t1.optional_type().item(), t2.optional_type().item());
        case Ydb::Type::kTaggedType:
            return t1.tagged_type().tag() == t2.tagged_type().tag() &&
                TypesEqual(t1.tagged_type().type(), t2.tagged_type().type());
        case Ydb::Type::kListType:
            return TypesEqual(t1.list_type().item(), t2.list_type().item());
        case Ydb::Type::kTupleType:
            if (t1.tuple_type().elements_size() != t2.tuple_type().elements_size()) {
                return false;
            }
            for (size_t i = 0; i < (size_t)t1.tuple_type().elements_size(); ++i) {
                if (!TypesEqual(t1.tuple_type().elements(i), t2.tuple_type().elements(i))) {
                    return false;
                }
            }
            return true;
        case Ydb::Type::kStructType:
            if (t1.struct_type().members_size() != t2.struct_type().members_size()) {
                return false;
            }
            for (size_t i = 0; i < (size_t)t1.struct_type().members_size(); ++i) {
                auto& m1 = t1.struct_type().members(i);
                auto& m2 = t2.struct_type().members(i);
                if (m1.name() != m2.name()) {
                    return false;
                }

                if (!TypesEqual(m1.type(), m2.type())) {
                    return false;
                }
            }
            return true;
        case Ydb::Type::kDictType:
            return TypesEqual(t1.dict_type().key(), t2.dict_type().key())
                && TypesEqual(t1.dict_type().payload(), t2.dict_type().payload());
        case Ydb::Type::kVariantType: {
            const auto& v1 = t1.variant_type();
            const auto& v2 = t2.variant_type();
            if (v1.type_case() != v2.type_case()) {
                return false;
            }
            switch (v1.type_case()) {
                case Ydb::VariantType::kTupleItems:
                    if (v1.tuple_items().elements_size() != v2.tuple_items().elements_size()) {
                        return false;
                    }
                    for (size_t i = 0; i < (size_t)v1.tuple_items().elements_size(); ++i) {
                        if (!TypesEqual(v1.tuple_items().elements(i), v2.tuple_items().elements(i))) {
                            return false;
                        }
                    }
                    return true;
                case Ydb::VariantType::kStructItems:
                    if (v1.struct_items().members_size() != v2.struct_items().members_size()) {
                        return false;
                    }
                    for (size_t i = 0; i < (size_t)v1.struct_items().members_size(); ++i) {
                        auto& m1 = v1.struct_items().members(i);
                        auto& m2 = v2.struct_items().members(i);
                        if (m1.name() != m2.name()) {
                            return false;
                        }

                        if (!TypesEqual(m1.type(), m2.type())) {
                            return false;
                        }
                    }
                    return true;
                default:
                    ThrowFatalError(TStringBuilder() << "Unexpected Variant type case " << static_cast<int>(t1.type_case()));
                    return false;
            }
            return false;
        }
        case Ydb::Type::kVoidType:
            return true;
        case Ydb::Type::kNullType:
            return true;
        case Ydb::Type::kEmptyListType:
            return true;
        case Ydb::Type::kEmptyDictType:
            return true;
        default:
            ThrowFatalError(TStringBuilder() << "Unexpected type case " << static_cast<int>(t1.type_case()));
            return false;
    }
}

} // namespace NYdb
