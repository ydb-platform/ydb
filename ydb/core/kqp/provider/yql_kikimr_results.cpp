#include "yql_kikimr_results.h"

#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/dynumber/dynumber.h>
#include <yql/essentials/types/uuid/uuid.h>

#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>
#include <yql/essentials/public/result_format/yql_codec_results.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

namespace NYql {

using namespace NNodes;

const TTypeAnnotationNode* ParseTypeFromYdbType(const Ydb::Type& type, TExprContext& ctx) {
    switch (type.type_case()) {
        case Ydb::Type::kVoidType: {
            return ctx.MakeType<TVoidExprType>();
        }

        case Ydb::Type::kTypeId: {
            auto slot = NKikimr::NUdf::FindDataSlot(type.type_id());
            if (!slot) {
                ctx.AddError(TIssue(TPosition(), TStringBuilder() << "Unsupported data type: "
                    << type.ShortDebugString()));

                return nullptr;
            }
            return ctx.MakeType<TDataExprType>(*slot);
        }

        case Ydb::Type::kDecimalType: {
            auto slot = NKikimr::NUdf::FindDataSlot(NYql::NProto::TypeIds::Decimal);
            if (!slot) {
                ctx.AddError(TIssue(TPosition(), TStringBuilder() << "Unsupported data type: "
                    << type.ShortDebugString()));

                return nullptr;
            }

            return ctx.MakeType<TDataExprParamsType>(*slot, ToString(type.decimal_type().precision()), ToString(type.decimal_type().scale()));
        }

        case Ydb::Type::kOptionalType: {
            auto itemType = ParseTypeFromYdbType(type.optional_type().item(), ctx);
            if (!itemType) {
                return nullptr;
            }

            return ctx.MakeType<TOptionalExprType>(itemType);
        }

        case Ydb::Type::kTupleType: {
            TTypeAnnotationNode::TListType tupleItems;

            for (auto& element : type.tuple_type().Getelements()) {
                auto elementType = ParseTypeFromYdbType(element, ctx);
                if (!elementType) {
                    return nullptr;
                }

                tupleItems.push_back(elementType);
            }

            return ctx.MakeType<TTupleExprType>(tupleItems);
        }

        case Ydb::Type::kListType: {
            auto itemType = ParseTypeFromYdbType(type.list_type().item(), ctx);
            if (!itemType) {
                return nullptr;
            }

            return ctx.MakeType<TListExprType>(itemType);
        }

        case Ydb::Type::kStructType: {
            TVector<const TItemExprType*> structMembers;
            for (auto& member : type.struct_type().Getmembers()) {
                auto memberType = ParseTypeFromYdbType(member.Gettype(), ctx);
                if (!memberType) {
                    return nullptr;
                }

                structMembers.push_back(ctx.MakeType<TItemExprType>(member.Getname(), memberType));
            }

            return ctx.MakeType<TStructExprType>(structMembers);
        }

        case Ydb::Type::kDictType: {
            auto keyType = ParseTypeFromYdbType(type.dict_type().key(), ctx);
            if (!keyType) {
                return nullptr;
            }

            auto payloadType = ParseTypeFromYdbType(type.dict_type().payload(), ctx);
            if (!payloadType) {
                return nullptr;
            }

            return ctx.MakeType<TDictExprType>(keyType, payloadType);
        }

        case Ydb::Type::kPgType: {
            if (!type.pg_type().type_name().empty()) {
                const auto& typeName = type.pg_type().type_name();
                auto typeDesc = NKikimr::NPg::TypeDescFromPgTypeName(typeName);
                return ctx.MakeType<TPgExprType>(NKikimr::NPg::PgTypeIdFromTypeDesc(typeDesc));
            }
            return ctx.MakeType<TPgExprType>(type.pg_type().Getoid());
        }

        case Ydb::Type::kNullType:
            [[fallthrough]];
        default: {
            ctx.AddError(TIssue(TPosition(), TStringBuilder() << "Unsupported protobuf type: "
                << type.ShortDebugString()));
            return nullptr;
        }
    }
}

} // namespace NYql
