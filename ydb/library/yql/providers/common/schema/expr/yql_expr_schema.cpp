#include "yql_expr_schema.h"

#include <ydb/library/yql/providers/common/schema/parser/yql_type_parser.h>
#include <ydb/library/yql/ast/yql_expr_types.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_builder.h>
#include <library/cpp/yson/writer.h>

#include <util/generic/map.h>
#include <util/stream/str.h>


namespace NYql {
namespace NCommon {

template <template<typename> class TSaver>
class TExprTypeSaver: public TSaver<TExprTypeSaver<TSaver>> {
    typedef TSaver<TExprTypeSaver<TSaver>> TBase;

    struct TStructAdaptor {
        const TStructExprType* Type;

        TStructAdaptor(const TStructExprType* type)
            : Type(type)
        {
        }

        ui32 GetMembersCount() const {
            return Type->GetItems().size();
        }

        const TStringBuf& GetMemberName(ui32 idx) const {
            return Type->GetItems()[idx]->GetName();
        }

        const TTypeAnnotationNode* GetMemberType(ui32 idx) const {
            return Type->GetItems()[idx]->GetItemType();
        }
    };

    struct TMappingOrderedStructAdaptor {
        TVector<std::pair<TStringBuf, const TTypeAnnotationNode*>> Members;

        TMappingOrderedStructAdaptor(const TStructMemberMapper& mapper, const TMaybe<TColumnOrder>& columns, const TStructExprType* type)
        {
            TMap<TStringBuf, const TTypeAnnotationNode*> members;
            for (auto& item: type->GetItems()) {
                TMaybe<TStringBuf> name = mapper ? mapper(item->GetName()) : item->GetName();
                if (!name) {
                    continue;
                }
                members[*name] = item->GetItemType();
            }

            if (columns) {
                for (auto& [column, gen_column] : *columns) {
                    auto it = members.find(gen_column);
                    if (it != members.end()) {
                        Members.emplace_back(column, it->second);
                    }
                }
            } else {
                Members.insert(Members.end(), members.begin(), members.end());
            }
        }

        ui32 GetMembersCount() const {
            return Members.size();
        }

        const TStringBuf& GetMemberName(ui32 idx) const {
            return Members[idx].first;
        }

        const TTypeAnnotationNode* GetMemberType(ui32 idx) const {
            return Members[idx].second;
        }
    };

    struct TTupleAdaptor {
        const TTupleExprType* Type;

        TTupleAdaptor(const TTupleExprType* type)
            : Type(type)
        {
        }

        ui32 GetElementsCount() const {
            return Type->GetItems().size();
        }

        const TTypeAnnotationNode* GetElementType(ui32 idx) const {
            return Type->GetItems()[idx];
        }
    };

    struct TCallableAdaptor {
        const TCallableExprType* Type;

        TCallableAdaptor(const TCallableExprType* type)
            : Type(type)
        {
        }

        size_t GetOptionalArgsCount() const {
            return Type->GetOptionalArgumentsCount();
        }

        TStringBuf GetPayload() const {
            return Type->GetPayload();
        }

        const TTypeAnnotationNode* GetReturnType() const {
            return Type->GetReturnType();
        }

        size_t GetArgumentsCount() const {
            return Type->GetArgumentsSize();
        }

        TStringBuf GetArgumentName(size_t i) const {
            return Type->GetArguments().at(i).Name;
        }

        ui64 GetArgumentFlags(size_t i) const {
            return Type->GetArguments().at(i).Flags;
        }

        const TTypeAnnotationNode* GetArgumentType(size_t i) const {
            return Type->GetArguments().at(i).Type;
        }
    };

    void SaveErrorType(const TErrorExprType& errorType) {
        TBase::SaveTypeHeader("ErrorType");
        auto err = errorType.GetError();
        TBase::Writer.OnListItem();
        TBase::Writer.OnInt64Scalar(err.Position.Row);
        TBase::Writer.OnListItem();
        TBase::Writer.OnInt64Scalar(err.Position.Column);
        TBase::Writer.OnListItem();
        TBase::Writer.OnStringScalar(err.Position.File);
        TBase::Writer.OnListItem();
        TBase::Writer.OnStringScalar(err.GetMessage());
        TBase::Writer.OnEndList();
    }

public:
    TExprTypeSaver(typename TBase::TConsumer& consumer, bool extendedForm)
        : TBase(consumer, extendedForm)
    {
    }

    void Save(const TTypeAnnotationNode* type) {
        switch (type->GetKind()) {
            case ETypeAnnotationKind::Data: {
                const auto dataType = type->Cast<TDataExprType>();
                if (const auto dataParamsType = dynamic_cast<const TDataExprParamsType*>(dataType)) {
                    TBase::SaveDataTypeParams(dataType->GetName(), dataParamsType->GetParamOne(), dataParamsType->GetParamTwo());
                } else {
                    TBase::SaveDataType(dataType->GetName());
                }
                break;
            }
            case ETypeAnnotationKind::Pg:
                TBase::SavePgType(type->Cast<TPgExprType>()->GetName());
                break;
            case ETypeAnnotationKind::Struct:
                TBase::SaveStructType(TStructAdaptor(type->Cast<TStructExprType>()));
                break;
            case ETypeAnnotationKind::List:
                TBase::SaveListType(*type->Cast<TListExprType>());
                break;
            case ETypeAnnotationKind::Optional:
                TBase::SaveOptionalType(*type->Cast<TOptionalExprType>());
                break;
            case ETypeAnnotationKind::Tuple:
                TBase::SaveTupleType(TTupleAdaptor(type->Cast<TTupleExprType>()));
                break;
            case ETypeAnnotationKind::Dict:
                TBase::SaveDictType(*type->Cast<TDictExprType>());
                break;
            case ETypeAnnotationKind::Type:
                TBase::SaveType();
                break;
            case ETypeAnnotationKind::Generic:
                TBase::SaveGenericType();
                break;
            case ETypeAnnotationKind::Void:
                TBase::SaveVoidType();
                break;
            case ETypeAnnotationKind::Null:
                TBase::SaveNullType();
                break;
            case ETypeAnnotationKind::Unit:
                TBase::SaveUnitType();
                break;
            case ETypeAnnotationKind::EmptyList:
                TBase::SaveEmptyListType();
                break;
            case ETypeAnnotationKind::EmptyDict:
                TBase::SaveEmptyDictType();
                break;
            case ETypeAnnotationKind::Resource:
                TBase::SaveResourceType(type->Cast<TResourceExprType>()->GetTag());
                break;
            case ETypeAnnotationKind::Tagged:
                TBase::SaveTaggedType(*type->Cast<TTaggedExprType>());
                break;
            case ETypeAnnotationKind::Error:
                SaveErrorType(*type->Cast<TErrorExprType>());
                break;
            case ETypeAnnotationKind::Callable:
                TBase::SaveCallableType(TCallableAdaptor(type->Cast<TCallableExprType>()));
                break;
            case ETypeAnnotationKind::Variant:
                TBase::SaveVariantType(*type->Cast<TVariantExprType>());
                break;
            case ETypeAnnotationKind::Stream:
                TBase::SaveStreamType(*type->Cast<TStreamExprType>());
                break;
            default:
                YQL_ENSURE(false, "Unsupported type annotation kind: " << type->GetKind());
        }
    }

    void SaveStructType(const TStructExprType* type, const TMaybe<TColumnOrder>& columns, const TStructMemberMapper& mapper) {
        if (mapper || columns) {
            TBase::SaveStructType(TMappingOrderedStructAdaptor(mapper, columns, type));
        } else {
            Save(type);
        }
    }
};

void SaveStructTypeToYson(NYson::TYsonConsumerBase& writer, const TStructExprType* type, const TMaybe<TColumnOrder>& columns, const TStructMemberMapper& mapper, bool extendedForm) {
    TExprTypeSaver<TYqlTypeYsonSaverImpl> saver(writer, extendedForm);
    saver.SaveStructType(type, columns, mapper);
}

void WriteTypeToYson(NYson::TYsonConsumerBase& writer, const TTypeAnnotationNode* type, bool extendedForm) {
    TExprTypeSaver<TYqlTypeYsonSaverImpl> saver(writer, extendedForm);
    saver.Save(type);
}

NYT::TNode TypeToYsonNode(const TTypeAnnotationNode* type, bool extendedForm) {
    NYT::TNode res;
    NYT::TNodeBuilder builder(&res);
    WriteTypeToYson(builder, type, extendedForm);
    return res;
}

TString WriteTypeToYson(const TTypeAnnotationNode* type, NYson::EYsonFormat format, bool extendedForm) {
    TStringStream stream;
    NYson::TYsonWriter writer(&stream, format);
    WriteTypeToYson(writer, type, extendedForm);
    return stream.Str();
}

struct TExprTypeLoader {
    typedef const TTypeAnnotationNode* TType;

    TExprContext& Ctx;
    TPosition Pos;

    TExprTypeLoader(TExprContext& ctx, const TPosition& pos = TPosition())
        : Ctx(ctx)
        , Pos(pos)
    {
    }
    TMaybe<TType> LoadVoidType(ui32 /*level*/) {
        return Ctx.MakeType<TVoidExprType>();
    }
    TMaybe<TType> LoadNullType(ui32 /*level*/) {
        return Ctx.MakeType<TNullExprType>();
    }
    TMaybe<TType> LoadUnitType(ui32 /*level*/) {
        return Ctx.MakeType<TUnitExprType>();
    }
    TMaybe<TType> LoadGenericType(ui32 /*level*/) {
        return Ctx.MakeType<TGenericExprType>();
    }
    TMaybe<TType> LoadEmptyListType(ui32 /*level*/) {
        return Ctx.MakeType<TEmptyListExprType>();
    }
    TMaybe<TType> LoadEmptyDictType(ui32 /*level*/) {
        return Ctx.MakeType<TEmptyDictExprType>();
    }
    TMaybe<TType> LoadDataType(const TString& dataType, ui32 /*level*/) {
        return Ctx.MakeType<TDataExprType>(NYql::NUdf::GetDataSlot(dataType));
    }
    TMaybe<TType> LoadPgType(const TString& name, ui32 /*level*/) {
        return Ctx.MakeType<TPgExprType>(NYql::NPg::LookupType(name).TypeId);
    }
    TMaybe<TType> LoadDataTypeParams(const TString& dataType, const TString& paramOne, const TString& paramTwo, ui32 /*level*/) {
        auto ret = Ctx.MakeType<TDataExprParamsType>(NYql::NUdf::GetDataSlot(dataType), paramOne, paramTwo);
        YQL_ENSURE(ret->Validate(TPosition(), Ctx));
        return ret;
    }
    TMaybe<TType> LoadResourceType(const TString& tag, ui32 /*level*/) {
        return Ctx.MakeType<TResourceExprType>(tag);
    }
    TMaybe<TType> LoadTaggedType(TType baseType, const TString& tag, ui32 /*level*/) {
        auto ret = Ctx.MakeType<TTaggedExprType>(baseType, tag);
        YQL_ENSURE(ret->Validate(TPosition(), Ctx));
        return ret;
    }
    TMaybe<TType> LoadErrorType(ui32 row, ui32 column, const TString& file, const TString& msg, ui32 /*level*/) {
        return Ctx.MakeType<TErrorExprType>(TIssue(TPosition(column, row, file), msg));
    }
    TMaybe<TType> LoadStructType(const TVector<std::pair<TString, TType>>& members, ui32 /*level*/) {
        TVector<const TItemExprType*> items;
        for (auto& member: members) {
            items.push_back(Ctx.MakeType<TItemExprType>(member.first, member.second));
        }
        auto ret = Ctx.MakeType<TStructExprType>(items);
        YQL_ENSURE(ret->Validate(TPosition(), Ctx));
        return ret;
    }
    TMaybe<TType> LoadListType(TType itemType, ui32 /*level*/) {
        return Ctx.MakeType<TListExprType>(itemType);
    }
    TMaybe<TType> LoadStreamType(TType itemType, ui32 /*level*/) {
        return Ctx.MakeType<TStreamExprType>(itemType);
    }
    TMaybe<TType> LoadOptionalType(TType itemType, ui32 /*level*/) {
        return Ctx.MakeType<TOptionalExprType>(itemType);
    }
    TMaybe<TType> LoadTupleType(const TVector<TType>& elements, ui32 /*level*/) {
        auto ret = Ctx.MakeType<TTupleExprType>(elements);
        YQL_ENSURE(ret->Validate(TPosition(), Ctx));
        return ret;
    }
    TMaybe<TType> LoadDictType(TType keyType, TType valType, ui32 /*level*/) {
        auto ret = Ctx.MakeType<TDictExprType>(keyType, valType);
        YQL_ENSURE(ret->Validate(TPosition(), Ctx));
        return ret;
    }
    TMaybe<TType> LoadCallableType(TType returnType, const TVector<TType>& argTypes, const TVector<TString>& argNames,
        const TVector<ui64>& argFlags, size_t optionalCount, const TString& payload, ui32 /*level*/) {
        YQL_ENSURE(argTypes.size() == argNames.size() && argTypes.size() == argFlags.size());
        TVector<TCallableExprType::TArgumentInfo> args;
        for (size_t i = 0; i < argTypes.size(); ++i) {
            args.emplace_back();
            args.back().Type = argTypes[i];
            args.back().Name = Ctx.AppendString(argNames[i]);
            args.back().Flags = argFlags[i];
        }
        auto ret = Ctx.MakeType<TCallableExprType>(returnType, args, optionalCount, Ctx.AppendString(payload));
        YQL_ENSURE(ret->Validate(TPosition(), Ctx));
        return ret;
    }
    TMaybe<TType> LoadVariantType(TType underlyingType, ui32 /*level*/) {
        auto ret = Ctx.MakeType<TVariantExprType>(underlyingType);
        YQL_ENSURE(ret->Validate(TPosition(), Ctx));
        return ret;
    }
    void Error(const TString& info) {
        Ctx.AddError(TIssue(Pos, info));
    }
};

const TTypeAnnotationNode* ParseTypeFromYson(const TStringBuf yson, TExprContext& ctx, const TPosition& pos) {
    NYT::TNode node;
    TStringStream err;
    if (!ParseYson(node, yson, err)) {
        ctx.AddError(TIssue(pos, err.Str()));
        return nullptr;
    }

    return ParseTypeFromYson(node, ctx, pos);
}

const TTypeAnnotationNode* ParseOrderAwareTypeFromYson(const TStringBuf yson, TColumnOrder& topLevelColumns, TExprContext& ctx, const TPosition& pos) {
    NYT::TNode node;
    TStringStream err;
    if (!ParseYson(node, yson, err)) {
        ctx.AddError(TIssue(pos, err.Str()));
        return nullptr;
    }

    return ParseOrderAwareTypeFromYson(node, topLevelColumns, ctx, pos);
}

const TTypeAnnotationNode* ParseTypeFromYson(const NYT::TNode& node, TExprContext& ctx, const TPosition& pos) {
    TExprTypeLoader loader(ctx, pos);
    return DoLoadTypeFromYson(loader, node, 0).GetOrElse(nullptr);
}

struct TOrderAwareExprTypeLoader: public TExprTypeLoader {
    typedef const TTypeAnnotationNode* TType;
    TColumnOrder& TopLevelColumns;

    TOrderAwareExprTypeLoader(TExprContext& ctx, const TPosition& pos, TColumnOrder& topLevelColumns)
        : TExprTypeLoader(ctx, pos)
        , TopLevelColumns(topLevelColumns)
    {
        TopLevelColumns.Clear();
    }

    TMaybe<TType> LoadStructType(const TVector<std::pair<TString, TType>>& members, ui32 level) {
        if (level == 0) {
            YQL_ENSURE(TopLevelColumns.Size() == 0);
            for (auto& [column, type] : members) {
                TopLevelColumns.AddColumn(column);
            }
        }
        return TExprTypeLoader::LoadStructType(members, level);
    }
};

const TTypeAnnotationNode* ParseOrderAwareTypeFromYson(const NYT::TNode& node, TColumnOrder& topLevelColumns, TExprContext& ctx, const TPosition& pos) {
    TOrderAwareExprTypeLoader loader(ctx, pos, topLevelColumns);
    return DoLoadTypeFromYson(loader, node, 0).GetOrElse(nullptr);
}

void WriteResOrPullType(NYson::TYsonConsumerBase& writer, const TTypeAnnotationNode* type, const TColumnOrder& columns) {
    if (columns.Size() == 0 ||
        type->GetKind() != ETypeAnnotationKind::List ||
        type->Cast<TListExprType>()->GetItemType()->GetKind() != ETypeAnnotationKind::Struct) {
        WriteTypeToYson(writer, type, true);
    } else {
        writer.OnBeginList();
        writer.OnListItem();
        writer.OnStringScalar("ListType");
        writer.OnListItem();

        SaveStructTypeToYson(writer, type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>(), columns, {}, true);

        writer.OnEndList();
    }
}

} // namespace NCommon
} // namespace NYql
