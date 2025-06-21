#include "yql_default_valid_value.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>

namespace NYql {

namespace {

class TIsValidValueSupportedVisitor: public TTypeAnnotationVisitor {
public:
    TIsValidValueSupportedVisitor(const TTypeAnnotationNode* type)
        : Type_(type) {
    }

    bool IsSupported() {
        Type_->Accept(*this);
        return Result_;
    }

private:
    void Visit(const TUnitExprType& type) override {
        Y_UNUSED(type);
        Result_ = false; // YQL_ENSURE(false, "UnitExprType is not supported.");
    }

    void Visit(const TMultiExprType& type) override {
        Y_UNUSED(type);
        Result_ = false; // YQL_ENSURE(false, "MultiExprType is not supported.");
    }

    void Visit(const TTupleExprType& type) override {
        const auto& items = type.GetItems();
        for (size_t i = 0; i < items.size(); ++i) {
            if (!TIsValidValueSupportedVisitor(items[i]).IsSupported()) {
                Result_ = false;
                return;
            }
        }
        Result_ = true;
    }

    void Visit(const TStructExprType& type) override {
        const auto& items = type.GetItems();
        for (size_t i = 0; i < items.size(); ++i) {
            if (!TIsValidValueSupportedVisitor(items[i]->GetItemType()).IsSupported()) {
                Result_ = false;
                return;
            }
        }
        Result_ = true;
    }

    void Visit(const TItemExprType& type) override {
        Y_UNUSED(type);
        Result_ = false; // YQL_ENSURE(false, "Item expression type is implemented via Struct visitor branch.");
    }

    void Visit(const TListExprType& type) override {
        Y_UNUSED(type);
        Result_ = false; // YQL_ENSURE(false, "List type is not supported yet.");
    }

    void Visit(const TStreamExprType& type) override {
        Y_UNUSED(type);
        Result_ = false; // YQL_ENSURE(false, "StreamExprType is not supported.");
    }

    void Visit(const TFlowExprType& type) override {
        Y_UNUSED(type);
        Result_ = false; // YQL_ENSURE(false, "FlowExprType is not supported.");
    }

    void Visit(const TDataExprType& type) override {
        Y_UNUSED(type);
        Result_ = true; // All data types are supported
    }

    void Visit(const TPgExprType& type) override {
        Y_UNUSED(type);
        Result_ = true;
    }

    void Visit(const TWorldExprType& type) override {
        Y_UNUSED(type);
        Result_ = false; // YQL_ENSURE(false, "WorldExprType is not supported.");
    }

    void Visit(const TOptionalExprType& type) override {
        Y_UNUSED(type);
        Result_ = true; // Optional types are supported
    }

    void Visit(const TCallableExprType& type) override {
        Y_UNUSED(type);
        Result_ = false; // YQL_ENSURE(false, "CallableExprType is not supported.");
    }

    void Visit(const TResourceExprType& type) override {
        Y_UNUSED(type);
        Result_ = false; // YQL_ENSURE(false, "ResourceExprType is not supported.");
    }

    void Visit(const TTypeExprType& type) override {
        Y_UNUSED(type);
        Result_ = false; // YQL_ENSURE(false, "TypeExprType is not supported.");
    }

    void Visit(const TDictExprType& type) override {
        Y_UNUSED(type);
        Result_ = false; // YQL_ENSURE(false, "Dict type is not supported yet.");
    }

    void Visit(const TVoidExprType& type) override {
        Y_UNUSED(type);
        Result_ = true; // Void type is supported
    }

    void Visit(const TNullExprType& type) override {
        Y_UNUSED(type);
        Result_ = true; // Null type is supported
    }

    void Visit(const TGenericExprType& type) override {
        Y_UNUSED(type);
        Result_ = false; // YQL_ENSURE(false, "TypeExprType is not supported.");
    }

    void Visit(const TTaggedExprType& type) override {
        Result_ = TIsValidValueSupportedVisitor(type.GetBaseType()).IsSupported();
    }

    void Visit(const TErrorExprType& type) override {
        Y_UNUSED(type);
        Result_ = false; // YQL_ENSURE(false, "ErrorExprType is not supported.");
    }

    void Visit(const TVariantExprType& type) override {
        Y_UNUSED(type);
        Result_ = false; // YQL_ENSURE(false, "ErrorExprType is not implemented yet.");
    }

    void Visit(const TEmptyListExprType& type) override {
        Y_UNUSED(type);
        Result_ = true; // Empty list is supported
    }

    void Visit(const TEmptyDictExprType& type) override {
        Y_UNUSED(type);
        Result_ = true; // Empty dict is supported
    }

    void Visit(const TBlockExprType& type) override {
        Y_UNUSED(type);
        Result_ = false; // YQL_ENSURE(false, "BlockExprType is not supported.");
    }

    void Visit(const TScalarExprType& type) override {
        Y_UNUSED(type);
        Result_ = false; // YQL_ENSURE(false, "ScalarExprType is not supported.");
    }

private:
    const TTypeAnnotationNode* Type_;
    bool Result_ = false;
};

class TValidValueNodeVisitor: public TTypeAnnotationVisitor {
public:
    TValidValueNodeVisitor(const TTypeAnnotationNode* type, TExprContext& ctx, TPositionHandle pos)
        : Type_(type)
        , Ctx_(ctx)
        , Pos_(pos) {
    }

    TExprNode::TPtr CreateDefaultValue() {
        Type_->Accept(*this);
        return Result_;
    }

private:
    void Visit(const TUnitExprType& type) override {
        Y_UNUSED(type);
        YQL_ENSURE(false, "UnitExprType is not supported.");
    }

    void Visit(const TMultiExprType& type) override {
        Y_UNUSED(type);
        YQL_ENSURE(false, "MultiExprType is not supported.");
    }

    void Visit(const TTupleExprType& type) override {
        auto topBuilder = Ctx_.Builder(Pos_);
        auto builder = topBuilder.List();
        const auto& items = type.GetItems();
        for (size_t i = 0; i < items.size(); ++i) {
            builder.Add(i, TValidValueNodeVisitor(items[i], Ctx_, Pos_).CreateDefaultValue());
        }
        Result_ = builder.Seal().Build();
    }

    void Visit(const TStructExprType& type) override {
        auto topBuilder = Ctx_.Builder(Pos_);
        auto builder = topBuilder.Callable("AsStruct");
        const auto& items = type.GetItems();
        for (size_t i = 0; i < items.size(); ++i) {
            // clang-format off
            builder.List(i)
                        .Atom(0, items[i]->GetName())
                        .Add(1, TValidValueNodeVisitor(items[i]->GetItemType(), Ctx_, Pos_).CreateDefaultValue())
                    .Seal();
            // clang-format on
        }
        Result_ = builder.Seal().Build();
    }

    void Visit(const TItemExprType& type) override {
        Y_UNUSED(type);
        YQL_ENSURE(false, "Item expression type is implemented via Struct visitor branch.");
    }

    void Visit(const TListExprType& type) override {
        Y_UNUSED(type);
        YQL_ENSURE(false, "List type is not supported yet.");
    }

    void Visit(const TStreamExprType& type) override {
        Y_UNUSED(type);
        YQL_ENSURE(false, "StreamExprType is not supported.");
    }

    void Visit(const TFlowExprType& type) override {
        Y_UNUSED(type);
        YQL_ENSURE(false, "FlowExprType is not supported.");
    }

    void Visit(const TDataExprType& type) override {
        switch (type.GetSlot()) {
            case NUdf::EDataSlot::Bool:
                Result_ = Ctx_.Builder(Pos_).Callable("Bool").Atom(0, "false").Seal().Build();
                break;
            case NUdf::EDataSlot::Int8:
                Result_ = Ctx_.Builder(Pos_).Callable("Int8").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Uint8:
                Result_ = Ctx_.Builder(Pos_).Callable("Uint8").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Int16:
                Result_ = Ctx_.Builder(Pos_).Callable("Int16").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Uint16:
                Result_ = Ctx_.Builder(Pos_).Callable("Uint16").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Int32:
                Result_ = Ctx_.Builder(Pos_).Callable("Int32").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Uint32:
                Result_ = Ctx_.Builder(Pos_).Callable("Uint32").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Int64:
                Result_ = Ctx_.Builder(Pos_).Callable("Int64").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Uint64:
                Result_ = Ctx_.Builder(Pos_).Callable("Uint64").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Double:
                Result_ = Ctx_.Builder(Pos_).Callable("Double").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Float:
                Result_ = Ctx_.Builder(Pos_).Callable("Float").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::String:
                Result_ = Ctx_.Builder(Pos_).Callable("String").Atom(0, "").Seal().Build();
                break;
            case NUdf::EDataSlot::Utf8:
                Result_ = Ctx_.Builder(Pos_).Callable("Utf8").Atom(0, "").Seal().Build();
                break;
            case NUdf::EDataSlot::Yson:
                Result_ = Ctx_.Builder(Pos_).Callable("Yson").Atom(0, "#").Seal().Build();
                break;
            case NUdf::EDataSlot::Json:
                Result_ = Ctx_.Builder(Pos_).Callable("Json").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Uuid: {
                constexpr char UUID_VALID_LITERAL[] = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
                Result_ = Ctx_.Builder(Pos_).Callable("Uuid").Atom(0, TStringBuf(UUID_VALID_LITERAL, sizeof(UUID_VALID_LITERAL) - 1)).Seal().Build();
                break;
            }
            case NUdf::EDataSlot::Date:
                Result_ = Ctx_.Builder(Pos_).Callable("Date").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Datetime:
                Result_ = Ctx_.Builder(Pos_).Callable("Datetime").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Timestamp:
                Result_ = Ctx_.Builder(Pos_).Callable("Timestamp").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Interval:
                Result_ = Ctx_.Builder(Pos_).Callable("Interval").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::TzDate:
                Result_ = Ctx_.Builder(Pos_).Callable("TzDate").Atom(0, "0,Europe/Moscow").Seal().Build();
                break;
            case NUdf::EDataSlot::TzDatetime:
                Result_ = Ctx_.Builder(Pos_).Callable("TzDatetime").Atom(0, "7200,Europe/Moscow").Seal().Build();
                break;
            case NUdf::EDataSlot::TzTimestamp:
                Result_ = Ctx_.Builder(Pos_).Callable("TzTimestamp").Atom(0, "93600000000,Europe/Moscow").Seal().Build();
                break;
            case NUdf::EDataSlot::Decimal: {
                const auto& decimalType = *type.Cast<TDataExprParamsType>();
                // clang-format off
                Result_ = Ctx_.Builder(Pos_)
                    .Callable("Decimal")
                        .Atom(0, "0")
                        .Atom(1, decimalType.GetParamOne())
                        .Atom(2, decimalType.GetParamTwo())
                    .Seal()
                    .Build();
                // clang-format on
                break;
            }
            case NUdf::EDataSlot::DyNumber:
                Result_ = Ctx_.Builder(Pos_).Callable("DyNumber").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::JsonDocument:
                Result_ = Ctx_.Builder(Pos_).Callable("JsonDocument").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Date32:
                Result_ = Ctx_.Builder(Pos_).Callable("Date32").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Datetime64:
                Result_ = Ctx_.Builder(Pos_).Callable("Datetime64").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Timestamp64:
                Result_ = Ctx_.Builder(Pos_).Callable("Timestamp64").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::Interval64:
                Result_ = Ctx_.Builder(Pos_).Callable("Interval64").Atom(0, "0").Seal().Build();
                break;
            case NUdf::EDataSlot::TzDate32:
                Result_ = Ctx_.Builder(Pos_).Callable("TzDate32").Atom(0, "0,Europe/Moscow").Seal().Build();
                break;
            case NUdf::EDataSlot::TzDatetime64:
                Result_ = Ctx_.Builder(Pos_).Callable("TzDatetime64").Atom(0, "7200,Europe/Moscow").Seal().Build();
                break;
            case NUdf::EDataSlot::TzTimestamp64:
                Result_ = Ctx_.Builder(Pos_).Callable("TzTimestamp64").Atom(0, "93600000000,Europe/Moscow").Seal().Build();
                break;
        }
    }

    void Visit(const TPgExprType& type) override {
        // clang-format off
        Result_ = Ctx_.Builder(Pos_)
                        .Callable("Nothing")
                            .Add(0, ExpandType(Pos_, type, Ctx_))
                            .Seal()
                        .Build();
        // clang-format on
    }

    void Visit(const TWorldExprType& type) override {
        Y_UNUSED(type);
        YQL_ENSURE(false, "WorldExprType is not supported.");
    }

    void Visit(const TOptionalExprType& type) override {
        Y_UNUSED(type, "Optional content type is not interested.");
        // clang-format off
        Result_ = Ctx_.Builder(Pos_)
                        .Callable("Nothing")
                            .Add(0, ExpandType(Pos_, type, Ctx_))
                            .Seal()
                        .Build();
        // clang-format on
    }

    void Visit(const TCallableExprType& type) override {
        Y_UNUSED(type);
        YQL_ENSURE(false, "CallableExprType is not supported.");
    }

    void Visit(const TResourceExprType& type) override {
        Y_UNUSED(type);
        YQL_ENSURE(false, "ResourceExprType is not supported.");
    }

    void Visit(const TTypeExprType& type) override {
        Y_UNUSED(type);
        YQL_ENSURE(false, "TypeExprType is not supported.");
    }

    void Visit(const TDictExprType& type) override {
        Y_UNUSED(type, "Just create empty dict instead of real dict.");
        YQL_ENSURE(false, "Dict type is not supported yet.");
    }

    void Visit(const TVoidExprType& type) override {
        Y_UNUSED(type, "Void type is singleton type.");
        Result_ = Ctx_.Builder(Pos_).Callable("Void").Seal().Build();
    }

    void Visit(const TNullExprType& type) override {
        Y_UNUSED(type, "Null type is singleton type.");
        Result_ = Ctx_.Builder(Pos_).Callable("Null").Seal().Build();
    }

    void Visit(const TGenericExprType& type) override {
        Y_UNUSED(type);
        YQL_ENSURE(false, "TypeExprType is not supported.");
    }

    void Visit(const TTaggedExprType& type) override {
        // clang-format off
        Result_ = Ctx_.Builder(Pos_)
                        .Callable("AsTagged")
                            .Add(0, TValidValueNodeVisitor(type.GetBaseType(), Ctx_, Pos_).CreateDefaultValue())
                            .Atom(1, type.GetTag())
                        .Seal()
                        .Build();
        // clang-format on
    }

    void Visit(const TErrorExprType& type) override {
        Y_UNUSED(type);
        YQL_ENSURE(false, "ErrorExprType is not supported.");
    }

    void Visit(const TVariantExprType& type) override {
        // clang-format off
        Y_UNUSED(type);
        YQL_ENSURE(false, "VariantExprType is not implemented yet.");
        // clang-format on
    }

    void Visit(const TEmptyListExprType& type) override {
        Y_UNUSED(type, "Empty list is a singleton variable.");
        Result_ = Ctx_.Builder(Pos_).Callable("EmptyList").Seal().Build();
    }

    void Visit(const TEmptyDictExprType& type) override {
        Y_UNUSED(type, "Empty dict is a singleton variable.");
        Result_ = Ctx_.Builder(Pos_).Callable("EmptyDict").Seal().Build();
    }

    void Visit(const TBlockExprType& type) override {
        Y_UNUSED(type);
        YQL_ENSURE(false, "BlockExprType is not supported.");
    }

    void Visit(const TScalarExprType& type) override {
        Y_UNUSED(type);
        YQL_ENSURE(false, "ScalarExprType is not supported.");
    }

private:
    const TTypeAnnotationNode* Type_;
    TExprContext& Ctx_;
    TExprNode::TPtr Result_ = nullptr;
    TPositionHandle Pos_;
};

const TTypeAnnotationNode* UnwrapType(const TTypeAnnotationNode* type) {
    bool isScalar;
    if (type->IsBlockOrScalar()) {
        type = GetBlockItemType(*type, isScalar);
    }
    if (type->GetKind() != ETypeAnnotationKind::Optional) {
        return nullptr;
    }
    type = type->Cast<TOptionalExprType>()->GetItemType();
    return type;
}

} // namespace

TExprNode::TPtr MakeValidValue(const TTypeAnnotationNode* type, TPositionHandle pos, TExprContext& ctx) {
    type = UnwrapType(type);
    YQL_ENSURE(type);
    return TValidValueNodeVisitor(type, ctx, pos).CreateDefaultValue();
}

bool IsValidValueSupported(const TTypeAnnotationNode* type) {
    type = UnwrapType(type);
    if (!type) {
        return false;
    }
    return TIsValidValueSupportedVisitor(type).IsSupported();
}

} // namespace NYql
