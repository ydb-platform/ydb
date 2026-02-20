#include "mkql_node_printer.h"
#include "mkql_node_visitor.h"
#include <util/stream/str.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>

namespace NKikimr::NMiniKQL {

namespace {
class TPrintVisitor: public INodeVisitor {
    friend class TIndentScope;

public:
    struct TIndentScope {
        explicit TIndentScope(TPrintVisitor* self)
            : Self(self)
        {
            ++Self->Indent_;
        }

        ~TIndentScope() {
            --Self->Indent_;
        }

        TPrintVisitor* Self;
    };

    explicit TPrintVisitor(bool singleLine)
        : SingleLine_(singleLine)
        , Indent_(0)
    {
    }

    void Visit(TTypeType& node) override {
        Y_UNUSED(node);
        WriteIndentation();
        Out_ << "Type (Type)";
        WriteNewline();
    }

    void Visit(TVoidType& node) override {
        Y_UNUSED(node);
        WriteIndentation();
        Out_ << "Type (Void) ";
        WriteNewline();
    }

    void Visit(TNullType& node) override {
        Y_UNUSED(node);
        WriteIndentation();
        Out_ << "Type (Null) ";
        WriteNewline();
    }

    void Visit(TEmptyListType& node) override {
        Y_UNUSED(node);
        WriteIndentation();
        Out_ << "Type (EmptyList) ";
        WriteNewline();
    }

    void Visit(TEmptyDictType& node) override {
        Y_UNUSED(node);
        WriteIndentation();
        Out_ << "Type (EmptyDict) ";
        WriteNewline();
    }

    void Visit(TDataType& node) override {
        WriteIndentation();
        auto slot = NUdf::FindDataSlot(node.GetSchemeType());
        Out_ << "Type (Data), schemeType: ";
        if (slot) {
            Out_ << GetDataTypeInfo(*slot).Name;
            if (node.GetSchemeType() == NUdf::TDataType<NUdf::TDecimal>::Id) {
                const auto params = static_cast<TDataDecimalType&>(node).GetParams();
                Out_ << '(' << int(params.first) << ',' << int(params.second) << ')';
            }
        } else {
            Out_ << "<" << node.GetSchemeType() << ">";
        }

        Out_ << ", schemeTypeId: ";
        Out_ << node.GetSchemeType();
        WriteNewline();
    }

    void Visit(TPgType& node) override {
        WriteIndentation();
        Out_ << "Type (Pg), name: " << NYql::NPg::LookupType(node.GetTypeId()).Name;
        WriteNewline();
    }

    void Visit(TStructType& node) override {
        WriteIndentation();
        Out_ << "Type (Struct) with " << node.GetMembersCount() << " members {";
        WriteNewline();

        {
            TIndentScope scope(this);
            for (ui32 index = 0; index < node.GetMembersCount(); ++index) {
                WriteIndentation();
                Out_ << "Member [" << node.GetMemberName(index) << "] : {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    node.GetMemberType(index)->Accept(*this);
                }

                WriteIndentation();
                Out_ << "}";
                WriteNewline();
            }
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TListType& node) override {
        WriteIndentation();
        Out_ << "Type (List) {";
        WriteNewline();

        {
            TIndentScope scope(this);
            WriteIndentation();
            Out_ << "List item type: {";
            WriteNewline();

            {
                TIndentScope scope2(this);
                node.GetItemType()->Accept(*this);
            }

            WriteIndentation();
            Out_ << "}";
            WriteNewline();
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TStreamType& node) override {
        WriteIndentation();
        Out_ << "Type (Stream) {";
        WriteNewline();

        {
            TIndentScope scope(this);
            WriteIndentation();
            Out_ << "Stream item type: {";
            WriteNewline();

            {
                TIndentScope scope2(this);
                node.GetItemType()->Accept(*this);
            }

            WriteIndentation();
            Out_ << "}";
            WriteNewline();
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TFlowType& node) override {
        WriteIndentation();
        Out_ << "Type (Flow) {";
        WriteNewline();

        {
            TIndentScope scope(this);
            WriteIndentation();
            Out_ << "Flow item type: {";
            WriteNewline();

            {
                TIndentScope scope2(this);
                node.GetItemType()->Accept(*this);
            }

            WriteIndentation();
            Out_ << "}";
            WriteNewline();
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TBlockType& node) override {
        WriteIndentation();
        Out_ << "Type (Block) {";
        WriteNewline();

        {
            TIndentScope scope(this);
            WriteIndentation();
            Out_ << "Block item type: {";
            WriteNewline();

            {
                TIndentScope scope2(this);
                node.GetItemType()->Accept(*this);
            }

            WriteIndentation();
            Out_ << "}";
            WriteNewline();

            WriteIndentation();
            Out_ << "Block shape: " << (node.GetShape() == TBlockType::EShape::Scalar ? "Scalar" : "Many");
            WriteNewline();
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TOptionalType& node) override {
        WriteIndentation();
        Out_ << "Type (Optional) {";
        WriteNewline();

        {
            TIndentScope scope(this);
            WriteIndentation();
            Out_ << "Optional item type: {";
            WriteNewline();

            {
                TIndentScope scope2(this);
                node.GetItemType()->Accept(*this);
            }

            WriteIndentation();
            Out_ << "}";
            WriteNewline();
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TLinearType& node) override {
        WriteIndentation();
        Out_ << "Type (Linear), isDynamic: " << node.IsDynamic() << " {";
        WriteNewline();

        {
            TIndentScope scope(this);
            WriteIndentation();
            Out_ << "Linear item type: {";
            WriteNewline();

            {
                TIndentScope scope2(this);
                node.GetItemType()->Accept(*this);
            }

            WriteIndentation();
            Out_ << "}";
            WriteNewline();
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TDictType& node) override {
        WriteIndentation();
        Out_ << "Type (Dict) {";
        WriteNewline();

        {
            TIndentScope scope(this);
            WriteIndentation();
            Out_ << "Key type: {";
            WriteNewline();

            {
                TIndentScope scope2(this);
                node.GetKeyType()->Accept(*this);
            }

            WriteIndentation();
            Out_ << "}";
            WriteNewline();

            WriteIndentation();
            Out_ << "Payload type: {";
            WriteNewline();

            {
                TIndentScope scope2(this);
                node.GetPayloadType()->Accept(*this);
            }

            WriteIndentation();
            Out_ << "}";
            WriteNewline();
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TCallableType& node) override {
        WriteIndentation();
        Out_ << "Type (Callable), name: [" << node.GetName() << "] with " << node.GetArgumentsCount() << " args {";
        WriteNewline();

        {
            TIndentScope scope(this);
            WriteIndentation();
            Out_ << "Return type";
            if (node.IsMergeDisabled()) {
                Out_ << ", merge disabled";
            }
            if (node.GetOptionalArgumentsCount() != 0) {
                Out_ << ", optional args: " << node.GetOptionalArgumentsCount();
            }
            Out_ << " : {";
            WriteNewline();

            {
                TIndentScope scope2(this);
                node.GetReturnType()->Accept(*this);
            }

            WriteIndentation();
            Out_ << "}";
            WriteNewline();
            for (ui32 index = 0; index < node.GetArgumentsCount(); ++index) {
                WriteIndentation();
                const auto& type = node.GetArgumentType(index);
                Out_ << "Argument #" << index << " : {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    type->Accept(*this);
                }

                WriteIndentation();
                Out_ << "}";
                WriteNewline();
            }

            if (node.GetPayload()) {
                WriteIndentation();
                Out_ << "Payload: {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    node.GetPayload()->Accept(*this);
                }

                WriteIndentation();
                Out_ << "}";
                WriteNewline();
            }
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TAnyType& node) override {
        Y_UNUSED(node);
        WriteIndentation();
        Out_ << "Type (Any) ";
        WriteNewline();
    }

    template <typename T>
    void VisitTupleLike(T& node, std::string_view name) {
        WriteIndentation();
        Out_ << "Type (" << name << ") with " << node.GetElementsCount() << " elements {";
        WriteNewline();

        {
            TIndentScope scope(this);
            for (ui32 index = 0; index < node.GetElementsCount(); ++index) {
                WriteIndentation();
                Out_ << "#" << index << " : {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    node.GetElementType(index)->Accept(*this);
                }

                WriteIndentation();
                Out_ << "}";
                WriteNewline();
            }
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TTupleType& node) override {
        VisitTupleLike(node, "Tuple");
    }

    void Visit(TMultiType& node) override {
        VisitTupleLike(node, "Multi");
    }

    void Visit(TResourceType& node) override {
        Y_UNUSED(node);
        WriteIndentation();
        Out_ << "Type (Resource) (" << node.GetTag() << ")";
        WriteNewline();
    }

    void Visit(TVariantType& node) override {
        WriteIndentation();
        Out_ << "Type (Variant) {";
        WriteNewline();

        {
            TIndentScope scope(this);
            WriteIndentation();
            Out_ << "Underlying type: {";
            WriteNewline();

            {
                TIndentScope scope2(this);
                node.GetUnderlyingType()->Accept(*this);
            }

            WriteIndentation();
            Out_ << "}";
            WriteNewline();
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TVoid& node) override {
        WriteIndentation();
        Out_ << "Void {";
        WriteNewline();

        {
            TIndentScope scope(this);
            node.GetType()->Accept(*this);
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TNull& node) override {
        WriteIndentation();
        Out_ << "Null {";
        WriteNewline();

        {
            TIndentScope scope(this);
            node.GetType()->Accept(*this);
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TEmptyList& node) override {
        WriteIndentation();
        Out_ << "EmptyList {";
        WriteNewline();

        {
            TIndentScope scope(this);
            node.GetType()->Accept(*this);
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TEmptyDict& node) override {
        WriteIndentation();
        Out_ << "EmptyDict {";
        WriteNewline();

        {
            TIndentScope scope(this);
            node.GetType()->Accept(*this);
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TDataLiteral& node) override {
        WriteIndentation();
        Out_ << "Data {";
        WriteNewline();

        {
            TIndentScope scope(this);
            node.GetType()->Accept(*this);

            WriteIndentation();
            if (node.GetType()->GetSchemeType() == 0) {
                Out_ << "null";
                WriteNewline();
            } else {
                Out_ << TString(node.AsValue().AsStringRef()).Quote();
                WriteNewline();
            }
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TStructLiteral& node) override {
        WriteIndentation();
        Out_ << "Struct {";
        WriteNewline();

        {
            TIndentScope scope(this);
            for (ui32 index = 0; index < node.GetValuesCount(); ++index) {
                WriteIndentation();
                const auto& value = node.GetValue(index);
                Out_ << "Member [" << node.GetType()->GetMemberName(index) << "], "
                     << (value.IsImmediate() ? "immediate" : "not immediate") << " {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    value.GetNode()->Accept(*this);
                }

                WriteIndentation();
                Out_ << "}";
                WriteNewline();
            }
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TListLiteral& node) override {
        WriteIndentation();
        Out_ << "List with " << node.GetItemsCount() << " items {";
        WriteNewline();

        {
            TIndentScope scope(this);
            node.GetType()->Accept(*this);
            ui32 index = 0;
            for (ui32 i = 0; i < node.GetItemsCount(); ++i) {
                WriteIndentation();
                const auto& item = node.GetItems()[i];
                Out_ << "Item #" << index << ", " << (item.IsImmediate() ? "immediate" : "not immediate") << " {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    item.GetNode()->Accept(*this);
                }

                WriteIndentation();
                Out_ << "}";
                WriteNewline();
            }
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TOptionalLiteral& node) override {
        WriteIndentation();
        Out_ << "Optional " << (node.HasItem() ? "with data" : "empty") << " {";
        WriteNewline();

        {
            TIndentScope scope(this);
            node.GetType()->Accept(*this);
            if (node.HasItem()) {
                WriteIndentation();
                const auto& item = node.GetItem();
                Out_ << "Item " << (item.IsImmediate() ? "immediate" : "not immediate") << " {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    item.GetNode()->Accept(*this);
                }

                WriteIndentation();
                Out_ << "}";
                WriteNewline();
            }
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TDictLiteral& node) override {
        WriteIndentation();
        Out_ << "Dict with " << node.GetItemsCount() << " items {";
        WriteNewline();

        {
            TIndentScope scope(this);
            node.GetType()->Accept(*this);
            for (ui32 index = 0; index < node.GetItemsCount(); ++index) {
                WriteIndentation();
                const auto& item = node.GetItem(index);
                Out_ << "Key of item #" << index << ", " << (item.first.IsImmediate() ? "immediate" : "not immediate") << " {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    item.first.GetNode()->Accept(*this);
                }

                WriteIndentation();
                Out_ << "}";
                WriteNewline();

                WriteIndentation();
                Out_ << "Payload of item #" << index << ", " << (item.second.IsImmediate() ? "immediate" : "not immediate") << " {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    item.second.GetNode()->Accept(*this);
                }

                WriteIndentation();
                Out_ << "}";
                WriteNewline();
            }
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TCallable& node) override {
        WriteIndentation();
        Out_ << "Callable";
        if (node.GetUniqueId() != 0) {
            Out_ << ", uniqueId: " << node.GetUniqueId();
        }
        Out_ << " {";
        WriteNewline();

        {
            TIndentScope scope(this);
            node.GetType()->Accept(*this);

            for (ui32 index = 0; index < node.GetInputsCount(); ++index) {
                WriteIndentation();
                const auto& input = node.GetInput(index);
                Out_ << "Input #" << index << ", " << (input.IsImmediate() ? "immediate" : "not immediate") << " {";
                WriteNewline();

                {
                    TIndentScope scope3(this);
                    input.GetNode()->Accept(*this);
                }

                WriteIndentation();
                Out_ << "}";
                WriteNewline();
            }

            if (node.HasResult()) {
                WriteIndentation();
                Out_ << "Result, " << (node.GetResult().IsImmediate() ? "immediate" : "not immediate") << " {";
                WriteNewline();

                {
                    TIndentScope scope3(this);
                    node.GetResult().GetNode()->Accept(*this);
                }

                WriteIndentation();
                Out_ << "}";
                WriteNewline();
            }
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TAny& node) override {
        WriteIndentation();
        Out_ << "Any " << (node.HasItem() ? "with data" : "empty") << " {";
        WriteNewline();

        {
            TIndentScope scope(this);
            node.GetType()->Accept(*this);
            if (node.HasItem()) {
                WriteIndentation();
                const auto& item = node.GetItem();
                Out_ << "Item " << (item.IsImmediate() ? "immediate" : "not immediate") << " {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    item.GetNode()->Accept(*this);
                }

                WriteIndentation();
                Out_ << "}";
                WriteNewline();
            }
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TTupleLiteral& node) override {
        WriteIndentation();
        Out_ << "Tuple {";
        WriteNewline();

        {
            TIndentScope scope(this);
            for (ui32 index = 0; index < node.GetValuesCount(); ++index) {
                WriteIndentation();
                const auto& value = node.GetValue(index);
                Out_ << "#" << index << " " << (value.IsImmediate() ? "immediate" : "not immediate") << " {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    value.GetNode()->Accept(*this);
                }

                WriteIndentation();
                Out_ << "}";
                WriteNewline();
            }
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TVariantLiteral& node) override {
        WriteIndentation();
        Out_ << "Variant with alternative " << node.GetIndex() << " {";
        WriteNewline();

        {
            TIndentScope scope(this);
            node.GetType()->Accept(*this);
            WriteIndentation();
            const auto& item = node.GetItem();
            Out_ << "Item " << (item.IsImmediate() ? "immediate" : "not immediate") << " {";
            WriteNewline();

            {
                TIndentScope scope2(this);
                item.GetNode()->Accept(*this);
            }

            WriteIndentation();
            Out_ << "}";
            WriteNewline();
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    void Visit(TTaggedType& node) override {
        WriteIndentation();
        Out_ << "Type (Tagged) (" << node.GetTag() << ") {";
        WriteNewline();

        {
            TIndentScope scope(this);
            WriteIndentation();
            Out_ << "Tagged base type: {";
            WriteNewline();

            {
                TIndentScope scope2(this);
                node.GetBaseType()->Accept(*this);
            }

            WriteIndentation();
            Out_ << "}";
            WriteNewline();
        }

        WriteIndentation();
        Out_ << "}";
        WriteNewline();
    }

    TString ToString() {
        return Out_.Str();
    }

private:
    void WriteIndentation() {
        if (SingleLine_) {
        } else {
            for (ui32 i = 0; i < 2 * Indent_; ++i) {
                Out_ << ' ';
            }
        }
    }

    void WriteNewline() {
        if (SingleLine_) {
            Out_ << ' ';
        } else {
            Out_ << '\n';
        }
    }

private:
    const bool SingleLine_;
    TStringStream Out_;
    ui32 Indent_;
};
} // namespace

TString PrintNode(const TNode* node, bool singleLine) {
    TPrintVisitor visitor(singleLine);
    const_cast<TNode*>(node)->Accept(visitor);
    return visitor.ToString();
}

} // namespace NKikimr::NMiniKQL

template <>
void Out<NKikimr::NMiniKQL::TType>(
    IOutputStream& os,
    TTypeTraits<NKikimr::NMiniKQL::TType>::TFuncParam t)
{
    os << PrintNode(&t, true);
}
