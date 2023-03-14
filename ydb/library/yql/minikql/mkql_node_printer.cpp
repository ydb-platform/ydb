#include "mkql_node_printer.h"
#include "mkql_node_visitor.h"
#include <util/stream/str.h>
#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {
    class TPrintVisitor : public INodeVisitor {
    friend class TIndentScope;
    public:
        struct TIndentScope {
            TIndentScope(TPrintVisitor* self)
                : Self(self)
            {
                ++Self->Indent;
            }

            ~TIndentScope() {
                --Self->Indent;
            }

            TPrintVisitor* Self;
        };

        TPrintVisitor(bool singleLine)
            : SingleLine(singleLine)
            , Indent(0)
        {}

        void Visit(TTypeType& node) override {
            Y_UNUSED(node);
            WriteIndentation();
            Out << "Type (Type)";
            WriteNewline();
        }

        void Visit(TVoidType& node) override {
            Y_UNUSED(node);
            WriteIndentation();
            Out << "Type (Void) ";
            WriteNewline();
        }

        void Visit(TNullType& node) override {
            Y_UNUSED(node);
            WriteIndentation();
            Out << "Type (Null) ";
            WriteNewline();
        }

        void Visit(TEmptyListType& node) override {
            Y_UNUSED(node);
            WriteIndentation();
            Out << "Type (EmptyList) ";
            WriteNewline();
        }

        void Visit(TEmptyDictType& node) override {
            Y_UNUSED(node);
            WriteIndentation();
            Out << "Type (EmptyDict) ";
            WriteNewline();
        }

        void Visit(TDataType& node) override {
            WriteIndentation();
            auto slot = NUdf::FindDataSlot(node.GetSchemeType());
            Out << "Type (Data), schemeType: ";
            if (slot) {
                Out << GetDataTypeInfo(*slot).Name;
                if (node.GetSchemeType() == NUdf::TDataType<NUdf::TDecimal>::Id) {
                    const auto params = static_cast<TDataDecimalType&>(node).GetParams();
                    Out << '(' << int(params.first) << ',' << int(params.second) << ')';
                }
            } else {
                Out << "<" << node.GetSchemeType() << ">";
            }

            Out << ", schemeTypeId: ";
            Out << node.GetSchemeType();
            WriteNewline();
        }

        void Visit(TPgType& node) override {
            WriteIndentation();
            Out << "Type (Pg), name: " << NYql::NPg::LookupType(node.GetTypeId()).Name;
            WriteNewline();
        }

        void Visit(TStructType& node) override {
            WriteIndentation();
            Out << "Type (Struct) with " << node.GetMembersCount() << " members {";
            WriteNewline();

            {
                TIndentScope scope(this);
                for (ui32 index = 0; index < node.GetMembersCount(); ++index) {
                    WriteIndentation();
                    Out << "Member [" << node.GetMemberName(index) << "] : {";
                    WriteNewline();

                    {
                        TIndentScope scope2(this);
                        node.GetMemberType(index)->Accept(*this);
                    }

                    WriteIndentation();
                    Out << "}";
                    WriteNewline();
                }
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TListType& node) override {
            WriteIndentation();
            Out << "Type (List) {";
            WriteNewline();

            {
                TIndentScope scope(this);
                WriteIndentation();
                Out << "List item type: {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    node.GetItemType()->Accept(*this);
                }

                WriteIndentation();
                Out << "}";
                WriteNewline();
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TStreamType& node) override {
            WriteIndentation();
            Out << "Type (Stream) {";
            WriteNewline();

            {
                TIndentScope scope(this);
                WriteIndentation();
                Out << "Stream item type: {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    node.GetItemType()->Accept(*this);
                }

                WriteIndentation();
                Out << "}";
                WriteNewline();
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TFlowType& node) override {
            WriteIndentation();
            Out << "Type (Flow) {";
            WriteNewline();

            {
                TIndentScope scope(this);
                WriteIndentation();
                Out << "Flow item type: {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    node.GetItemType()->Accept(*this);
                }

                WriteIndentation();
                Out << "}";
                WriteNewline();
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TBlockType& node) override {
            WriteIndentation();
            Out << "Type (Block) {";
            WriteNewline();

            {
                TIndentScope scope(this);
                WriteIndentation();
                Out << "Block item type: {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    node.GetItemType()->Accept(*this);
                }

                WriteIndentation();
                Out << "}";
                WriteNewline();

                WriteIndentation();
                Out << "Block shape: " << (node.GetShape() == TBlockType::EShape::Scalar ? "Scalar" : "Many");
                WriteNewline();
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TOptionalType& node) override {
            WriteIndentation();
            Out << "Type (Optional) {";
            WriteNewline();

            {
                TIndentScope scope(this);
                WriteIndentation();
                Out << "Optional item type: {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    node.GetItemType()->Accept(*this);
                }

                WriteIndentation();
                Out << "}";
                WriteNewline();
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TDictType& node) override {
            WriteIndentation();
            Out << "Type (Dict) {";
            WriteNewline();

            {
                TIndentScope scope(this);
                WriteIndentation();
                Out << "Key type: {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    node.GetKeyType()->Accept(*this);
                }

                WriteIndentation();
                Out << "}";
                WriteNewline();

                WriteIndentation();
                Out << "Payload type: {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    node.GetPayloadType()->Accept(*this);
                }

                WriteIndentation();
                Out << "}";
                WriteNewline();
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TCallableType& node) override {
            WriteIndentation();
            Out << "Type (Callable), name: [" << node.GetName() << "] with " << node.GetArgumentsCount() << " args {";
            WriteNewline();

            {
                TIndentScope scope(this);
                WriteIndentation();
                Out << "Return type";
                if (node.IsMergeDisabled())
                    Out << ", merge disabled";
                if (node.GetOptionalArgumentsCount() != 0)
                    Out << ", optional args: " << node.GetOptionalArgumentsCount();
                Out << " : {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    node.GetReturnType()->Accept(*this);
                }

                WriteIndentation();
                Out << "}";
                WriteNewline();
                for (ui32 index = 0; index < node.GetArgumentsCount(); ++index) {
                    WriteIndentation();
                    const auto& type = node.GetArgumentType(index);
                    Out << "Argument #" << index << " : {";
                    WriteNewline();

                    {
                        TIndentScope scope2(this);
                        type->Accept(*this);
                    }

                    WriteIndentation();
                    Out << "}";
                    WriteNewline();
                }

                if (node.GetPayload()) {
                    WriteIndentation();
                    Out << "Payload: {";
                    WriteNewline();

                    {
                        TIndentScope scope2(this);
                        node.GetPayload()->Accept(*this);
                    }

                    WriteIndentation();
                    Out << "}";
                    WriteNewline();
                }
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TAnyType& node) override {
            Y_UNUSED(node);
            WriteIndentation();
            Out << "Type (Any) ";
            WriteNewline();
        }

        template<typename T>
        void VisitTupleLike(T& node, std::string_view name) {
            WriteIndentation();
            Out << "Type (" << name << ") with " << node.GetElementsCount() << " elements {";
            WriteNewline();

            {
                TIndentScope scope(this);
                for (ui32 index = 0; index < node.GetElementsCount(); ++index) {
                    WriteIndentation();
                    Out << "#" << index << " : {";
                    WriteNewline();

                    {
                        TIndentScope scope2(this);
                        node.GetElementType(index)->Accept(*this);
                    }

                    WriteIndentation();
                    Out << "}";
                    WriteNewline();
                }
            }

            WriteIndentation();
            Out << "}";
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
            Out << "Type (Resource) (" << node.GetTag() << ")";
            WriteNewline();
        }

        void Visit(TVariantType& node) override {
            WriteIndentation();
            Out << "Type (Variant) {";
            WriteNewline();

            {
                TIndentScope scope(this);
                WriteIndentation();
                Out << "Underlying type: {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    node.GetUnderlyingType()->Accept(*this);
                }

                WriteIndentation();
                Out << "}";
                WriteNewline();
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TVoid& node) override {
            WriteIndentation();
            Out << "Void {";
            WriteNewline();

            {
                TIndentScope scope(this);
                node.GetType()->Accept(*this);
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TNull& node) override {
            WriteIndentation();
            Out << "Null {";
            WriteNewline();

            {
                TIndentScope scope(this);
                node.GetType()->Accept(*this);
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TEmptyList& node) override {
            WriteIndentation();
            Out << "EmptyList {";
            WriteNewline();

            {
                TIndentScope scope(this);
                node.GetType()->Accept(*this);
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TEmptyDict& node) override {
            WriteIndentation();
            Out << "EmptyDict {";
            WriteNewline();

            {
                TIndentScope scope(this);
                node.GetType()->Accept(*this);
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TDataLiteral& node) override {
            WriteIndentation();
            Out << "Data {";
            WriteNewline();

            {
                TIndentScope scope(this);
                node.GetType()->Accept(*this);

                WriteIndentation();
                if (node.GetType()->GetSchemeType() == 0) {
                    Out << "null";
                    WriteNewline();
                } else {
                    Out << TString(node.AsValue().AsStringRef()).Quote();
                    WriteNewline();
                }
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TStructLiteral& node) override {
            WriteIndentation();
            Out << "Struct {";
            WriteNewline();

            {
                TIndentScope scope(this);
                for (ui32 index = 0; index < node.GetValuesCount(); ++index) {
                    WriteIndentation();
                    const auto& value = node.GetValue(index);
                    Out << "Member [" << node.GetType()->GetMemberName(index) << "], "
                        << (value.IsImmediate() ? "immediate" : "not immediate") << " {";
                    WriteNewline();

                    {
                        TIndentScope scope2(this);
                        value.GetNode()->Accept(*this);
                    }

                    WriteIndentation();
                    Out << "}";
                    WriteNewline();
                }
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TListLiteral& node) override {
            WriteIndentation();
            Out << "List with " << node.GetItemsCount() << " items {";
            WriteNewline();

            {
                TIndentScope scope(this);
                node.GetType()->Accept(*this);
                ui32 index = 0;
                for (ui32 i = 0; i < node.GetItemsCount(); ++i) {
                    WriteIndentation();
                    const auto& item = node.GetItems()[i];
                    Out << "Item #" << index << ", " << (item.IsImmediate() ? "immediate" : "not immediate") << " {";
                    WriteNewline();

                    {
                        TIndentScope scope2(this);
                        item.GetNode()->Accept(*this);
                    }

                    WriteIndentation();
                    Out << "}";
                    WriteNewline();
                }
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TOptionalLiteral& node) override {
            WriteIndentation();
            Out << "Optional " << (node.HasItem() ? "with data" : "empty") << " {";
            WriteNewline();

            {
                TIndentScope scope(this);
                node.GetType()->Accept(*this);
                if (node.HasItem()) {
                    WriteIndentation();
                    const auto& item = node.GetItem();
                    Out << "Item " << (item.IsImmediate() ? "immediate" : "not immediate") << " {";
                    WriteNewline();

                    {
                        TIndentScope scope2(this);
                        item.GetNode()->Accept(*this);
                    }

                    WriteIndentation();
                    Out << "}";
                    WriteNewline();
                }
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TDictLiteral& node) override {
            WriteIndentation();
            Out << "Dict with " << node.GetItemsCount() << " items {";
            WriteNewline();

            {
                TIndentScope scope(this);
                node.GetType()->Accept(*this);
                for (ui32 index = 0; index < node.GetItemsCount(); ++index) {
                    WriteIndentation();
                    const auto& item = node.GetItem(index);
                    Out << "Key of item #" << index << ", " << (item.first.IsImmediate() ? "immediate" : "not immediate") << " {";
                    WriteNewline();

                    {
                        TIndentScope scope2(this);
                        item.first.GetNode()->Accept(*this);
                    }

                    WriteIndentation();
                    Out << "}";
                    WriteNewline();

                    WriteIndentation();
                    Out << "Payload of item #" << index << ", " << (item.second.IsImmediate() ? "immediate" : "not immediate") << " {";
                    WriteNewline();

                    {
                        TIndentScope scope2(this);
                        item.second.GetNode()->Accept(*this);
                    }

                    WriteIndentation();
                    Out << "}";
                    WriteNewline();
                }
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TCallable& node) override {
            WriteIndentation();
            Out << "Callable";
            if (node.GetUniqueId() != 0)
                Out << ", uniqueId: " << node.GetUniqueId();
            Out << " {";
            WriteNewline();

            {
                TIndentScope scope(this);
                node.GetType()->Accept(*this);

                for (ui32 index = 0; index < node.GetInputsCount(); ++index) {
                    WriteIndentation();
                    const auto& input = node.GetInput(index);
                    Out << "Input #" << index << ", " << (input.IsImmediate() ? "immediate" : "not immediate") << " {";
                    WriteNewline();

                    {
                        TIndentScope scope3(this);
                        input.GetNode()->Accept(*this);
                    }

                    WriteIndentation();
                    Out << "}";
                    WriteNewline();
                }

                if (node.HasResult()) {
                    WriteIndentation();
                    Out << "Result, " << (node.GetResult().IsImmediate() ? "immediate" : "not immediate") << " {";
                    WriteNewline();

                    {
                        TIndentScope scope3(this);
                        node.GetResult().GetNode()->Accept(*this);
                    }

                    WriteIndentation();
                    Out << "}";
                    WriteNewline();
                }
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TAny& node) override {
            WriteIndentation();
            Out << "Any " << (node.HasItem() ? "with data" : "empty") << " {";
            WriteNewline();

            {
                TIndentScope scope(this);
                node.GetType()->Accept(*this);
                if (node.HasItem()) {
                    WriteIndentation();
                    const auto& item = node.GetItem();
                    Out << "Item " << (item.IsImmediate() ? "immediate" : "not immediate") << " {";
                    WriteNewline();

                    {
                        TIndentScope scope2(this);
                        item.GetNode()->Accept(*this);
                    }

                    WriteIndentation();
                    Out << "}";
                    WriteNewline();
                }
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TTupleLiteral& node) override {
            WriteIndentation();
            Out << "Tuple {";
            WriteNewline();

            {
                TIndentScope scope(this);
                for (ui32 index = 0; index < node.GetValuesCount(); ++index) {
                    WriteIndentation();
                    const auto& value = node.GetValue(index);
                    Out << "#" << index << " " << (value.IsImmediate() ? "immediate" : "not immediate") << " {";
                    WriteNewline();

                    {
                        TIndentScope scope2(this);
                        value.GetNode()->Accept(*this);
                    }

                    WriteIndentation();
                    Out << "}";
                    WriteNewline();
                }
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TVariantLiteral& node) override {
            WriteIndentation();
            Out << "Variant with alternative " << node.GetIndex() << " {";
            WriteNewline();

            {
                TIndentScope scope(this);
                node.GetType()->Accept(*this);
                WriteIndentation();
                const auto& item = node.GetItem();
                Out << "Item " << (item.IsImmediate() ? "immediate" : "not immediate") << " {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    item.GetNode()->Accept(*this);
                }

                WriteIndentation();
                Out << "}";
                WriteNewline();
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        void Visit(TTaggedType& node) override {
            WriteIndentation();
            Out << "Type (Tagged) (" << node.GetTag() << ") {";
            WriteNewline();

            {
                TIndentScope scope(this);
                WriteIndentation();
                Out << "Tagged base type: {";
                WriteNewline();

                {
                    TIndentScope scope2(this);
                    node.GetBaseType()->Accept(*this);
                }

                WriteIndentation();
                Out << "}";
                WriteNewline();
            }

            WriteIndentation();
            Out << "}";
            WriteNewline();
        }

        TString ToString() {
            return Out.Str();
        }

    private:
        void WriteIndentation() {
            if (SingleLine) {
            } else {
                for (ui32 i = 0; i < 2 * Indent; ++i) {
                    Out << ' ';
                }
            }
        }

        void WriteNewline() {
            if (SingleLine) {
                Out << ' ';
            } else {
                Out << '\n';
            }
        }

    private:
        const bool SingleLine;
        TStringStream Out;
        ui32 Indent;
    };
}

TString PrintNode(const TNode* node, bool singleLine) {
    TPrintVisitor visitor(singleLine);
    const_cast<TNode*>(node)->Accept(visitor);
    return visitor.ToString();
}

}
}

template <>
void Out<NKikimr::NMiniKQL::TType>(
    IOutputStream& os,
    TTypeTraits<NKikimr::NMiniKQL::TType>::TFuncParam t
)
{
    os << PrintNode(&t, true);
}
