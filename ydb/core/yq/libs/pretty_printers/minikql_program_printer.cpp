#include "minikql_program_printer.h"

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_printer.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/mkql_node_visitor.h>

namespace NFq {

namespace {

using namespace NKikimr::NMiniKQL;

struct TPrettyPrinter : public INodeVisitor {
public:
    // Types
    void Visit(TTypeType&) override {
        Out << "TypeType";
    }
    void Visit(TVoidType&) override {
        Out << "VoidType";
    }
    void Visit(TNullType&) override {
        Out << "NullType";
    }
    void Visit(TEmptyListType&) override {
        Out << "EmptyListType";
    }
    void Visit(TEmptyDictType&) override {
        Out << "EmptyDictType";
    }
    void Visit(TDataType&) override {
        Out << "DataType";
    }
    void Visit(TPgType&) override {
        Out << "PgType";
    }
    void Visit(TStructType&) override {
        Out << "StructType";
    }
    void Visit(TListType&) override {
        Out << "ListType";
    }
    void Visit(TOptionalType&) override {
        Out << "OptionalType";
    }
    void Visit(TDictType&) override {
        Out << "DictType";
    }
    void Visit(TCallableType&) override {
        Out << "CallableType";
    }
    void Visit(TAnyType&) override {
        Out << "AnyType";
    }
    void Visit(TTupleType&) override {
        Out << "TupleType";
    }
    void Visit(TResourceType&) override {
        Out << "ResourceType";
    }
    void Visit(TVariantType&) override {
        Out << "VariantType";
    }
    void Visit(TStreamType&) override {
        Out << "StreamType";
    }
    void Visit(TFlowType&) override {
        Out << "FlowType";
    }
    void Visit(TTaggedType&) override {
        Out << "TaggedType";
    }
    void Visit(TBlockType&) override {
        Out << "BlockType";
    }

    void Visit(TMultiType&) override {
        Out << "MultiType";
    }

    // Values
    void Visit(TVoid&) override {
        Out << "void";
    }

    void Visit(NKikimr::NMiniKQL::TNull&) override {
        Out << "null";
    }

    void Visit(TEmptyList&) override {
        Out << "()";
    }

    void Visit(TEmptyDict&) override {
        Out << "()";
    }

    void Visit(TDataLiteral& node) override {
        if (node.GetType()->GetSchemeType() == 0) {
            Out << "null";
        } else {
            Out << '\'' << TString(node.AsValue().AsStringRef()).Quote();
        }
    }

    void Visit(TStructLiteral& node) override {
        Out << '(';
        TIndentScope scope(*this);
        TStructType* type = node.GetType();
        for (size_t i = 0; i < node.GetValuesCount(); ++i) {
            if (i) {
                NewLine();
            }
            Out << "('" << type->GetMemberName(i) << ' ';
            const TRuntimeNode& val = node.GetValue(i);
            val.GetNode()->Accept(*this);
            Out << ')';
        }
        Out << ')';
    }

    void Visit(TListLiteral& node) override {
        Out << '(';
        TIndentScope scope(*this);
        for (size_t i = 0; i < node.GetItemsCount(); ++i) {
            if (i) {
                Out << ' ';
            }
            const TRuntimeNode& val = node.GetItems()[i];
            val.GetNode()->Accept(*this);
        }
        Out << ')';
    }

    void Visit(TOptionalLiteral& node) override {
        if (node.HasItem()) {
            const TRuntimeNode& item = node.GetItem();
            item.GetNode()->Accept(*this);
        } else {
            Out << "null";
        }
    }

    void Visit(TDictLiteral& node) override {
        Out << '(';
        TIndentScope scope(*this);
        for (size_t i = 0; i < node.GetItemsCount(); ++i) {
            NewLine();
            const std::pair<TRuntimeNode, TRuntimeNode>& item = node.GetItem(i);
            item.first.GetNode()->Accept(*this);
            Out << ':';
            item.second.GetNode()->Accept(*this);
        }
        Out << ')';
    }

    void Visit(TCallable& node) override {
        TCallableType* type = node.GetType();
        Out << '(' << type->GetName();
        TIndentScope scope(*this);
        for (size_t i = 0; i < node.GetInputsCount(); ++i) {
            NewLine();
            const TRuntimeNode& input = node.GetInput(i);
            input.GetNode()->Accept(*this);
        }
        Out << ')';
    }

    void Visit(TAny& node) override {
        if (node.HasItem()) {
            const TRuntimeNode& item = node.GetItem();
            item.GetNode()->Accept(*this);
        } else {
            Out << "null";
        }
    }

    void Visit(TTupleLiteral& node) override {
        Out << '(';
        TIndentScope scope(*this);
        for (size_t i = 0; i < node.GetValuesCount(); ++i) {
            if (i) {
                Out << ' ';
            }
            const TRuntimeNode& val = node.GetValue(i);
            val.GetNode()->Accept(*this);
        }
        Out << ')';
    }

    void Visit(TVariantLiteral& node) override {
        const TRuntimeNode& item = node.GetItem();
        item.GetNode()->Accept(*this);
    }

    TString GetResult() {
        return Out;
    }

    struct TIndentScope {
        TIndentScope(TPrettyPrinter& printer)
            : Printer(printer)
        {
            ++Printer.CurrentIndent;
        }

        ~TIndentScope() {
            --Printer.CurrentIndent;
        }

        TPrettyPrinter& Printer;
    };

    explicit TPrettyPrinter(size_t initialIndentChars)
        : BaseIndent(initialIndentChars)
    {
        Indent();
    }

    void Indent() {
        for (size_t i = 0, cnt = CurrentIndent * 2 + BaseIndent; i < cnt; ++i) {
            Out << ' ';
        }
    }

    void NewLine() {
        Out << '\n';
        Indent();
    }

    TStringBuilder Out;
    size_t CurrentIndent = 0;
    const size_t BaseIndent = 0;
};

} // namespace

TString PrettyPrintMkqlProgram(const NKikimr::NMiniKQL::TNode* node, size_t initialIndentChars) {
    TPrettyPrinter printer(initialIndentChars);
    const_cast<NKikimr::NMiniKQL::TNode*>(node)->Accept(printer);
    return printer.GetResult();
}

TString PrettyPrintMkqlProgram(const NKikimr::NMiniKQL::TRuntimeNode& node, size_t initialIndentChars) {
    return PrettyPrintMkqlProgram(node.GetNode(), initialIndentChars);
}

TString PrettyPrintMkqlProgram(const TString& rawProgram, size_t initialIndentChars) {
    TScopedAlloc alloc(__LOCATION__);
    NKikimr::NMiniKQL::TTypeEnvironment env(alloc);
    NKikimr::NMiniKQL::TRuntimeNode node = DeserializeRuntimeNode(rawProgram, env);
    return PrettyPrintMkqlProgram(node, initialIndentChars);
}

} // namespace NFq
