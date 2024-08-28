#include "yql_expr.h"
#include "yql_ast_annotation.h"
#include "yql_gc_nodes.h"

#include <ydb/library/yql/utils/utf8.h>
#include <ydb/library/yql/utils/fetch/fetch.h>
#include <ydb/library/yql/core/issue/yql_issue.h>

#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/hash.h>
#include <util/generic/size_literals.h>
#include <util/string/cast.h>
#include <util/string/join.h>

#include <util/digest/fnv.h>
#include <util/digest/murmur.h>
#include <util/digest/city.h>
#include <util/digest/numeric.h>
#include <util/string/cast.h>

#include <map>
#include <unordered_set>

namespace NYql {

const TStringBuf ZeroString = "";
const char Dot = '.';
const char Sep = '/';
const TStringBuf PkgPrefix = "pkg";

void ReportError(TExprContext& ctx, const TIssue& issue) {
    ctx.AddError(issue);
}

namespace {
    template <typename T>
    const T* FindType(const T& sample, TExprContext& ctx) {
        const auto it = ctx.TypeSet.find(&sample);
        return ctx.TypeSet.cend() != it ? static_cast<const T*>(*it) : nullptr;
    }

    template <typename T, typename... Args>
    const T* AddType(TExprContext& ctx, Args&&... args) {
        Y_DEBUG_ABORT_UNLESS(!ctx.Frozen);
        ctx.TypeNodes.emplace(new T(std::forward<Args>(args)...));
        const auto ins = ctx.TypeSet.emplace(ctx.TypeNodes.top().get());
        return static_cast<const T*>(*ins.first);
    }

    void DumpNode(const TExprNode& node, IOutputStream& out, ui32 level, TNodeSet& visited) {
        for (ui32 i = 0; i < level; ++i) {
            out.Write(' ');
        }

        out << "#" << node.UniqueId() << " [" << node.Type() << "]";
        if (node.Type() == TExprNode::Atom || node.Type() == TExprNode::Callable || node.Type() == TExprNode::Argument) {
            out << " <" << node.Content() << ">";
        }

        constexpr bool WithTypes = false;
        constexpr bool WithConstraints = false;
        constexpr bool WithScope = false;

        if constexpr (WithTypes) {
            if (node.GetTypeAnn()) {
                out << ", " << *node.GetTypeAnn();
            }
        }

        if constexpr (WithConstraints) {
            if (node.GetState() >= TExprNode::EState::ConstrComplete) {
                out << ", " << node.GetConstraintSet();
            }
        }

        if constexpr (WithScope) {
            if (const auto scope = node.GetDependencyScope()) {
                out << ", (";
                if (const auto outer = scope->first) {
                    out << '#' << outer->UniqueId();
                } else {
                    out << "null";
                }

                out << ',';
                if (const auto inner = scope->second) {
                    out << '#' << inner->UniqueId();
                } else {
                    out << "null";
                }
                out << ')';
            }
        }

        bool showChildren = true;
        if (!visited.emplace(&node).second) {
            if (node.Type() == TExprNode::Callable || node.Type() == TExprNode::List
                || node.Type() == TExprNode::Lambda || node.Type() == TExprNode::Arguments) {
                out << " ...";
                showChildren = false;
            }
        }

        out << "\n";
        if (showChildren) {
            for (auto& child : node.Children()) {
                DumpNode(*child, out, level + 1, visited);
            }
        }
    }

    struct TContext {
        struct TFrame {
            THashMap<TString, TExprNode::TListType> Bindings;
            THashMap<TString, TString> Imports;
            TExprNode::TListType Return;
        };

        TExprContext& Expr;
        TVector<TFrame> Frames;
        TLibraryCohesion Cohesion;
        std::unordered_set<TString> OverrideLibraries;

        TNodeOnNodeOwnedMap DeepClones;

        const TAnnotationNodeMap* Annotations = nullptr;
        IModuleResolver* ModuleResolver = nullptr;
        IUrlListerManager* UrlListerManager = nullptr;
        ui32 TypeAnnotationIndex = Max<ui32>();
        TString File;
        ui16 SyntaxVersion = 0;

        TContext(TExprContext& expr)
            : Expr(expr)
        {
        }

        void AddError(const TAstNode& node, const TString& message) {
            Expr.AddError(TIssue(node.GetPosition(), message));
        }

        void AddInfo(const TAstNode& node, const TString& message) {
            auto issue = TIssue(node.GetPosition(), message);
            issue.SetCode(TIssuesIds::INFO, TSeverityIds::S_INFO);
            Expr.AddError(issue);
        }

        TExprNode::TPtr&& ProcessNode(const TAstNode& node, TExprNode::TPtr&& exprNode) {
            if (TypeAnnotationIndex != Max<ui32>()) {
                exprNode->SetTypeAnn(CompileTypeAnnotation(node));
            }

            return std::move(exprNode);
        }

        void PushFrame() {
            Frames.push_back(TFrame());
        }

        void PopFrame() {
            Frames.pop_back();
        }

        TExprNode::TListType FindBinding(const TStringBuf& name) const {
            for (auto it = Frames.crbegin(); it != Frames.crend(); ++it) {
                const auto r = it->Bindings.find(name);
                if (it->Bindings.cend() != r)
                    return r->second;
            }

            return {};
        }

        TString FindImport(const TStringBuf& name) const {
            for (auto it = Frames.crbegin(); it != Frames.crend(); ++it) {
                const auto r = it->Imports.find(name);
                if (it->Imports.cend() != r)
                    return r->second;
            }

            return TString();
        }

        const TTypeAnnotationNode* CompileTypeAnnotation(const TAstNode& node) {
            auto ptr = Annotations->FindPtr(&node);
            if (!ptr || TypeAnnotationIndex >= ptr->size()) {
                AddError(node, "Failed to load type annotation");
                return nullptr;
            }

            return CompileTypeAnnotationNode(*(*ptr)[TypeAnnotationIndex]);
        }

        const TTypeAnnotationNode* CompileTypeAnnotationNode(const TAstNode& node) {
            if (node.IsAtom()) {
                if (node.GetContent() == TStringBuf(".")) {
                    return nullptr;
                }
                else if (node.GetContent() == TStringBuf("Unit")) {
                    return Expr.MakeType<TUnitExprType>();
                }
                else if (node.GetContent() == TStringBuf("World")) {
                    return Expr.MakeType<TWorldExprType>();
                }
                else if (node.GetContent() == TStringBuf("Void")) {
                    return Expr.MakeType<TVoidExprType>();
                }
                else if (node.GetContent() == TStringBuf("Null")) {
                    return Expr.MakeType<TNullExprType>();
                }
                else if (node.GetContent() == TStringBuf("Generic")) {
                    return Expr.MakeType<TGenericExprType>();
                }
                else if (node.GetContent() == TStringBuf("EmptyList")) {
                    return Expr.MakeType<TEmptyListExprType>();
                }
                else if (node.GetContent() == TStringBuf("EmptyDict")) {
                    return Expr.MakeType<TEmptyDictExprType>();
                }
                else {
                    AddError(node, TStringBuilder() << "Unknown type annotation: " << node.GetContent());
                    return nullptr;
                }
            } else {
                if (node.GetChildrenCount() == 0) {
                    AddError(node, "Bad type annotation, expected not empty list");
                    return nullptr;
                }

                if (!node.GetChild(0)->IsAtom()) {
                    AddError(node, "Bad type annotation, first list item must be an atom");
                    return nullptr;
                }

                auto content = node.GetChild(0)->GetContent();
                if (content == TStringBuf("Data")) {
                    const auto count = node.GetChildrenCount();
                    if (!(count == 2 || count == 4) || !node.GetChild(1)->IsAtom()) {
                        AddError(node, "Bad data type annotation");
                        return nullptr;
                    }

                    auto slot = NUdf::FindDataSlot(node.GetChild(1)->GetContent());
                    if (!slot) {
                        AddError(node, "Bad data type annotation");
                        return nullptr;
                    }

                    if (count == 2) {
                        return Expr.MakeType<TDataExprType>(*slot);
                    } else {
                        if (!(node.GetChild(2)->IsAtom() && node.GetChild(3)->IsAtom())) {
                            AddError(node, "Bad data type annotation");
                            return nullptr;
                        }
                        auto ann = Expr.MakeType<TDataExprParamsType>(*slot, node.GetChild(2)->GetContent(), node.GetChild(3)->GetContent());
                        if (!ann->Validate(node.GetPosition(), Expr)) {
                            return nullptr;
                        }

                        return ann;
                    }
                }  else if (content == TStringBuf("Pg")) {
                    const auto count = node.GetChildrenCount();
                    if (count != 2 || !node.GetChild(1)->IsAtom()) {
                        AddError(node, "Bad data type annotation");
                        return nullptr;
                    }

                    auto typeId = NPg::LookupType(TString(node.GetChild(1)->GetContent())).TypeId;
                    return Expr.MakeType<TPgExprType>(typeId);
                } else if (content == TStringBuf("List")) {
                    if (node.GetChildrenCount() != 2) {
                        AddError(node, "Bad list type annotation");
                        return nullptr;
                    }

                    auto r = CompileTypeAnnotationNode(*node.GetChild(1));
                    if (!r)
                        return nullptr;

                    return Expr.MakeType<TListExprType>(r);
                } else if (content == TStringBuf("Stream")) {
                    if (node.GetChildrenCount() != 2) {
                        AddError(node, "Bad stream type annotation");
                        return nullptr;
                    }

                    auto r = CompileTypeAnnotationNode(*node.GetChild(1));
                    if (!r)
                        return nullptr;

                    return Expr.MakeType<TStreamExprType>(r);
                } else if (content == TStringBuf("Struct")) {
                    TVector<const TItemExprType*> children;
                    for (size_t index = 1; index < node.GetChildrenCount(); ++index) {
                        auto r = CompileTypeAnnotationNode(*node.GetChild(index));
                        if (!r)
                            return nullptr;

                        if (r->GetKind() != ETypeAnnotationKind::Item) {
                            AddError(node, "Expected item type annotation");
                            return nullptr;
                        }

                        children.push_back(r->Cast<TItemExprType>());
                    }

                    auto ann = Expr.MakeType<TStructExprType>(children);
                    if (!ann->Validate(node.GetPosition(), Expr)) {
                        return nullptr;
                    }

                    return ann;
                } else if (content == TStringBuf("Multi")) {
                    TTypeAnnotationNode::TListType children;
                    for (size_t index = 1; index < node.GetChildrenCount(); ++index) {
                        auto r = CompileTypeAnnotationNode(*node.GetChild(index));
                        if (!r)
                            return nullptr;

                        children.push_back(r);
                    }

                    auto ann = Expr.MakeType<TMultiExprType>(children);
                    if (!ann->Validate(node.GetPosition(), Expr)) {
                        return nullptr;
                    }

                    return ann;
                } else if (content == TStringBuf("Tuple")) {
                    TTypeAnnotationNode::TListType children;
                    for (size_t index = 1; index < node.GetChildrenCount(); ++index) {
                        auto r = CompileTypeAnnotationNode(*node.GetChild(index));
                        if (!r)
                            return nullptr;

                        children.push_back(r);
                    }

                    auto ann = Expr.MakeType<TTupleExprType>(children);
                    if (!ann->Validate(node.GetPosition(), Expr)) {
                        return nullptr;
                    }

                    return ann;
                } else if (content == TStringBuf("Item")) {
                    if (node.GetChildrenCount() != 3 || !node.GetChild(1)->IsAtom()) {
                        AddError(node, "Bad item type annotation");
                        return nullptr;
                    }

                    auto r = CompileTypeAnnotationNode(*node.GetChild(2));
                    if (!r)
                        return nullptr;

                    return Expr.MakeType<TItemExprType>(TString(node.GetChild(1)->GetContent()), r);
                } else if (content == TStringBuf("Optional")) {
                    if (node.GetChildrenCount() != 2) {
                        AddError(node, "Bad optional type annotation");
                        return nullptr;
                    }

                    auto r = CompileTypeAnnotationNode(*node.GetChild(1));
                    if (!r)
                        return nullptr;

                    return Expr.MakeType<TOptionalExprType>(r);
                } else if (content == TStringBuf("Type")) {
                    if (node.GetChildrenCount() != 2) {
                        AddError(node, "Bad type annotation");
                        return nullptr;
                    }

                    auto r = CompileTypeAnnotationNode(*node.GetChild(1));
                    if (!r)
                        return nullptr;

                    return Expr.MakeType<TTypeExprType>(r);
                }
                else if (content == TStringBuf("Dict")) {
                    if (node.GetChildrenCount() != 3) {
                        AddError(node, "Bad dict annotation");
                        return nullptr;
                    }

                    auto r1 = CompileTypeAnnotationNode(*node.GetChild(1));
                    if (!r1)
                        return nullptr;

                    auto r2 = CompileTypeAnnotationNode(*node.GetChild(2));
                    if (!r2)
                        return nullptr;

                    auto ann = Expr.MakeType<TDictExprType>(r1, r2);
                    if (!ann->Validate(node.GetPosition(), Expr)) {
                        return nullptr;
                    }

                    return ann;
                }
                else if (content == TStringBuf("Callable")) {
                    if (node.GetChildrenCount() <= 2) {
                        AddError(node, "Bad callable annotation");
                        return nullptr;
                    }

                    TVector<TCallableExprType::TArgumentInfo> args;
                    size_t optCount = 0;
                    TString payload;
                    if (!node.GetChild(1)->IsList()) {
                        AddError(node, "Bad callable annotation - expected list");
                        return nullptr;
                    }

                    if (node.GetChild(1)->GetChildrenCount() > 2) {
                        AddError(node, "Bad callable annotation - too many settings nodes");
                        return nullptr;
                    }

                    if (node.GetChild(1)->GetChildrenCount() > 0) {
                        auto optChild = node.GetChild(1)->GetChild(0);
                        if (!optChild->IsAtom()) {
                            AddError(node, "Bad callable annotation - expected atom");
                            return nullptr;
                        }

                        if (!TryFromString(optChild->GetContent(), optCount)) {
                            AddError(node, TStringBuilder() << "Bad callable optional args count: " << node.GetChild(1)->GetContent());
                            return nullptr;
                        }
                    }

                    if (node.GetChild(1)->GetChildrenCount() > 1) {
                        auto payloadChild = node.GetChild(1)->GetChild(1);
                        if (!payloadChild->IsAtom()) {
                            AddError(node, "Bad callable annotation - expected atom");
                            return nullptr;
                        }

                        payload = payloadChild->GetContent();
                    }

                    auto retSettings = node.GetChild(2);
                    if (!retSettings->IsList() || retSettings->GetChildrenCount() != 1) {
                        AddError(node, "Bad callable annotation - expected list of size 1");
                        return nullptr;
                    }

                    auto retType = CompileTypeAnnotationNode(*retSettings->GetChild(0));
                    if (!retType)
                        return nullptr;

                    for (size_t index = 3; index < node.GetChildrenCount(); ++index) {
                        auto argSettings = node.GetChild(index);
                        if (!argSettings->IsList() || argSettings->GetChildrenCount() < 1 ||
                            argSettings->GetChildrenCount() > 3) {
                            AddError(node, "Bad callable annotation - expected list of size 1..3");
                            return nullptr;
                        }

                        auto r = CompileTypeAnnotationNode(*argSettings->GetChild(0));
                        if (!r)
                            return nullptr;

                        TCallableExprType::TArgumentInfo arg;
                        arg.Type = r;

                        if (argSettings->GetChildrenCount() > 1) {
                            auto nameChild = argSettings->GetChild(1);
                            if (!nameChild->IsAtom()) {
                                AddError(node, "Bad callable annotation - expected atom");
                                return nullptr;
                            }

                            arg.Name = nameChild->GetContent();
                        }

                        if (argSettings->GetChildrenCount() > 2) {
                            auto flagsChild = argSettings->GetChild(2);
                            if (!flagsChild->IsAtom()) {
                                AddError(node, "Bad callable annotation - expected atom");
                                return nullptr;
                            }

                            if (!TryFromString(flagsChild->GetContent(), arg.Flags)) {
                                AddError(node, "Bad callable annotation - bad integer");
                                return nullptr;
                            }
                        }

                        args.push_back(arg);
                    }

                    auto ann = Expr.MakeType<TCallableExprType>(retType, args, optCount, payload);
                    if (!ann->Validate(node.GetPosition(), Expr)) {
                        return nullptr;
                    }

                    return ann;
                } else if (content == TStringBuf("Resource")) {
                    if (node.GetChildrenCount() != 2 || !node.GetChild(1)->IsAtom()) {
                        AddError(node, "Bad resource type annotation");
                        return nullptr;
                    }

                    return Expr.MakeType<TResourceExprType>(TString(node.GetChild(1)->GetContent()));
                } else if (content == TStringBuf("Tagged")) {
                    if (node.GetChildrenCount() != 3 || !node.GetChild(2)->IsAtom()) {
                        AddError(node, "Bad tagged type annotation");
                        return nullptr;
                    }

                    auto type = CompileTypeAnnotationNode(*node.GetChild(1));
                    if (!type)
                        return nullptr;

                    TString tag(node.GetChild(2)->GetContent());
                    auto ann = Expr.MakeType<TTaggedExprType>(type, tag);
                    if (!ann->Validate(node.GetPosition(), Expr)) {
                        return nullptr;
                    }

                    return ann;
                } else if (content == TStringBuf("Error")) {
                    if (node.GetChildrenCount() != 5 || !node.GetChild(1)->IsAtom() ||
                        !node.GetChild(2)->IsAtom() || !node.GetChild(3)->IsAtom() || !node.GetChild(4)->IsAtom()) {
                        AddError(node, "Bad error type annotation");
                        return nullptr;
                    }

                    ui32 row;
                    if (!TryFromString(node.GetChild(1)->GetContent(), row)) {
                        AddError(node, TStringBuilder() << "Bad integer: " << node.GetChild(1)->GetContent());
                        return nullptr;
                    }

                    ui32 column;
                    if (!TryFromString(node.GetChild(2)->GetContent(), column)) {
                        AddError(node, TStringBuilder() << "Bad integer: " << node.GetChild(2)->GetContent());
                        return nullptr;
                    }

                    auto file = TString(node.GetChild(3)->GetContent());
                    return Expr.MakeType<TErrorExprType>(TIssue(TPosition(column, row, file), TString(node.GetChild(4)->GetContent())));
                } else if (content == TStringBuf("Variant")) {
                    if (node.GetChildrenCount() != 2) {
                        AddError(node, "Bad variant type annotation");
                        return nullptr;
                    }

                    auto r = CompileTypeAnnotationNode(*node.GetChild(1));
                    if (!r)
                        return nullptr;

                    auto ann = Expr.MakeType<TVariantExprType>(r);
                    if (!ann->Validate(node.GetPosition(), Expr)) {
                        return nullptr;
                    }

                    return ann;
                } else if (content == TStringBuf("Stream")) {
                    if (node.GetChildrenCount() != 2) {
                        AddError(node, "Bad stream type annotation");
                        return nullptr;
                    }

                    auto r = CompileTypeAnnotationNode(*node.GetChild(1));
                    if (!r)
                        return nullptr;

                    return Expr.MakeType<TStreamExprType>(r);
                } else if (content == TStringBuf("Flow")) {
                    if (node.GetChildrenCount() != 2) {
                        AddError(node, "Bad flow type annotation");
                        return nullptr;
                    }

                    auto r = CompileTypeAnnotationNode(*node.GetChild(1));
                    if (!r)
                        return nullptr;

                    return Expr.MakeType<TFlowExprType>(r);
                } else if (content == TStringBuf("Block")) {
                    if (node.GetChildrenCount() != 2) {
                        AddError(node, "Bad block type annotation");
                        return nullptr;
                    }

                    auto r = CompileTypeAnnotationNode(*node.GetChild(1));
                    if (!r)
                        return nullptr;

                    return Expr.MakeType<TBlockExprType>(r);
                } else if (content == TStringBuf("Scalar")) {
                    if (node.GetChildrenCount() != 2) {
                        AddError(node, "Bad scalar type annotation");
                        return nullptr;
                    }

                    auto r = CompileTypeAnnotationNode(*node.GetChild(1));
                    if (!r)
                        return nullptr;

                    return Expr.MakeType<TScalarExprType>(r);
                } else {
                    AddError(node, TStringBuilder() << "Unknown type annotation");
                    return nullptr;
                }
            }
        }
    };

    TAstNode* ConvertTypeAnnotationToAst(const TTypeAnnotationNode& annotation, TMemoryPool& pool, bool refAtoms) {
        switch (annotation.GetKind()) {
        case ETypeAnnotationKind::Unit:
            {
                return TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Unit"), pool);
            }

        case ETypeAnnotationKind::Tuple:
            {
                auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Tuple"), pool);
                TSmallVec<TAstNode*> children;
                children.push_back(self);
                for (auto& child : annotation.Cast<TTupleExprType>()->GetItems()) {
                    children.push_back(ConvertTypeAnnotationToAst(*child, pool, refAtoms));
                }

                return TAstNode::NewList(TPosition(), children.data(), children.size(), pool);
            }

        case ETypeAnnotationKind::Struct:
            {
                auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Struct"), pool);
                TSmallVec<TAstNode*> children;
                children.push_back(self);
                for (auto& child : annotation.Cast<TStructExprType>()->GetItems()) {
                    children.push_back(ConvertTypeAnnotationToAst(*child, pool, refAtoms));
                }

                return TAstNode::NewList(TPosition(), children.data(), children.size(), pool);
            }

        case ETypeAnnotationKind::List:
            {
                auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("List"), pool);
                auto itemType = ConvertTypeAnnotationToAst(*annotation.Cast<TListExprType>()->GetItemType(), pool, refAtoms);
                return TAstNode::NewList(TPosition(), pool, self, itemType);
            }

        case ETypeAnnotationKind::Optional:
        {
            auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Optional"), pool);
            auto itemType = ConvertTypeAnnotationToAst(*annotation.Cast<TOptionalExprType>()->GetItemType(), pool, refAtoms);
            return TAstNode::NewList(TPosition(), pool, self, itemType);
        }

        case ETypeAnnotationKind::Item:
            {
                auto casted = annotation.Cast<TItemExprType>();
                auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Item"), pool);
                auto name = refAtoms ?
                    TAstNode::NewLiteralAtom(TPosition(), casted->GetName(), pool, TNodeFlags::ArbitraryContent) :
                    TAstNode::NewAtom(TPosition(), casted->GetName(), pool, TNodeFlags::ArbitraryContent);
                auto itemType = ConvertTypeAnnotationToAst(*casted->GetItemType(), pool, refAtoms);
                return TAstNode::NewList(TPosition(), pool, self, name, itemType);
            }

        case ETypeAnnotationKind::Data:
            {
                auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Data"), pool);
                auto datatype = refAtoms ?
                    TAstNode::NewLiteralAtom(TPosition(), annotation.Cast<TDataExprType>()->GetName(), pool) :
                    TAstNode::NewAtom(TPosition(), annotation.Cast<TDataExprType>()->GetName(), pool);
                if (auto params = dynamic_cast<const TDataExprParamsType*>(&annotation)) {
                    auto param1 = refAtoms ?
                        TAstNode::NewLiteralAtom(TPosition(), params->GetParamOne(), pool) :
                        TAstNode::NewAtom(TPosition(), params->GetParamOne(), pool);

                    auto param2 = refAtoms ?
                        TAstNode::NewLiteralAtom(TPosition(), params->GetParamTwo(), pool) :
                        TAstNode::NewAtom(TPosition(), params->GetParamTwo(), pool);

                    return TAstNode::NewList(TPosition(), pool, self, datatype, param1, param2);
                }

                return TAstNode::NewList(TPosition(), pool, self, datatype);
            }

        case ETypeAnnotationKind::Pg:
        {
            auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Pg"), pool);
            auto name = TAstNode::NewLiteralAtom(TPosition(), annotation.Cast<TPgExprType>()->GetName(), pool);
            return TAstNode::NewList(TPosition(), pool, self, name);
        }

        case ETypeAnnotationKind::World:
        {
            return TAstNode::NewLiteralAtom(TPosition(), TStringBuf("World"), pool);
        }

        case ETypeAnnotationKind::Type:
        {
            auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Type"), pool);
            auto type = ConvertTypeAnnotationToAst(*annotation.Cast<TTypeExprType>()->GetType(), pool, refAtoms);
            return TAstNode::NewList(TPosition(), pool, self, type);
        }

        case ETypeAnnotationKind::Dict:
        {
            auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Dict"), pool);
            auto dictType = annotation.Cast<TDictExprType>();
            auto keyType = ConvertTypeAnnotationToAst(*dictType->GetKeyType(), pool, refAtoms);
            auto payloadType = ConvertTypeAnnotationToAst(*dictType->GetPayloadType(), pool, refAtoms);
            return TAstNode::NewList(TPosition(), pool, self, keyType, payloadType);
        }

        case ETypeAnnotationKind::Void:
        {
            return TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Void"), pool);
        }

        case ETypeAnnotationKind::Null:
        {
            return TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Null"), pool);
        }

        case ETypeAnnotationKind::Callable:
        {
            auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Callable"), pool);
            auto callable = annotation.Cast<TCallableExprType>();
            TSmallVec<TAstNode*> mainSettings;
            if (callable->GetOptionalArgumentsCount() > 0 || !callable->GetPayload().empty()) {
                auto optArgs = TAstNode::NewAtom(TPosition(), ToString(callable->GetOptionalArgumentsCount()), pool);

                mainSettings.push_back(optArgs);
            }

            if (!callable->GetPayload().empty()) {
                auto payload = TAstNode::NewAtom(TPosition(), callable->GetPayload(), pool, TNodeFlags::ArbitraryContent);
                mainSettings.push_back(payload);
            }

            TSmallVec<TAstNode*> children;
            children.push_back(self);

            children.push_back(TAstNode::NewList(TPosition(), mainSettings.data(), mainSettings.size(), pool));

            TSmallVec<TAstNode*> retSettings;
            retSettings.push_back(ConvertTypeAnnotationToAst(*callable->GetReturnType(), pool, refAtoms));
            children.push_back(TAstNode::NewList(TPosition(), retSettings.data(), retSettings.size(), pool));

            for (auto& arg : callable->GetArguments()) {
                TSmallVec<TAstNode*> argSettings;
                argSettings.push_back(ConvertTypeAnnotationToAst(*arg.Type, pool, refAtoms));
                if (!arg.Name.empty() || arg.Flags != 0) {
                    auto name = TAstNode::NewAtom(TPosition(), arg.Name, pool, TNodeFlags::ArbitraryContent);
                    argSettings.push_back(name);
                }

                if (arg.Flags != 0) {
                    auto flags = TAstNode::NewAtom(TPosition(), ToString(arg.Flags), pool, TNodeFlags::ArbitraryContent);
                    argSettings.push_back(flags);
                }

                children.push_back(TAstNode::NewList(TPosition(), argSettings.data(), argSettings.size(), pool));
            }

            return TAstNode::NewList(TPosition(), children.data(), children.size(), pool);
        }

        case ETypeAnnotationKind::Generic:
        {
            return TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Generic"), pool);
        }

        case ETypeAnnotationKind::Resource:
        {
            auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Resource"), pool);
            auto restype = refAtoms ?
                TAstNode::NewLiteralAtom(TPosition(), annotation.Cast<TResourceExprType>()->GetTag(), pool, TNodeFlags::ArbitraryContent) :
                TAstNode::NewAtom(TPosition(), annotation.Cast<TResourceExprType>()->GetTag(), pool, TNodeFlags::ArbitraryContent);
            return TAstNode::NewList(TPosition(), pool, self, restype);
        }

        case ETypeAnnotationKind::Tagged:
        {
            auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Tagged"), pool);
            auto type = ConvertTypeAnnotationToAst(*annotation.Cast<TTaggedExprType>()->GetBaseType(), pool, refAtoms);
            auto restype = refAtoms ?
                TAstNode::NewLiteralAtom(TPosition(), annotation.Cast<TTaggedExprType>()->GetTag(), pool, TNodeFlags::ArbitraryContent) :
                TAstNode::NewAtom(TPosition(), annotation.Cast<TTaggedExprType>()->GetTag(), pool, TNodeFlags::ArbitraryContent);
            return TAstNode::NewList(TPosition(), pool, self, type, restype);
        }

        case ETypeAnnotationKind::Error:
        {
            auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Error"), pool);
            const auto& err = annotation.Cast<TErrorExprType>()->GetError();
            auto row = TAstNode::NewAtom(TPosition(), ToString(err.Position.Row), pool);
            auto column = TAstNode::NewAtom(TPosition(), ToString(err.Position.Column), pool);
            auto file = refAtoms ?
                TAstNode::NewLiteralAtom(TPosition(), err.Position.File, pool, TNodeFlags::ArbitraryContent) :
                TAstNode::NewAtom(TPosition(), err.Position.File, pool, TNodeFlags::ArbitraryContent);
            auto message = refAtoms ?
                TAstNode::NewLiteralAtom(TPosition(), err.GetMessage(), pool, TNodeFlags::ArbitraryContent) :
                TAstNode::NewAtom(TPosition(), err.GetMessage(), pool, TNodeFlags::ArbitraryContent);
            return TAstNode::NewList(TPosition(), pool, self, row, column, file, message);
        }

        case ETypeAnnotationKind::Variant:
        {
            auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Variant"), pool);
            auto underlyingType = ConvertTypeAnnotationToAst(*annotation.Cast<TVariantExprType>()->GetUnderlyingType(), pool, refAtoms);
            return TAstNode::NewList(TPosition(), pool, self, underlyingType);
        }

        case ETypeAnnotationKind::Stream:
        {
            auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Stream"), pool);
            auto itemType = ConvertTypeAnnotationToAst(*annotation.Cast<TStreamExprType>()->GetItemType(), pool, refAtoms);
            return TAstNode::NewList(TPosition(), pool, self, itemType);
        }

        case ETypeAnnotationKind::Flow:
        {
            auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Flow"), pool);
            auto itemType = ConvertTypeAnnotationToAst(*annotation.Cast<TFlowExprType>()->GetItemType(), pool, refAtoms);
            return TAstNode::NewList(TPosition(), pool, self, itemType);
        }

        case ETypeAnnotationKind::Multi:
        {
            auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Multi"), pool);
            TSmallVec<TAstNode*> children;
            children.push_back(self);
            for (auto& child : annotation.Cast<TMultiExprType>()->GetItems()) {
                children.push_back(ConvertTypeAnnotationToAst(*child, pool, refAtoms));
            }

            return TAstNode::NewList(TPosition(), children.data(), children.size(), pool);
        }

        case ETypeAnnotationKind::EmptyList:
        {
            return TAstNode::NewLiteralAtom(TPosition(), TStringBuf("EmptyList"), pool);
        }
        case ETypeAnnotationKind::EmptyDict:
        {
            return TAstNode::NewLiteralAtom(TPosition(), TStringBuf("EmptyDict"), pool);
        }

        case ETypeAnnotationKind::Block:
        {
            auto type = annotation.Cast<TBlockExprType>();
            auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Block"), pool);
            auto itemType = ConvertTypeAnnotationToAst(*type->GetItemType(), pool, refAtoms);
            return TAstNode::NewList(TPosition(), pool, self, itemType);
        }

        case ETypeAnnotationKind::Scalar:
        {
            auto type = annotation.Cast<TScalarExprType>();
            auto self = TAstNode::NewLiteralAtom(TPosition(), TStringBuf("Scalar"), pool);
            auto itemType = ConvertTypeAnnotationToAst(*type->GetItemType(), pool, refAtoms);
            return TAstNode::NewList(TPosition(), pool, self, itemType);
        }

        case ETypeAnnotationKind::LastType:
            YQL_ENSURE(false, "Unknown kind: " << annotation.GetKind());

        }
    }

    TAstNode* AnnotateAstNode(TAstNode* node, const TExprNode* exprNode, ui32 flags, TMemoryPool& pool, bool refAtoms) {
        if (!flags)
            return node;

        TSmallVec<TAstNode*> children;
        if (flags & TExprAnnotationFlags::Position) {
            children.push_back(PositionAsNode(node->GetPosition(), pool));
        }

        if ((flags & TExprAnnotationFlags::Types)) {
            TAstNode* typeAnn = nullptr;
            if (exprNode) {
                YQL_ENSURE(exprNode->GetTypeAnn());
                typeAnn = ConvertTypeAnnotationToAst(*exprNode->GetTypeAnn(), pool, refAtoms);
            } else {
                typeAnn = TAstNode::NewLiteralAtom(node->GetPosition(), TStringBuf("."), pool);
            }

            children.push_back(typeAnn);
        }

        children.push_back(node);
        return TAstNode::NewList(node->GetPosition(), children.data(), children.size(), pool);
    }

    bool AddParameterDependencies(const TString& url, const TAstNode& node, TContext& ctx) {
        auto world = ctx.FindBinding("world");
        if (!world.empty()) {
            TSet<TString> names;
            SubstParameters(url, Nothing(), &names);
            for (const auto& name : names) {
                auto nameRef = ctx.FindBinding(name);
                if (nameRef.empty()) {
                    ctx.AddError(node, TStringBuilder() << "Name not found: " << name);
                    return false;
                }

                TExprNode::TListType args = world;
                args.insert(args.end(), nameRef.begin(), nameRef.end());
                auto newWorld = TExprNode::TListType{ ctx.Expr.NewCallable(node.GetPosition(), "Left!", {
                    ctx.Expr.NewCallable(node.GetPosition(), "Cons!", std::move(args)) })};

                ctx.Frames.back().Bindings["world"] = newWorld;
                world = newWorld;
            }
        }

        return true;
    }

    TExprNode::TListType Compile(const TAstNode& node, TContext& ctx);

    TExprNode::TPtr CompileQuote(const TAstNode& node, TContext& ctx) {
        if (node.IsAtom()) {
            return ctx.ProcessNode(node, ctx.Expr.NewAtom(node.GetPosition(), TString(node.GetContent()), node.GetFlags()));
        } else {
            TExprNode::TListType children;
            children.reserve(node.GetChildrenCount());
            for (ui32 index = 0; index < node.GetChildrenCount(); ++index) {
                auto r = Compile(*node.GetChild(index), ctx);
                if (r.empty())
                    return {};

                std::move(r.begin(), r.end(), std::back_inserter(children));
            }

            return ctx.ProcessNode(node, ctx.Expr.NewList(node.GetPosition(), std::move(children)));
        }
    }

    TExprNode::TListType CompileLambda(const TAstNode& node, TContext& ctx) {
        if (node.GetChildrenCount() < 2) {
            ctx.AddError(node, "Expected size of list at least 3.");
            return {};
        }

        const auto args = node.GetChild(1);
        if (!args->IsList() || args->GetChildrenCount() != 2 || !args->GetChild(0)->IsAtom() ||
            args->GetChild(0)->GetContent() != TStringBuf("quote") || !args->GetChild(1)->IsList()) {
            ctx.AddError(node, "Lambda arguments must be a quoted list of atoms");
            return {};
        }

        const auto params = args->GetChild(1);
        for (ui32 index = 0; index < params->GetChildrenCount(); ++index) {
            if (!params->GetChild(index)->IsAtom()) {
                ctx.AddError(node, "Lambda arguments must be a quoted list of atoms");
                return {};
            }
        }

        ctx.PushFrame();
        TExprNode::TListType argNodes;
        for (ui32 index = 0; index < params->GetChildrenCount(); ++index) {
            auto arg = params->GetChild(index);
            auto lambdaArg = ctx.ProcessNode(*arg, ctx.Expr.NewArgument(arg->GetPosition(), TString(arg->GetContent())));
            argNodes.push_back(lambdaArg);
            auto& binding = ctx.Frames.back().Bindings[arg->GetContent()];
            if (!binding.empty()) {
                ctx.PopFrame();
                ctx.AddError(*arg, TStringBuilder() << "Duplicated name of lambda parameter: " << arg->GetContent());
                return {};
            }

            binding = {lambdaArg};
        }

        TExprNode::TListType body;
        body.reserve(node.GetChildrenCount() - 2U);
        for (auto i = 2U; i < node.GetChildrenCount(); ++i) {
            auto r = Compile(*node.GetChild(i), ctx);
            if (r.empty())
                return {};
            std::move(r.begin(), r.end(), std::back_inserter(body));
        }
        ctx.PopFrame();

        auto arguments = ctx.ProcessNode(*args, ctx.Expr.NewArguments(args->GetPosition(), std::move(argNodes)));
        auto lambda = ctx.ProcessNode(node, ctx.Expr.NewLambda(node.GetPosition(), std::move(arguments), std::move(body)));
        return {lambda};
    }

    bool CompileSetPackageVersion(const TAstNode& node, TContext& ctx) {
        if (node.GetChildrenCount() != 3) {
            ctx.AddError(node, "Expected list of size 3");
            return false;
        }

        const auto name = node.GetChild(1);
        if (name->IsAtom() || name->GetChildrenCount() != 2 || !name->GetChild(0)->IsAtom() || !name->GetChild(1)->IsAtom() ||
            name->GetChild(0)->GetContent() != TStringBuf("quote")) {
            ctx.AddError(*name, "Expected quoted atom for package name");
            return false;
        }

        const auto versionNode = node.GetChild(2);
        if (versionNode->IsAtom() || versionNode->GetChildrenCount() != 2 || !versionNode->GetChild(0)->IsAtom() || !versionNode->GetChild(1)->IsAtom() ||
            versionNode->GetChild(0)->GetContent() != TStringBuf("quote")) {
            ctx.AddError(*versionNode, "Expected quoted atom for package version");
            return false;
        }

        ui32 version = 0;
        if (!versionNode->GetChild(1)->IsAtom() || !TryFromString(versionNode->GetChild(1)->GetContent(), version)) {
            ctx.AddError(*versionNode, TString("Expected package version as number, bad content ") + versionNode->GetChild(1)->GetContent());
            return false;
        }

        if (ctx.ModuleResolver && !ctx.ModuleResolver->SetPackageDefaultVersion(TString(name->GetChild(1)->GetContent()), version)) {
            ctx.AddError(*versionNode, TStringBuilder() << "Unable to specify version " << version << " for package " << name->GetChild(1)->GetContent());
            return false;
        }
        return true;
    }

    TExprNode::TPtr CompileBind(const TAstNode& node, TContext& ctx) {
        if (node.GetChildrenCount() != 3) {
            ctx.AddError(node, "Expected list of size 3");
            return nullptr;
        }

        const auto name = node.GetChild(1);
        if (!name->IsAtom()) {
            ctx.AddError(*name, "Expected atom");
            return nullptr;
        }

        const auto alias = node.GetChild(2);
        if (alias->IsAtom() || alias->GetChildrenCount() != 2 || !alias->GetChild(0)->IsAtom() || !alias->GetChild(1)->IsAtom() ||
            alias->GetChild(0)->GetContent() != TStringBuf("quote")) {
            ctx.AddError(*alias, "Expected quoted pair");
            return nullptr;
        }

        const auto& aliasValue = alias->GetChild(1)->GetContent();
        const auto& moduleName = name->GetContent();
        TStringBuilder baseMsg;
        baseMsg << "Module  '" << name->GetContent() << "'";

        const auto& import = ctx.FindImport(moduleName);
        if (import.empty()) {
            ctx.AddError(*name, baseMsg << " does not exist");
            return nullptr;
        }

        if (ctx.ModuleResolver) {
            auto exportsPtr = ctx.ModuleResolver->GetModule(import);
            if (!exportsPtr) {
                ctx.AddError(*name, baseMsg << "'" << import << "' does not exist");
                return nullptr;
            }

            const auto& exports = exportsPtr->Symbols();

            const auto ex = exports.find(aliasValue);
            if (exports.cend() == ex) {
                ctx.AddError(*alias, baseMsg << " export '" << aliasValue << "' does not exist");
                return nullptr;
            }

            return ctx.Expr.DeepCopy(*ex->second, exportsPtr->ExprCtx(), ctx.DeepClones, true, false);
        } else {
            const auto stub = ctx.Expr.NewAtom(node.GetPosition(), "stub");
            ctx.Frames.back().Bindings[name->GetContent()] = {stub};
            ctx.Cohesion.Imports[stub.Get()] = std::make_pair(import, TString(aliasValue));
            return stub;
        }
    }

    bool CompileLet(const TAstNode& node, TContext& ctx) {
        if (node.GetChildrenCount() < 3) {
            ctx.AddError(node, "Expected size of list at least 3.");
            return false;
        }

        const auto name = node.GetChild(1);
        if (!name->IsAtom()) {
            ctx.AddError(*name, "Expected atom");
            return false;
        }

        TExprNode::TListType bind;
        bind.reserve(node.GetChildrenCount() - 2U);
        for (auto i = 2U; i < node.GetChildrenCount(); ++i) {
            auto r = Compile(*node.GetChild(i), ctx);
            if (r.empty())
                return false;
            std::move(r.begin(), r.end(), std::back_inserter(bind));
        }

        ctx.Frames.back().Bindings[name->GetContent()] = std::move(bind);
        return true;
    }

    bool CompileImport(const TAstNode& node, TContext& ctx) {
        if (node.GetChildrenCount() != 3) {
            ctx.AddError(node, "Expected list of size 3");
            return false;
        }

        const auto name = node.GetChild(1);
        if (!name->IsAtom()) {
            ctx.AddError(*name, "Expected atom");
            return false;
        }

        const auto alias = node.GetChild(2);
        if (!alias->IsListOfSize(2) || !alias->GetChild(0)->IsAtom() || !alias->GetChild(1)->IsAtom() ||
            alias->GetChild(0)->GetContent() != TStringBuf("quote")) {
            ctx.AddError(node, "Expected quoted pair");
            return false;
        }

        ctx.Frames.back().Imports[name->GetContent()] = alias->GetChild(1)->GetContent();
        return true;
    }

    bool CompileExport(const TAstNode& node, TContext& ctx) {
        if (node.GetChildrenCount() != 2) {
            ctx.AddError(node, "Expected list of size 2");
            return false;
        }

        const auto name = node.GetChild(1);
        if (!name->IsAtom()) {
            ctx.AddError(*name, "Expected atom");
            return false;
        }

        auto r = Compile(*node.GetChild(1), ctx);
        if (r.size() != 1U)
            return false;

        ctx.Cohesion.Exports.Symbols(ctx.Expr)[name->GetContent()] = std::move(r.front());
        return true;
    }

    bool CompileDeclare(const TAstNode& node, TContext& ctx, bool checkOnly) {
        if (node.GetChildrenCount() != 3) {
            ctx.AddError(node, "Expected list of size 3");
            return false;
        }

        const auto name = node.GetChild(1);
        if (!name->IsAtom()) {
            ctx.AddError(*name, "Expected atom");
            return false;
        }

        TString nameStr = TString(name->GetContent());
        if (nameStr.size() < 2) {
            ctx.AddError(*name, "Parameter name should be at least 2 characters long.");
            return false;
        }

        if (nameStr[0] == '$' && std::isdigit(nameStr[1])) {
            ctx.AddError(*name, "Parameter name cannot start with digit.");
            return false;
        }

        auto typeExpr = Compile(*node.GetChild(2), ctx);
        if (typeExpr.size() != 1U)
            return false;

        auto typePos = node.GetChild(2)->GetPosition();
        auto parameterExpr = ctx.ProcessNode(node,
            ctx.Expr.NewCallable(typePos, "Parameter", {
                ctx.Expr.NewAtom(node.GetPosition(), nameStr),
                std::move(typeExpr.front())
            }));

        bool error = false;
        if (checkOnly) {
            auto it = ctx.Frames.back().Bindings.find(nameStr);
            if (it == ctx.Frames.back().Bindings.end()) {
                ctx.AddError(*name, TStringBuilder() << "Missing parameter: " << nameStr);
                return false;
            }

            if (it->second.size() != 1 || !it->second.front()->IsCallable("Parameter")) {
                error = true;
            }
        } else {
            if (!ctx.Frames.back().Bindings.emplace(nameStr, TExprNode::TListType{ std::move(parameterExpr) }).second) {
                error = true;
            }
        }

        if (error) {
            ctx.AddError(node, TStringBuilder() << "Declare statement hides previously defined name: " << nameStr);
            return false;
        }

        return true;
    }

    bool CompileLibraryDef(const TAstNode& node, TContext& ctx) {
        if (node.GetChildrenCount() < 2 || node.GetChildrenCount() > 4) {
            ctx.AddError(node, "Expected list of size from 2 to 4");
            return false;
        }

        const auto name = node.GetChild(1);
        if (!name->IsAtom()) {
            ctx.AddError(*name, "Expected atom");
            return false;
        }

        TString url;
        TString token;
        if (node.GetChildrenCount() > 2) {
            const auto file = node.GetChild(2);
            if (!file->IsAtom()) {
                ctx.AddError(*file, "Expected atom");
                return false;
            }

            url = file->GetContent();

            if (node.GetChildrenCount() > 3) {
                const auto tokenNode = node.GetChild(3);
                if (!tokenNode->IsAtom()) {
                    ctx.AddError(*tokenNode, "Expected atom");
                    return false;
                }

                token = tokenNode->GetContent();
            }
        }

        if (url && !AddParameterDependencies(url, node, ctx)) {
            return false;
        }

        if (!ctx.ModuleResolver) {
            return true;
        }

        if (url) {
            if (!ctx.ModuleResolver->AddFromUrl(name->GetContent(), url, token, ctx.Expr, ctx.SyntaxVersion, 0, name->GetPosition())) {
                return false;
            }
        } else {
            if (!ctx.ModuleResolver->AddFromFile(name->GetContent(), ctx.Expr, ctx.SyntaxVersion, 0, name->GetPosition())) {
                return false;
            }
        }

        return true;
    }

    bool CompilePackageDef(const TAstNode& node, TContext& ctx) {
        if (node.GetChildrenCount() < 2 || node.GetChildrenCount() > 4) {
            ctx.AddError(node, "Expected list of size from 2 to 4");
            return false;
        }

        auto nameNode = node.GetChild(1);
        if (!nameNode->IsAtom()) {
            ctx.AddError(*nameNode, "Expected atom");
            return false;
        }

        auto name = TString(nameNode->GetContent());

        TString url;
        if (node.GetChildrenCount() > 2) {
            const auto file = node.GetChild(2);
            if (!file->IsAtom()) {
                ctx.AddError(*file, "Expected atom");
                return false;
            }

            url = file->GetContent();
        }

        TString token;
        if (node.GetChildrenCount() > 3) {
            const auto tokenNode = node.GetChild(3);
            if (!tokenNode->IsAtom()) {
                ctx.AddError(*tokenNode, "Expected atom");
                return false;
            }

            token = tokenNode->GetContent();
        }

        if (url && !AddParameterDependencies(url, node, ctx)) {
            return false;
        }

        if (!ctx.ModuleResolver) {
            return true;
        }

        if (!ctx.UrlListerManager) {
            return true;
        }

        ctx.ModuleResolver->RegisterPackage(name);

        auto packageModuleName = TStringBuilder() << PkgPrefix;

        TStringBuf nameBuf(name);
        while (auto part = nameBuf.NextTok(Dot)) {
            packageModuleName << Sep << part;
        }

        auto queue = TVector<std::pair<TString, THttpURL>> {
            {packageModuleName, ParseURL(url)}
        };

        while (queue) {
            auto [prefix, httpUrl] = queue.back();
            queue.pop_back();

            TVector<TUrlListEntry> urlListEntries;
            try {
                urlListEntries = ctx.UrlListerManager->ListUrl(httpUrl, token);
            } catch (const std::exception& e) {
                ctx.AddError(*nameNode,
                    TStringBuilder()
                        << "UrlListerManager: failed to list URL \"" << httpUrl.PrintS()
                        << "\", details: " << e.what()
                );

                return false;
            }

            for (auto& urlListEntry: urlListEntries) {
                switch (urlListEntry.Type) {
                case EUrlListEntryType::FILE: {
                    auto moduleName = TStringBuilder()
                        << prefix << Sep << urlListEntry.Name;

                    if (ctx.OverrideLibraries.contains(moduleName)) {
                        continue;
                    }

                    if (!ctx.ModuleResolver->AddFromUrl(
                        moduleName, urlListEntry.Url.PrintS(), token, ctx.Expr,
                        ctx.SyntaxVersion, 0, nameNode->GetPosition()
                    )) {
                        return false;
                    }

                    break;
                }

                case EUrlListEntryType::DIRECTORY: {
                    queue.push_back({
                        TStringBuilder() << prefix << Sep << urlListEntry.Name,
                        urlListEntry.Url
                    });

                    break;
                }
                }
            }
        }

        return true;
    }

    bool CompileOverrideLibraryDef(const TAstNode& node, TContext& ctx) {
        if (node.GetChildrenCount() != 2) {
            ctx.AddError(node, "Expected list of size 2");
            return false;
        }

        auto nameNode = node.GetChild(1);
        if (!nameNode->IsAtom()) {
            ctx.AddError(*nameNode, "Expected atom");
            return false;
        }

        if (!ctx.ModuleResolver) {
            return true;
        }

        auto overrideLibraryName = TStringBuilder()
            << PkgPrefix << Sep << nameNode->GetContent();

        if (!ctx.ModuleResolver->AddFromFile(
            overrideLibraryName, ctx.Expr, ctx.SyntaxVersion, 0, nameNode->GetPosition()
        )) {
            return false;
        }

        ctx.OverrideLibraries.insert(std::move(overrideLibraryName));

        return true;
    }

    bool CompileReturn(const TAstNode& node, TContext& ctx) {
        if (node.GetChildrenCount() < 2U) {
            ctx.AddError(node, "Expected non empty list.");
            return false;
        }

        TExprNode::TListType returns;
        returns.reserve(node.GetChildrenCount() - 1U);
        for (auto i = 1U; i < node.GetChildrenCount(); ++i) {
            auto r = Compile(*node.GetChild(i), ctx);
            if (r.empty())
                return false;
            std::move(r.begin(), r.end(), std::back_inserter(returns));
        }

        ctx.Frames.back().Return = std::move(returns);
        return true;
    }

    TExprNode::TListType CompileFunction(const TAstNode& root, TContext& ctx, bool topLevel = false) {
        if (!root.IsList()) {
            ctx.AddError(root, "Expected list");
            return {};
        }

        if (ctx.Frames.size() > 1000U) {
            ctx.AddError(root, "Too deep graph!");
            return {};
        }

        ctx.PushFrame();
        if (topLevel) {
            for (ui32 index = 0; index < root.GetChildrenCount(); ++index) {
                const auto node = root.GetChild(index);
                if (!node->IsList()) {
                    ctx.AddError(*node, "Expected list");
                    return {};
                }

                if (node->GetChildrenCount() == 0) {
                    ctx.AddError(*node, "Expected not empty list");
                    return {};
                }

                const auto firstChild = node->GetChild(0);
                if (!firstChild->IsAtom()) {
                    ctx.AddError(*firstChild, "Expected atom");
                    return {};
                }

                if (firstChild->GetContent() == TStringBuf("library")) {
                    if (!CompileLibraryDef(*node, ctx))
                        return {};
                } else if (firstChild->GetContent() == TStringBuf("set_package_version")) {
                    if (!CompileSetPackageVersion(*node, ctx))
                        return {};
                } else if (firstChild->GetContent() == TStringBuf("declare")) {
                    if (!CompileDeclare(*node, ctx, false))
                        return {};
                } else if (firstChild->GetContent() == TStringBuf("package")) {
                    if (!CompilePackageDef(*node, ctx)) {
                        return {};
                    }
                } else if (firstChild->GetContent() == TStringBuf("override_library")) {
                    if (!CompileOverrideLibraryDef(*node, ctx)) {
                        return {};
                    }
                }
            }

            if (ctx.ModuleResolver) {
                if (!ctx.ModuleResolver->Link(ctx.Expr)) {
                    return {};
                }

                ctx.ModuleResolver->UpdateNextUniqueId(ctx.Expr);
            }
        }

        for (ui32 index = 0; index < root.GetChildrenCount(); ++index) {
            const auto node = root.GetChild(index);
            if (!ctx.Frames.back().Return.empty()) {
                ctx.Frames.back().Return.clear();
                ctx.AddError(*node, "Return is already exist");
                return {};
            }

            if (!node->IsList()) {
                ctx.AddError(*node, "Expected list");
                return {};
            }

            if (node->GetChildrenCount() == 0) {
                ctx.AddError(*node, "Expected not empty list");
                return {};
            }

            auto firstChild = node->GetChild(0);
            if (!firstChild->IsAtom()) {
                ctx.AddError(*firstChild, "Expected atom");
                return {};
            }

            if (firstChild->GetContent() == TStringBuf("let")) {
                if (!CompileLet(*node, ctx))
                    return {};
            } else if (firstChild->GetContent() == TStringBuf("return")) {
                if (!CompileReturn(*node, ctx))
                    return {};
            } else if (firstChild->GetContent() == TStringBuf("import")) {
                if (!CompileImport(*node, ctx))
                    return {};
            } else if (firstChild->GetContent() == TStringBuf("declare")) {
                if (!topLevel) {
                    ctx.AddError(*firstChild, "Declare statements are only allowed on top level block");
                    return {};
                }

                if (!CompileDeclare(*node, ctx, true))
                    return {};

                continue;
            } else if (firstChild->GetContent() == TStringBuf("library")) {
                if (!topLevel) {
                    ctx.AddError(*firstChild, "Library statements are only allowed on top level block");
                    return {};
                }

                continue;
            } else if (firstChild->GetContent() == TStringBuf("set_package_version")) {
                if (!topLevel) {
                    ctx.AddError(*firstChild, "set_package_version statements are only allowed on top level block");
                    return {};
                }

                continue;
            } else if (firstChild->GetContent() == TStringBuf("package")) {
                if (!topLevel) {
                    ctx.AddError(*firstChild, "Package statements are only allowed on top level block");
                    return {};
                }
            } else if (firstChild->GetContent() == TStringBuf("override_library")) {
                if (!topLevel) {
                    ctx.AddError(*firstChild, "override_library statements are only allowed on top level block");
                    return {};
                }
            } else {
                ctx.AddError(*firstChild, ToString("expected either let, return or import, but have ") + firstChild->GetContent());
                return {};
            }
        }

        auto ret = std::move(ctx.Frames.back().Return);
        ctx.PopFrame();
        if (ret.empty()) {
            ctx.AddError(root, "No return found");
        }

        return ret;
    }

    bool CompileLibrary(const TAstNode& root, TContext& ctx) {
        if (!root.IsList()) {
            ctx.AddError(root, "Expected list");
            return false;
        }

        ctx.PushFrame();
        for (ui32 index = 0; index < root.GetChildrenCount(); ++index) {
            const auto node = root.GetChild(index);

            if (!node->IsList()) {
                ctx.AddError(*node, "Expected list");
                return false;
            }

            if (node->GetChildrenCount() == 0) {
                ctx.AddError(*node, "Expected not empty list");
                return false;
            }

            auto firstChild = node->GetChild(0);
            if (!firstChild->IsAtom()) {
                ctx.AddError(*firstChild, "Expected atom");
                return false;
            }

            if (firstChild->GetContent() == TStringBuf("let")) {
                if (!CompileLet(*node, ctx))
                    return false;
            } else if (firstChild->GetContent() == TStringBuf("import")) {
                if (!CompileImport(*node, ctx))
                    return false;
            } else if (firstChild->GetContent() == TStringBuf("export")) {
                if (!CompileExport(*node, ctx))
                    return false;
            } else {
                ctx.AddError(*firstChild, "expected either let, export or import");
                return false;
            }
        }

        ctx.PopFrame();
        return true;
    }

    TExprNode::TListType Compile(const TAstNode& node, TContext& ctx) {
        if (node.IsAtom()) {
            const auto foundNode = ctx.FindBinding(node.GetContent());
            if (foundNode.empty()) {
                ctx.AddError(node, TStringBuilder() << "Name not found: " << node.GetContent());
                return {};
            }

            return foundNode;
        }

        if (node.GetChildrenCount() == 0) {
            ctx.AddError(node, "Empty list, did you forget quote?");
            return {};
        }

        if (!node.GetChild(0)->IsAtom()) {
            ctx.AddError(node, "First item in list is not an atom, did you forget quote?");
            return {};
        }

        auto function = node.GetChild(0)->GetContent();
        if (function == TStringBuf("quote")) {
            if (node.GetChildrenCount() != 2) {
                ctx.AddError(node, "Quote should have one argument");
                return {};
            }

            if (auto quote = CompileQuote(*node.GetChild(1), ctx))
                return {std::move(quote)};

            return {};
        }

        if (function == TStringBuf("let") || function == TStringBuf("return")) {
            ctx.AddError(node, "Let and return should be used only at first level or inside def");
            return {};
        }

        if (function == TStringBuf("lambda")) {
            return CompileLambda(node, ctx);
        }

        if (function == TStringBuf("bind")) {
            if (auto bind = CompileBind(node, ctx))
                return {std::move(bind)};
            return {};
        }

        if (function == TStringBuf("block")) {
            if (node.GetChildrenCount() != 2) {
                ctx.AddError(node, "Block should have one argument");
                return {};
            }

            const auto quotedList = node.GetChild(1);
            if (quotedList->GetChildrenCount() != 2 || !quotedList->GetChild(0)->IsAtom() ||
                quotedList->GetChild(0)->GetContent() != TStringBuf("quote")) {
                ctx.AddError(node, "Expected quoted list");
                return {};
            }

            return CompileFunction(*quotedList->GetChild(1), ctx);
        }

        TExprNode::TListType children;
        children.reserve(node.GetChildrenCount() - 1U);
        for (auto index = 1U; index < node.GetChildrenCount(); ++index) {
            auto r = Compile(*node.GetChild(index), ctx);
            if (r.empty())
                return {};

            std::move(r.begin(), r.end(), std::back_inserter(children));
        }

        return {ctx.ProcessNode(node, ctx.Expr.NewCallable(node.GetPosition(), TString(function), std::move(children)))};
    }

    struct TFrameContext {
        size_t Index = 0;
        size_t Parent = 0;
        std::map<size_t, const TExprNode*> Nodes;
        std::vector<const TExprNode*> TopoSortedNodes;
        TNodeMap<TString> Bindings;
    };

    struct TVisitNodeContext {
        explicit  TVisitNodeContext(TExprContext& expr)
          : Expr(expr)
        {}

        TExprContext& Expr;
        size_t Order = 0ULL;
        bool RefAtoms = false;
        bool AllowFreeArgs = false;
        bool NormalizeAtomFlags = false;
        TNodeMap<size_t> FreeArgs;
        std::unique_ptr<TMemoryPool> Pool;
        std::vector<TFrameContext> Frames;
        TFrameContext* CurrentFrame = nullptr;
        TNodeMap<size_t> LambdaFrames;
        std::map<TStringBuf, std::pair<const TExprNode*, TAstNode*>> Parameters;

        struct TCounters {
            size_t References = 0ULL, Neighbors = 0ULL, Order = 0ULL, Frame = 0ULL;
        };

        TNodeMap<TCounters> References;

        const TString& FindBinding(const TExprNode* node) const {
            for (const auto* frame = CurrentFrame; frame; frame = frame->Index > 0 ? &Frames[frame->Parent] : nullptr) {
                const auto it = frame->Bindings.find(node);
                if (frame->Bindings.cend() != it)
                    return it->second;
            }

            static const TString stub;
            return stub;
        }

        size_t FindCommonAncestor(size_t one, size_t two) const {
            while (one && two) {
                if (one == two)
                    return one;
                if (one > two)
                    one = Frames[one].Parent;
                else
                    two = Frames[two].Parent;
            }

            return 0ULL;
        }
    };

    void VisitArguments(const TExprNode& node, TVisitNodeContext& ctx) {
        YQL_ENSURE(node.Type() == TExprNode::Arguments);
        for (const auto& arg : node.Children()) {
            auto& counts = ctx.References[arg.Get()];
            ++counts.References;
            YQL_ENSURE(ctx.CurrentFrame->Nodes.emplace(counts.Order = ++ctx.Order, arg.Get()).second);
        }
    }

    void RevisitNode(const TExprNode& node, TVisitNodeContext& ctx);

    void RevisitNode(TVisitNodeContext::TCounters& counts, const TExprNode& node, TVisitNodeContext& ctx) {
        const auto nf = ctx.FindCommonAncestor(ctx.CurrentFrame->Index, counts.Frame);
        if (counts.Frame != nf) {
            auto& frame = ctx.Frames[counts.Frame = nf];
            frame.Nodes.emplace(counts.Order, &node);
            if (TExprNode::Lambda == node.Type()) {
                ctx.Frames[ctx.LambdaFrames[&node]].Parent = counts.Frame;
            } else {
                node.ForEachChild([&ctx](const TExprNode& child) {
                    RevisitNode(child, ctx);
                });
            }
        }
    }

    void RevisitNode(const TExprNode& node, TVisitNodeContext& ctx) {
        if (TExprNode::Argument != node.Type()) {
            RevisitNode(ctx.References[&node], node, ctx);
        }
    }

    void VisitNode(const TExprNode& node, size_t neighbors, TVisitNodeContext& ctx) {
         if (TExprNode::Argument == node.Type())
             return;

        auto& counts = ctx.References[&node];
        counts.Neighbors += neighbors;
        if (counts.References++) {
            RevisitNode(counts, node, ctx);
        } else {
            counts.Frame = ctx.CurrentFrame->Index;

            if (node.Type() == TExprNode::Lambda) {
                YQL_ENSURE(node.ChildrenSize() > 0U);
                const auto index = ctx.Frames.size();
                if (ctx.LambdaFrames.emplace(&node, index).second) {
                    const auto prevFrameIndex = ctx.CurrentFrame - &ctx.Frames.front();
                    const auto parentIndex = ctx.CurrentFrame->Index;
                    ctx.Frames.emplace_back();
                    ctx.CurrentFrame = &ctx.Frames.back();
                    ctx.CurrentFrame->Index = index;
                    ctx.CurrentFrame->Parent = parentIndex;
                    VisitArguments(node.Head(), ctx);
                    for(ui32 i = 1U; i < node.ChildrenSize(); ++i) {
                        VisitNode(*node.Child(i), node.ChildrenSize() - 1U, ctx);
                    }
                    ctx.CurrentFrame = &ctx.Frames.front() + prevFrameIndex;
                }
            } else {
                node.ForEachChild([&](const TExprNode& child) {
                    VisitNode(child, node.ChildrenSize(), ctx);
                });
            }

            if (!counts.Order)
                counts.Order = ++ctx.Order;

            ctx.CurrentFrame->Nodes.emplace(counts.Order, &node);
        }
    }

    using TRoots = TSmallVec<const TExprNode*>;

    TAstNode* ConvertFunction(TPositionHandle position, const TRoots& roots, TVisitNodeContext& ctx, ui32 annotationFlags, TMemoryPool& pool);

    TAstNode* BuildValueNode(const TExprNode& node, TVisitNodeContext& ctx, const TString& topLevelName, ui32 annotationFlags, TMemoryPool& pool, bool useBindings) {
        TAstNode* res = nullptr;
        const auto& name = ctx.FindBinding(&node);
        if (!name.empty() && name != topLevelName && useBindings) {
            res = TAstNode::NewAtom(ctx.Expr.GetPosition(node.Pos()), name, pool);
        } else {
            switch (node.Type()) {
            case TExprNode::Atom:
                {
                    auto quote = AnnotateAstNode(&TAstNode::QuoteAtom, nullptr, annotationFlags, pool, ctx.RefAtoms);
                    auto flags = ctx.NormalizeAtomFlags ? TNodeFlags::ArbitraryContent : node.Flags();
                    auto content = AnnotateAstNode(
                        ctx.RefAtoms ?
                            TAstNode::NewLiteralAtom(ctx.Expr.GetPosition(node.Pos()), node.Content(), pool, flags) :
                            TAstNode::NewAtom(ctx.Expr.GetPosition(node.Pos()), node.Content(), pool, flags),
                        &node, annotationFlags, pool, ctx.RefAtoms);

                    res = TAstNode::NewList(ctx.Expr.GetPosition(node.Pos()), pool, quote, content);
                    break;
                }

            case TExprNode::List:
                {
                    TSmallVec<TAstNode*> values;
                    for (const auto& child : node.Children()) {
                        values.push_back(BuildValueNode(*child, ctx, topLevelName, annotationFlags, pool, useBindings));
                    }

                    auto quote = AnnotateAstNode(&TAstNode::QuoteAtom, nullptr, annotationFlags, pool, ctx.RefAtoms);
                    auto list = AnnotateAstNode(TAstNode::NewList(
                        ctx.Expr.GetPosition(node.Pos()), values.data(), values.size(), pool), &node, annotationFlags, pool, ctx.RefAtoms);

                    res = TAstNode::NewList(ctx.Expr.GetPosition(node.Pos()), pool, quote, list);
                    break;
                }

            case TExprNode::Callable:
                {
                    if (node.Content() == "Parameter") {
                        const auto& nameNode = *node.Child(0);
                        const auto& typeNode = *node.Child(1);
                        Y_UNUSED(typeNode);

                        res = TAstNode::NewAtom(ctx.Expr.GetPosition(node.Pos()), nameNode.Content(), pool);

                        auto it = ctx.Parameters.find(nameNode.Content());
                        if (it != ctx.Parameters.end()) {
                            break;
                        }

                        auto declareAtom = TAstNode::NewLiteralAtom(ctx.Expr.GetPosition(node.Pos()), TStringBuf("declare"), pool);
                        auto nameAtom = ctx.RefAtoms
                            ? TAstNode::NewLiteralAtom(ctx.Expr.GetPosition(nameNode.Pos()), nameNode.Content(), pool)
                            : TAstNode::NewAtom(ctx.Expr.GetPosition(nameNode.Pos()), nameNode.Content(), pool);

                        TSmallVec<TAstNode*> children;
                        children.push_back(AnnotateAstNode(declareAtom, nullptr, annotationFlags, pool, ctx.RefAtoms));
                        children.push_back(AnnotateAstNode(nameAtom, nullptr, annotationFlags, pool, ctx.RefAtoms));
                        children.push_back(BuildValueNode(typeNode, ctx, topLevelName, annotationFlags, pool, false));
                        auto declareNode = TAstNode::NewList(ctx.Expr.GetPosition(node.Pos()), children.data(), children.size(), pool);
                        declareNode = AnnotateAstNode(declareNode, &node, annotationFlags, pool, ctx.RefAtoms);

                        ctx.Parameters.insert(std::make_pair(nameNode.Content(),
                            std::make_pair(&typeNode, declareNode)));
                        break;
                    }

                    TSmallVec<TAstNode*> children;
                    children.push_back(AnnotateAstNode(
                        ctx.RefAtoms ?
                            TAstNode::NewLiteralAtom(ctx.Expr.GetPosition(node.Pos()), node.Content(), pool) :
                            TAstNode::NewAtom(ctx.Expr.GetPosition(node.Pos()), node.Content(), pool),
                            nullptr, annotationFlags, pool, ctx.RefAtoms));
                    for (const auto& child : node.Children()) {
                        children.push_back(BuildValueNode(*child, ctx, topLevelName, annotationFlags, pool, useBindings));
                    }

                    res = TAstNode::NewList(ctx.Expr.GetPosition(node.Pos()), children.data(), children.size(), pool);
                    break;
                }

            case TExprNode::Lambda:
                {
                    const auto prevFrame = ctx.CurrentFrame;
                    ctx.CurrentFrame = &ctx.Frames[ctx.LambdaFrames.find(&node)->second];
                    YQL_ENSURE(node.ChildrenSize() > 0U);
                    const auto& args = node.Head();
                    TSmallVec<TAstNode*> argsChildren;
                    for (const auto& arg : args.Children()) {
                        const auto& name = ctx.FindBinding(arg.Get());
                        const auto atom = TAstNode::NewAtom(ctx.Expr.GetPosition(node.Pos()), name, pool);
                        argsChildren.emplace_back(AnnotateAstNode(atom, arg.Get(), annotationFlags, pool, ctx.RefAtoms));
                    }

                    auto argsNode = TAstNode::NewList(ctx.Expr.GetPosition(args.Pos()), argsChildren.data(), argsChildren.size(), pool);
                    auto argsContainer = TAstNode::NewList(ctx.Expr.GetPosition(args.Pos()), pool,
                        AnnotateAstNode(&TAstNode::QuoteAtom, nullptr, annotationFlags, pool, ctx.RefAtoms),
                        AnnotateAstNode(argsNode, nullptr, annotationFlags, pool, ctx.RefAtoms));

                    const bool block = ctx.CurrentFrame->Bindings.cend() != std::find_if(ctx.CurrentFrame->Bindings.cbegin(), ctx.CurrentFrame->Bindings.cend(),
                        [](const auto& bind) { return bind.first->Type() != TExprNode::Argument; }
                    );

                    if (block) {
                        TSmallVec<const TExprNode*> body(node.ChildrenSize() - 1U);
                        for (ui32 i = 0U; i < body.size(); ++i)
                            body[i] = node.Child(i + 1U);
                        const auto blockNode = TAstNode::NewLiteralAtom(ctx.Expr.GetPosition(node.Pos()), TStringBuf("block"), pool);
                        const auto quotedListNode = TAstNode::NewList(ctx.Expr.GetPosition(node.Pos()), pool,
                            AnnotateAstNode(&TAstNode::QuoteAtom, nullptr, annotationFlags, pool, ctx.RefAtoms),
                            ConvertFunction(node.Pos(), body, ctx, annotationFlags, pool));

                        const auto blockBody = TAstNode::NewList(ctx.Expr.GetPosition(node.Pos()), pool,
                            AnnotateAstNode(blockNode, nullptr, annotationFlags, pool, ctx.RefAtoms),
                            AnnotateAstNode(quotedListNode, nullptr, annotationFlags, pool, ctx.RefAtoms));
                        res = AnnotateAstNode(blockBody, nullptr, annotationFlags, pool, ctx.RefAtoms);

                        ctx.CurrentFrame = prevFrame;
                        res = TAstNode::NewList(ctx.Expr.GetPosition(node.Pos()), pool,
                            AnnotateAstNode(TAstNode::NewLiteralAtom(
                                ctx.Expr.GetPosition(node.Pos()), TStringBuf("lambda"), pool), nullptr, annotationFlags, pool, ctx.RefAtoms),
                            AnnotateAstNode(argsContainer, &args, annotationFlags, pool, ctx.RefAtoms),
                            res);
                    } else {
                        TSmallVec<TAstNode*> children(node.ChildrenSize() + 1U);
                        for (ui32 i = 1U; i < node.ChildrenSize(); ++i) {
                            children[i + 1U] = BuildValueNode(*node.Child(i), ctx, topLevelName, annotationFlags, pool, useBindings);
                        }

                        ctx.CurrentFrame = prevFrame;
                        children[0] = AnnotateAstNode(TAstNode::NewLiteralAtom(
                                ctx.Expr.GetPosition(node.Pos()), TStringBuf("lambda"), pool), nullptr, annotationFlags, pool, ctx.RefAtoms);
                        children[1] = AnnotateAstNode(argsContainer, &args, annotationFlags, pool, ctx.RefAtoms);
                        res = TAstNode::NewList(ctx.Expr.GetPosition(node.Pos()), children.data(), children.size(), pool);
                    }
                    break;
                }

            case TExprNode::World:
                res = TAstNode::NewLiteralAtom(ctx.Expr.GetPosition(node.Pos()), TStringBuf("world"), pool);
                break;
            case TExprNode::Argument: {
                YQL_ENSURE(ctx.AllowFreeArgs, "Free arguments are not allowed");
                auto iter = ctx.FreeArgs.emplace(&node, ctx.FreeArgs.size());
                res = TAstNode::NewLiteralAtom(ctx.Expr.GetPosition(node.Pos()), ctx.Expr.AppendString("_FreeArg" + ToString(iter.first->second)), pool);
                break;
            }
            default:
                YQL_ENSURE(false, "Unknown type: " << static_cast<ui32>(node.Type()));
            }
        }

        return AnnotateAstNode(res, &node, annotationFlags, pool, ctx.RefAtoms);
    }

    TAstNode* ConvertFunction(TPositionHandle position, const TRoots& roots, TVisitNodeContext& ctx, ui32 annotationFlags, TMemoryPool& pool) {
        YQL_ENSURE(!roots.empty(), "Missed roots.");
        TSmallVec<TAstNode*> children;
        for (const auto& node : ctx.CurrentFrame->TopoSortedNodes) {
            const auto& name = ctx.FindBinding(node);
            if (name.empty() || node->Type() == TExprNode::Arguments || node->Type() == TExprNode::Argument) {
                continue;
            }

            const auto letAtom = TAstNode::NewLiteralAtom(ctx.Expr.GetPosition(node->Pos()), TStringBuf("let"), pool);
            const auto nameAtom = TAstNode::NewAtom(ctx.Expr.GetPosition(node->Pos()), name, pool);
            const auto valueNode = BuildValueNode(*node, ctx, name, annotationFlags, pool, true);

            const auto letNode = TAstNode::NewList(ctx.Expr.GetPosition(node->Pos()), pool,
                AnnotateAstNode(letAtom, nullptr, annotationFlags, pool, ctx.RefAtoms),
                AnnotateAstNode(nameAtom, nullptr, annotationFlags, pool, ctx.RefAtoms),
                valueNode);
            children.push_back(AnnotateAstNode(letNode, nullptr, annotationFlags, pool, ctx.RefAtoms));
        }

        const auto returnAtom = TAstNode::NewLiteralAtom(ctx.Expr.GetPosition(position), TStringBuf("return"), pool);
        TSmallVec<TAstNode*> returnChildren;
        returnChildren.reserve(roots.size() + 1U);
        returnChildren.emplace_back(AnnotateAstNode(returnAtom, nullptr, annotationFlags, pool, ctx.RefAtoms));
        for (const auto root : roots) {
            returnChildren.emplace_back(BuildValueNode(*root, ctx, TString(), annotationFlags, pool, true));
        }
        const auto returnList = TAstNode::NewList(ctx.Expr.GetPosition(position), returnChildren.data(), returnChildren.size(), pool);
        children.emplace_back(AnnotateAstNode(returnList, 1U == roots.size() ? roots.front() : nullptr, annotationFlags, pool, ctx.RefAtoms));

        if (!ctx.CurrentFrame->Index && !ctx.Parameters.empty()) {
            TSmallVec<TAstNode*> parameterNodes;
            parameterNodes.reserve(ctx.Parameters.size());

            for (auto& pair : ctx.Parameters) {
                parameterNodes.push_back(pair.second.second);
            }

            children.insert(children.begin(), parameterNodes.begin(), parameterNodes.end());
        }

        const auto res = TAstNode::NewList(ctx.Expr.GetPosition(position), children.data(), children.size(), pool);
        return AnnotateAstNode(res, nullptr, annotationFlags, pool, ctx.RefAtoms);
    }

    bool InlineNode(const TExprNode& node, size_t references, size_t neighbors, const TConvertToAstSettings& settings) {
        if (settings.NoInlineFunc) {
            if (settings.NoInlineFunc(node)) {
                return false;
            }
        }

        switch (node.Type()) {
        case TExprNode::Argument:
            return false;
        case TExprNode::Atom:
            if (const auto flags = node.Flags()) {
                if ((TNodeFlags::BinaryContent | TNodeFlags::MultilineContent) & flags)
                    return false;
                else {
                    if (TNodeFlags::ArbitraryContent & flags)
                        return node.Content().length() <= (references == 1U ? 0x40U : 0x10U);
                    else
                        return true;
                }
            } else
                return true;
        default:
            if (neighbors < 2U)
                return true;
            if (const auto children = node.ChildrenSize())
                return references == 1U && children < 3U;
            else
                return true;
        }
    }

    typedef std::pair<const TExprNode*, const TExprNode*> TPairOfNodePotinters;
    typedef std::unordered_set<TPairOfNodePotinters, THash<TPairOfNodePotinters>> TNodesPairSet;
    typedef TNodeMap<std::pair<ui32, ui32>> TArgumentsMap;

    bool CompareExpressions(const TExprNode*& one, const TExprNode*& two, TArgumentsMap& argumentsMap, ui32 level, TNodesPairSet& visited) {
        const auto ins = visited.emplace(one, two);
        if (!ins.second) {
            return true;
        }

        if (one->Type() != two->Type())
            return false;

        if (one->ChildrenSize() != two->ChildrenSize())
            return false;

        switch (two->Type()) {
        case TExprNode::Arguments: {
            ui32 i1 = 0U, i2 = 0U;
            one->ForEachChild([&](const TExprNode& arg){ argumentsMap.emplace(&arg, std::make_pair(level, ++i1)); });
            two->ForEachChild([&](const TExprNode& arg){ argumentsMap.emplace(&arg, std::make_pair(level, ++i2)); });
            return true;
        }
        case TExprNode::Argument:
            if (const auto oneArg = argumentsMap.find(one), twoArg = argumentsMap.find(two); oneArg == twoArg)
                return argumentsMap.cend() != oneArg || one == two;
            else if (argumentsMap.cend() != oneArg && argumentsMap.cend() != twoArg) {
                return oneArg->second == twoArg->second;
            }
            return false;
        case TExprNode::Atom:
            if (one->GetFlagsToCompare() != two->GetFlagsToCompare())
                return false;
            [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
        case TExprNode::Callable:
            if (one->Content() != two->Content())
                return false;
            [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
        default:
            break;
        case TExprNode::Lambda:
            ++level;
        }

        if (const auto childs = one->ChildrenSize()) {
            const auto& l = one->Children();
            const auto& r = two->Children();
            for (ui32 i = 0U; i < childs; ++i) {
                if (!CompareExpressions(one = l[i].Get(), two = r[i].Get(), argumentsMap, level, visited)) {
                    return false;
                }
            }
        }

        return true;
    }

    using TNodeSetPtr = std::shared_ptr<TNodeSet>;

    TNodeSetPtr ExcludeFromUnresolved(const TExprNode& args, const TNodeSetPtr& unresolved) {
        if (!unresolved || unresolved->empty() || args.ChildrenSize() == 0) {
            return unresolved;
        }

        size_t excluded = 0;
        auto newUnresolved = std::make_shared<TNodeSet>(*unresolved);
        for (auto& toExclude : args.Children()) {
            excluded += newUnresolved->erase(toExclude.Get());
        }

        return excluded ? newUnresolved : unresolved;
    }

    TNodeSetPtr MergeUnresolvedArgs(const TNodeSetPtr& one, const TNodeSetPtr& two) {
        if (!one || one->empty()) {
            return two;
        }

        if (!two || two->empty()) {
            return one;
        }

        const TNodeSetPtr& bigger  = (one->size() > two->size()) ? one : two;
        const TNodeSetPtr& smaller = (one->size() > two->size()) ? two : one;

        TNodeSetPtr result = std::make_shared<TNodeSet>(*bigger);

        bool inserted = false;
        for (auto& item : *smaller) {
            if (result->insert(item).second) {
                inserted = true;
            }
        }

        return inserted ? result : bigger;
    }

    TNodeSetPtr CollectUnresolvedArgs(const TExprNode& root, TNodeMap<TNodeSetPtr>& unresolvedArgs, TNodeSet& allArgs) {
        auto it = unresolvedArgs.find(&root);
        if (it != unresolvedArgs.end()) {
            return it->second;
        }

        TNodeSetPtr result;
        switch (root.Type()) {
            case TExprNode::Argument:
                result = std::make_shared<TNodeSet>(TNodeSet{&root});
                break;
            case TExprNode::Lambda:
            {
                if (!root.ChildrenSize()) {
                    ythrow yexception() << "lambda #" << root.UniqueId() << " has " << root.ChildrenSize() << " children";
                }

                const auto& arguments = root.Head();
                if (arguments.Type() != TExprNode::Arguments) {
                    ythrow yexception() << "unexpected type of arguments node in lambda #" << root.UniqueId();
                }

                arguments.ForEachChild([&](const TExprNode& arg) {
                    if (arg.Type() != TExprNode::Argument) {
                        ythrow yexception() << "expecting argument, #[" << arg.UniqueId() << "]";
                    }
                    if (!allArgs.insert(&arg).second) {
                        ythrow yexception() << "argument is duplicated, #[" << arg.UniqueId() << "]";
                    }
                });

                for (ui32 i = 1U; i < root.ChildrenSize(); ++i) {
                    const auto bodyUnresolvedArgs = CollectUnresolvedArgs(*root.Child(i), unresolvedArgs, allArgs);
                    result = ExcludeFromUnresolved(arguments, bodyUnresolvedArgs);
                }
                break;
            }
            case TExprNode::Callable:
            case TExprNode::List:
            {
                root.ForEachChild([&](const TExprNode& child) {
                    result = MergeUnresolvedArgs(result, CollectUnresolvedArgs(child, unresolvedArgs, allArgs));
                });
                break;
            }
            case TExprNode::Atom:
            case TExprNode::World:
                break;
            case TExprNode::Arguments:
                ythrow yexception() << "unexpected free arguments node #[" << root.UniqueId() << "]";
                break;
        }

        unresolvedArgs[&root] = result;
        return result;
    }

    typedef TNodeMap<long> TRefCountsMap;


    void CalculateReferences(const TExprNode& node, TRefCountsMap& refCounts) {
        if (!refCounts[&node]++)
            for (const auto& child : node.Children())
                CalculateReferences(*child, refCounts);
    }

    void CheckReferences(const TExprNode& node, TRefCountsMap& refCounts, TNodeSet& visited) {
        if (visited.emplace(&node).second) {
            for (const auto& child : node.Children()) {
                YQL_ENSURE(child->UseCount() == refCounts[child.Get()]);
                CheckReferences(*child, refCounts, visited);
            }
        }
    }

    bool GatherParentsImpl(const TExprNode& node, TParentsMap& parentsMap, TNodeSet& visited) {
        if (node.Type() == TExprNode::Arguments || node.Type() == TExprNode::Atom || node.Type() == TExprNode::World) {
            return false;
        }

        if (!visited.emplace(&node).second) {
            return true;
        }

        node.ForEachChild([&](const TExprNode& child) {
            if (GatherParentsImpl(child, parentsMap, visited)) {
                parentsMap[&child].emplace(&node);
            }
        });

        return true;
    }

} // namespace

bool CompileExpr(TAstNode& astRoot, TExprNode::TPtr& exprRoot, TExprContext& ctx,
    IModuleResolver* resolver, IUrlListerManager* urlListerManager,
    bool hasAnnotations, ui32 typeAnnotationIndex, ui16 syntaxVersion) {
    exprRoot.Reset();
    TAstNode* cleanRoot = nullptr;
    TAnnotationNodeMap annotations;
    const TAnnotationNodeMap* currentAnnotations = nullptr;
    TAstParseResult cleanupRes;
    if (!hasAnnotations) {
        typeAnnotationIndex = Max<ui32>();
        cleanRoot = &astRoot;
        currentAnnotations = nullptr;
    } else if (typeAnnotationIndex != Max<ui32>()) {
        cleanupRes.Pool = std::make_unique<TMemoryPool>(4096);
        cleanRoot = ExtractAnnotations(astRoot, annotations, *cleanupRes.Pool);
        cleanupRes.Root = cleanRoot;
        currentAnnotations = &annotations;
    } else {
        cleanupRes.Pool = std::make_unique<TMemoryPool>(4096);
        cleanRoot = RemoveAnnotations(astRoot, *cleanupRes.Pool);
        cleanupRes.Root = cleanRoot;
        currentAnnotations = nullptr;
    }

    if (!cleanRoot) {
        return false;
    }

    TContext compileCtx(ctx);
    compileCtx.SyntaxVersion = syntaxVersion;
    compileCtx.Annotations = currentAnnotations;
    compileCtx.TypeAnnotationIndex = typeAnnotationIndex;
    compileCtx.ModuleResolver = resolver;
    compileCtx.UrlListerManager = urlListerManager;
    compileCtx.PushFrame();
    auto world = compileCtx.Expr.NewWorld(astRoot.GetPosition());
    if (typeAnnotationIndex != Max<ui32>()) {
        world->SetTypeAnn(compileCtx.Expr.MakeType<TWorldExprType>());
    }

    compileCtx.Frames.back().Bindings[TStringBuf("world")] = {std::move(world)};
    auto ret = CompileFunction(*cleanRoot, compileCtx, true);
    if (1U != ret.size())
        return false;
    exprRoot = std::move(ret.front());
    compileCtx.PopFrame();
    return bool(exprRoot);
}

bool CompileExpr(TAstNode& astRoot, TExprNode::TPtr& exprRoot, TExprContext& ctx,
    IModuleResolver* resolver, IUrlListerManager* urlListerManager,
    ui32 annotationFlags, ui16 syntaxVersion)
{
    bool hasAnnotations = annotationFlags != TExprAnnotationFlags::None;
    ui32 typeAnnotationIndex = Max<ui32>();
    if (annotationFlags & TExprAnnotationFlags::Types) {
        bool hasPostions = annotationFlags & TExprAnnotationFlags::Position;
        typeAnnotationIndex = hasPostions ? 1 : 0;
    }

    return CompileExpr(astRoot, exprRoot, ctx, resolver, urlListerManager, hasAnnotations, typeAnnotationIndex, syntaxVersion);
}

bool CompileExpr(TAstNode& astRoot, TLibraryCohesion& library, TExprContext& ctx, ui16 syntaxVersion) {
    const TAstNode* cleanRoot = &astRoot;
    TContext compileCtx(ctx);
    compileCtx.Annotations = nullptr;
    compileCtx.TypeAnnotationIndex = Max<ui32>();
    compileCtx.SyntaxVersion = syntaxVersion;
    const bool ok = CompileLibrary(*cleanRoot, compileCtx);
    library = compileCtx.Cohesion;
    return ok;
}

const TTypeAnnotationNode* CompileTypeAnnotation(const TAstNode& node, TExprContext& ctx) {
    TContext compileCtx(ctx);
    return compileCtx.CompileTypeAnnotationNode(node);
}

template<class Set>
bool IsDependedImpl(const TExprNode& node, const Set& dependences, TNodeSet& visited) {
    if (!visited.emplace(&node).second)
        return false;

    if (dependences.cend() != dependences.find(&node))
        return true;

    for (const auto& child : node.Children()) {
        if (IsDependedImpl(*child, dependences, visited))
            return true;
    }

    return false;
}

namespace {

enum EChangeState : ui8 {
    Unknown = 0,
    Changed = 1,
    Unchanged = 2
};

ui64 CalcBloom(const ui64 id) {
    return 1ULL |
        (2ULL << (std::hash<ui64>()(id) % 63ULL)) |
        (2ULL << (IntHash<ui64>(id) % 63ULL)) |
        (2ULL << (FnvHash<ui64>(&id, sizeof(id)) % 63ULL)) |
        (2ULL << (MurmurHash<ui64>(&id, sizeof(id)) % 63ULL)) |
        (2ULL << (CityHash64(reinterpret_cast<const char*>(&id), sizeof(id)) % 63ULL));
}

inline bool InBloom(const ui64 set, const ui64 bloom) {
    return (bloom >> 1) == ((bloom & set) >> 1);
}

EChangeState GetChanges(TExprNode* start, const TNodeOnNodeOwnedMap& replaces, const TNodeMap<TNodeOnNodeOwnedMap>& localReplaces,
    TNodeMap<EChangeState>& changes, TNodeMap<bool>& updatedLambdas);

EChangeState DoGetChanges(TExprNode* start, const TNodeOnNodeOwnedMap& replaces, const TNodeMap<TNodeOnNodeOwnedMap>& localReplaces,
    TNodeMap<EChangeState>& changes, TNodeMap<bool>& updatedLambdas) {

    if (start->GetBloom() & 1ULL) {
        bool maybe = false;
        for (const auto& repl : replaces) {
            if (repl.second && !repl.first->Dead()) {
                if (TExprNode::Argument != repl.first->Type()) {
                    maybe = true;
                    break;
                }

                if (!repl.first->GetBloom())
                    const_cast<TExprNode*>(repl.first)->SetBloom(CalcBloom(repl.first->UniqueId()));

                if (InBloom(start->GetBloom(), repl.first->GetBloom())) {
                    maybe = true;
                    break;
                }
            }
        }

        if (!maybe) {
            return EChangeState::Unchanged;
        }
    }

    start->SetBloom(1ULL);
    ui32 combinedState = EChangeState::Unchanged;
    bool incompleteBloom = false;
    start->ForEachChild([&](TExprNode& child) {
        combinedState |= GetChanges(&child, replaces, localReplaces, changes, updatedLambdas);
        start->SetBloom(start->GetBloom() | child.GetBloom());
        incompleteBloom = incompleteBloom || (child.Type() != TExprNode::Arguments && !child.GetBloom());
    });
    if (incompleteBloom) {
        start->SetBloom(0ULL);
    }

    return (EChangeState)combinedState;
}

EChangeState GetChanges(TExprNode* start, const TNodeOnNodeOwnedMap& replaces, const TNodeMap<TNodeOnNodeOwnedMap>& localReplaces,
    TNodeMap<EChangeState>& changes, TNodeMap<bool>& updatedLambdas) {
    if (start->Type() == TExprNode::Arguments) {
        return EChangeState::Unchanged;
    }

    if (!start->GetBloom() && TExprNode::Argument == start->Type()) {
        start->SetBloom(CalcBloom(start->UniqueId()));
    }

    auto& state = changes[start];
    if (state != EChangeState::Unknown) {
        return state;
    }

    if (const auto it = replaces.find(start); it != replaces.cend()) {
        return state = it->second ? EChangeState::Changed : EChangeState::Unchanged;
    }

    if (start->ChildrenSize() == 0) {
        return state = EChangeState::Unchanged;
    }

    if (start->Type() == TExprNode::Lambda) {
        TNodeOnNodeOwnedMap newReplaces = replaces;

        start->Head().ForEachChild([&](const TExprNode& arg){ newReplaces[&arg] = {}; });

        const auto locIt = localReplaces.find(start);
        if (locIt != localReplaces.end()) {
            for (auto& r: locIt->second) {
                newReplaces[r.first] = r.second;
            }
        }

        state = DoGetChanges(start, newReplaces, localReplaces, changes, updatedLambdas);

        if ((state & EChangeState::Changed) != 0) {
            updatedLambdas.emplace(start, false);
        }

        return state;
    }

    return state = DoGetChanges(start, replaces, localReplaces, changes, updatedLambdas);
}

template<bool KeepTypeAnns>
TExprNode::TPtr DoReplace(const TExprNode::TPtr& start, const TNodeOnNodeOwnedMap& replaces,
    const TNodeOnNodeOwnedMap& argReplaces, const TNodeMap<TNodeOnNodeOwnedMap>& localReplaces,
    TNodeMap<EChangeState>& changes, TNodeOnNodeOwnedMap& processed, TExprContext& ctx)
{
    auto& target = processed[start.Get()];
    if (target) {
        return target;
    }

    TMaybe<TExprNode::TPtr> replace;
    const auto it = replaces.find(start.Get());
    if (it != replaces.end()) {
        replace = it->second;
    }
    const auto argIt = argReplaces.find(start.Get());
    if (argIt != argReplaces.end()) {
        YQL_ENSURE(!replace.Defined());
        replace = argIt->second;
    }

    if (replace.Defined()) {
        if (*replace) {
            return target = ctx.ReplaceNodes(std::move(*replace), argReplaces);
        }

        return target = start;
    }

    if (start->ChildrenSize() != 0) {
        auto changeIt = changes.find(start.Get());
        YQL_ENSURE(changeIt != changes.end(), "Missing change");
        const bool isChanged = (changeIt->second & EChangeState::Changed) != 0;
        if (isChanged) {
            if (start->Type() == TExprNode::Lambda) {
                TNodeOnNodeOwnedMap newArgReplaces = argReplaces;
                const auto locIt = localReplaces.find(start.Get());
                YQL_ENSURE(locIt != localReplaces.end(), "Missing local changes");
                for (auto& r: locIt->second) {
                    newArgReplaces[r.first] = r.second;
                }

                const auto& args = start->Head();
                TExprNode::TListType newArgsList;
                newArgsList.reserve(args.ChildrenSize());
                args.ForEachChild([&](const TExprNode& arg) {
                    const auto argIt = newArgReplaces.find(&arg);
                    YQL_ENSURE(argIt != newArgReplaces.end(), "Missing argument");
                    processed.emplace(&arg, argIt->second);
                    newArgsList.emplace_back(argIt->second);
                });

                auto newBody = GetLambdaBody(*start);
                std::for_each(newBody.begin(), newBody.end(), [&](TExprNode::TPtr& node) {
                    node = DoReplace<KeepTypeAnns>(node, replaces, newArgReplaces, localReplaces,
                        changes, processed, ctx);
                });
                auto newArgs = ctx.NewArguments(start->Pos(), std::move(newArgsList));
                if constexpr (KeepTypeAnns)
                    newArgs->SetTypeAnn(ctx.MakeType<TUnitExprType>());
                target = ctx.NewLambda(start->Pos(), std::move(newArgs), std::move(newBody));
                if constexpr (KeepTypeAnns)
                    target->SetTypeAnn(start->GetTypeAnn());
                return target;
            } else {
                bool replaced = false;
                TExprNode::TListType newChildren;
                newChildren.reserve(start->ChildrenSize());
                for (const auto& child : start->Children()) {
                    auto newChild = DoReplace<KeepTypeAnns>(child, replaces, argReplaces, localReplaces,
                        changes, processed, ctx);
                    if (newChild != child)
                        replaced = true;

                    newChildren.emplace_back(std::move(newChild));
                }

                if (replaced) {
                     target = ctx.ChangeChildren(*start, std::move(newChildren));
                     if constexpr (KeepTypeAnns)
                        target->SetTypeAnn(start->GetTypeAnn());
                     return target;
                }
            }
        }
    }

    return target = start;
}

void EnsureNoBadReplaces(const TExprNode& start, const TNodeOnNodeOwnedMap& replaces, TNodeSet&& visited = TNodeSet()) {
    if (!visited.insert(&start).second) {
        return;
    }

    const auto it = replaces.find(&start);
    if (it != replaces.end() && it->second) {
        ythrow yexception() << "Bad replace for node: " << start.UniqueId() << "\n";
    }

    if (start.Type() == TExprNode::Lambda) {
        TNodeOnNodeOwnedMap newReplaces = replaces;
        start.Head().ForEachChild([&](const TExprNode& arg){ newReplaces[&arg] = {}; });
        start.ForEachChild([&](const TExprNode& child){ EnsureNoBadReplaces(child, newReplaces, std::move(visited)); });
    } else {
        start.ForEachChild([&](const TExprNode& child){ EnsureNoBadReplaces(child, replaces, std::move(visited)); });
    }
}

const bool InternalDebug = false;

template<bool KeepTypeAnns>
TExprNode::TPtr ReplaceNodesImpl(TExprNode::TPtr&& start, const TNodeOnNodeOwnedMap& replaces, TNodeOnNodeOwnedMap& processed, TExprContext& ctx) {
    if (InternalDebug) {
        Cerr << "Before\n" << start->Dump() << "\n";
        Cerr << "Replaces\n";
        ui32 rep = 0;
        for (auto& x : replaces) {
            if (x.second) {
                Cerr << "#" << ++rep << " " << x.first->Dump() << "\n into " << x.second->Dump() << "\n";
            }
        }
    }

    TNodeMap<EChangeState> changes;
    TNodeMap<bool> updatedLambdas;
    TNodeMap<TNodeOnNodeOwnedMap> localReplaces;
    if ((GetChanges(start.Get(), replaces, localReplaces, changes, updatedLambdas) & EChangeState::Changed) == 0) {
        return std::move(start);
    }

    if (!updatedLambdas.empty()) {
        for (;;) {
            changes.clear();
            for (auto& x : updatedLambdas) {
                if (!x.second) {
                    TNodeOnNodeOwnedMap& lambdaReplaces = localReplaces[x.first];
                    const auto& args = x.first->Head();
                    args.ForEachChild([&](const TExprNode& arg) {
                        const auto newArg = lambdaReplaces.emplace(&arg, ctx.ShallowCopy(arg)).first->second;
                        if constexpr (KeepTypeAnns)
                            newArg->SetTypeAnn(arg.GetTypeAnn());
                    });
                    x.second = true;
                }
            }

            auto prevSize = updatedLambdas.size();
            GetChanges(start.Get(), replaces, localReplaces, changes, updatedLambdas);
            if (updatedLambdas.size() == prevSize) {
                break;
            }
        }
    }

    auto ret = DoReplace<KeepTypeAnns>(start, replaces, {}, localReplaces, changes, processed, ctx);
    if (InternalDebug) {
        Cerr << "After\n" << ret->Dump() << "\n";
        EnsureNoBadReplaces(*ret, replaces);
    }

    return ret;
}

}

TExprNode::TPtr TExprContext::ReplaceNode(TExprNode::TPtr&& start, const TExprNode& src, TExprNode::TPtr dst) {
    if (start->Type() == TExprNode::Lambda) {
        const auto& args = start->Head();
        auto body = GetLambdaBody(*start);
        std::optional<ui32> argIndex;
        for (ui32 index = 0U; index < args.ChildrenSize(); ++index) {
            const auto arg = args.Child(index);
            if (arg == &src) {
                if (argIndex) {
                    ythrow yexception() << "argument is duplicated, #[" << arg->UniqueId() << "]";
                }

                argIndex = index;
            }
        }

        if (argIndex) {
            TExprNode::TListType newArgNodes;
            newArgNodes.reserve(args.ChildrenSize());
            TNodeOnNodeOwnedMap replaces(args.ChildrenSize());

            for (ui32 i = 0U; i < args.ChildrenSize(); ++i) {
                const auto arg = args.Child(i);
                auto newArg = (i == *argIndex) ? dst : ShallowCopy(*arg);
                YQL_ENSURE(replaces.emplace(arg, newArg).second);
                newArgNodes.emplace_back(std::move(newArg));
            }

            return NewLambda(start->Pos(), NewArguments(args.Pos(), std::move(newArgNodes)), ReplaceNodes<false>(std::move(body), replaces));
        }
    } else if (&src == start) {
        return dst;
    }

    return ReplaceNodes(std::move(start), {{&src, std::move(dst)}});
}

TExprNode::TPtr TExprContext::ReplaceNodes(TExprNode::TPtr&& start, const TNodeOnNodeOwnedMap& replaces) {
    TNodeOnNodeOwnedMap processed;
    return replaces.empty() ? std::move(start) : ReplaceNodesImpl<false>(std::move(start), replaces, processed, *this);
}

template<bool KeepTypeAnns>
TExprNode::TListType TExprContext::ReplaceNodes(TExprNode::TListType&& starts, const TNodeOnNodeOwnedMap& replaces) {
    if (!replaces.empty()) {
        TNodeOnNodeOwnedMap processed;
        for (auto& node : starts) {
            node = ReplaceNodesImpl<KeepTypeAnns>(std::move(node), replaces, processed, *this);
        }
    }
    return std::move(starts);
}

template TExprNode::TListType TExprContext::ReplaceNodes<true>(TExprNode::TListType&& starts, const TNodeOnNodeOwnedMap& replaces);
template TExprNode::TListType TExprContext::ReplaceNodes<false>(TExprNode::TListType&& starts, const TNodeOnNodeOwnedMap& replaces);

bool IsDepended(const TExprNode& node, const TNodeSet& dependences) {
    TNodeSet visited;
    return !dependences.empty() && IsDependedImpl(node, dependences, visited);
}

void CheckArguments(const TExprNode& root) {
    try {
        TNodeMap<TNodeSetPtr> unresolvedArgsMap;
        TNodeSet allArgs;
        auto rootUnresolved = CollectUnresolvedArgs(root, unresolvedArgsMap, allArgs);
        if (rootUnresolved && !rootUnresolved->empty()) {
            TVector<ui64> ids;
            for (auto& i : *rootUnresolved) {
                ids.push_back(i->UniqueId());
            }
            ythrow yexception() << "detected unresolved arguments at top level: #[" << JoinSeq(", ", ids) << "]";
        }
    } catch (yexception& e) {
        e << "\n" << root.Dump();
        throw;
    }
}

TAstParseResult ConvertToAst(const TExprNode& root, TExprContext& exprContext, const TConvertToAstSettings& settings) {
#ifdef _DEBUG
    CheckArguments(root);
#endif
    TVisitNodeContext ctx(exprContext);
    ctx.RefAtoms = settings.RefAtoms;
    ctx.AllowFreeArgs = settings.AllowFreeArgs;
    ctx.NormalizeAtomFlags = settings.NormalizeAtomFlags;
    ctx.Pool = std::make_unique<TMemoryPool>(4096);
    ctx.Frames.push_back(TFrameContext());
    ctx.CurrentFrame = &ctx.Frames.front();
    VisitNode(root, 0ULL, ctx);
    ui32 uniqueNum = 0;

    for (auto& frame : ctx.Frames) {
        ctx.CurrentFrame = &frame;
        frame.TopoSortedNodes.reserve(frame.Nodes.size());
        for (const auto& node : frame.Nodes) {
            const auto name = ctx.FindBinding(node.second);
            if (name.empty()) {
                const auto& ref = ctx.References[node.second];
                if (!InlineNode(*node.second, ref.References, ref.Neighbors, settings)) {
                    if (settings.PrintArguments && node.second->IsArgument()) {
                        auto buffer = TStringBuilder() << "$" << ++uniqueNum
                            << "{" << node.second->Content() << ":"
                            << node.second->UniqueId() << "}";
                        YQL_ENSURE(frame.Bindings.emplace(node.second, buffer).second);
                    } else {
                        char buffer[1 + 10 + 1];
                        snprintf(buffer, sizeof(buffer), "$%" PRIu32, ++uniqueNum);
                        YQL_ENSURE(frame.Bindings.emplace(node.second, buffer).second);
                    }
                    frame.TopoSortedNodes.emplace_back(node.second);
                }
            }
        }
    }

    ctx.CurrentFrame = &ctx.Frames.front();
    TAstParseResult result;
    result.Root = ConvertFunction(exprContext.AppendPosition(TPosition(1, 1)), {&root}, ctx, settings.AnnotationFlags, *ctx.Pool);
    result.Pool = std::move(ctx.Pool);
    return result;
}

TAstParseResult ConvertToAst(const TExprNode& root, TExprContext& exprContext, ui32 annotationFlags, bool refAtoms) {
    TConvertToAstSettings settings;
    settings.AnnotationFlags = annotationFlags;
    settings.RefAtoms = refAtoms;
    return ConvertToAst(root, exprContext, settings);
}

TString TExprNode::Dump() const {
    TNodeSet visited;
    TStringStream out;
    DumpNode(*this, out, 0, visited);
    return out.Str();
}

TPosition TExprNode::Pos(const TExprContext& ctx) const {
    return ctx.GetPosition(Pos());
}

TExprNode::TPtr TExprContext::RenameNode(const TExprNode& node, const TStringBuf& name) {
    const auto newNode = node.ChangeContent(AllocateNextUniqueId(), AppendString(name));
    ExprNodes.emplace_back(newNode.Get());
    return newNode;
}

TExprNode::TPtr TExprContext::ShallowCopy(const TExprNode& node) {
    YQL_ENSURE(node.Type() != TExprNode::Lambda);
    const auto newNode = node.Clone(AllocateNextUniqueId());
    ExprNodes.emplace_back(newNode.Get());
    return newNode;
}

TExprNode::TPtr TExprContext::ShallowCopyWithPosition(const TExprNode& node, TPositionHandle pos) {
    YQL_ENSURE(node.Type() != TExprNode::Lambda);
    const auto newNode = node.CloneWithPosition(AllocateNextUniqueId(), pos);
    ExprNodes.emplace_back(newNode.Get());
    return newNode;
}

TExprNode::TPtr TExprContext::ChangeChildren(const TExprNode& node, TExprNode::TListType&& children) {
    const auto newNode = node.ChangeChildren(AllocateNextUniqueId(), std::move(children));
    ExprNodes.emplace_back(newNode.Get());
    return newNode;
}

TExprNode::TPtr TExprContext::ChangeChild(const TExprNode& node, ui32 index, TExprNode::TPtr&& child) {
    const auto newNode = node.ChangeChild(AllocateNextUniqueId(), index, std::move(child));
    ExprNodes.emplace_back(newNode.Get());
    return newNode;
}

TExprNode::TPtr TExprContext::ExactChangeChildren(const TExprNode& node, TExprNode::TListType&& children) {
    const auto newNode = node.ChangeChildren(AllocateNextUniqueId(), std::move(children));
    newNode->SetTypeAnn(node.GetTypeAnn());
    newNode->CopyConstraints(node);
    newNode->SetState(node.GetState());
    newNode->Result = node.Result;
    ExprNodes.emplace_back(newNode.Get());
    return newNode;
}

TExprNode::TPtr TExprContext::ExactShallowCopy(const TExprNode& node) {
    YQL_ENSURE(node.Type() != TExprNode::Lambda);
    const auto newNode = node.Clone(AllocateNextUniqueId());
    newNode->SetTypeAnn(node.GetTypeAnn());
    newNode->CopyConstraints(node);
    newNode->SetState(node.GetState());
    newNode->Result = node.Result;
    ExprNodes.emplace_back(newNode.Get());
    return newNode;
}

TExprNode::TListType GetLambdaBody(const TExprNode& node) {
    switch (node.ChildrenSize()) {
        case 1U: return {};
        case 2U: return {node.TailPtr()};
        default: break;
    }

    auto body = node.ChildrenList();
    body.erase(body.cbegin());
    return body;
}

TExprNode::TPtr TExprContext::DeepCopyLambda(const TExprNode& node, TExprNode::TListType&& body) {
    YQL_ENSURE(node.IsLambda());
    const auto& prevArgs = node.Head();

    TNodeOnNodeOwnedMap replaces(prevArgs.ChildrenSize());

    TExprNode::TListType newArgNodes;
    newArgNodes.reserve(prevArgs.ChildrenSize());
    prevArgs.ForEachChild([&](const TExprNode& arg) {
        auto newArg = ShallowCopy(arg);
        YQL_ENSURE(replaces.emplace(&arg, newArg).second);
        newArgNodes.emplace_back(std::move(newArg));
    });

    auto newBody = ReplaceNodes(std::move(body), replaces);
    return NewLambda(node.Pos(), NewArguments(prevArgs.Pos(), std::move(newArgNodes)), std::move(newBody));
}

TExprNode::TPtr TExprContext::DeepCopyLambda(const TExprNode& node, TExprNode::TPtr&& body) {
    YQL_ENSURE(node.IsLambda());
    const auto& prevArgs = node.Head();

    TNodeOnNodeOwnedMap replaces(prevArgs.ChildrenSize());

    TExprNode::TListType newArgNodes;
    newArgNodes.reserve(prevArgs.ChildrenSize());
    prevArgs.ForEachChild([&](const TExprNode& arg) {
        auto newArg = ShallowCopy(arg);
        YQL_ENSURE(replaces.emplace(&arg, newArg).second);
        newArgNodes.emplace_back(std::move(newArg));
    });

    auto newBody = ReplaceNodes(body ? TExprNode::TListType{std::move(body)} : GetLambdaBody(node), replaces);
    return NewLambda(node.Pos(), NewArguments(prevArgs.Pos(), std::move(newArgNodes)), std::move(newBody));
}

TExprNode::TPtr TExprContext::FuseLambdas(const TExprNode& outer, const TExprNode& inner) {
    YQL_ENSURE(outer.IsLambda() && inner.IsLambda());
    const auto& outerArgs = outer.Head();
    const auto& innerArgs = inner.Head();

    TNodeOnNodeOwnedMap innerReplaces(innerArgs.ChildrenSize());

    TExprNode::TListType newArgNodes;
    newArgNodes.reserve(innerArgs.ChildrenSize());

    innerArgs.ForEachChild([&](const TExprNode& arg) {
        auto newArg = ShallowCopy(arg);
        YQL_ENSURE(innerReplaces.emplace(&arg, newArg).second);
        newArgNodes.emplace_back(std::move(newArg));
    });

    auto body = ReplaceNodes(GetLambdaBody(inner), innerReplaces);

    TExprNode::TListType newBody;
    auto outerBody = GetLambdaBody(outer);
    if (outerArgs.ChildrenSize() + 1U == inner.ChildrenSize()) {
        auto i = 0U;
        TNodeOnNodeOwnedMap outerReplaces(outerArgs.ChildrenSize());
        outerArgs.ForEachChild([&](const TExprNode& arg) {
            YQL_ENSURE(outerReplaces.emplace(&arg, std::move(body[i++])).second);
        });
        newBody = ReplaceNodes(std::move(outerBody), outerReplaces);
    } else if (1U == outerArgs.ChildrenSize()) {
        newBody.reserve(newBody.size() * body.size());
        for (auto item : body) {
            for (auto root : outerBody) {
                newBody.emplace_back(ReplaceNode(TExprNode::TPtr(root), outerArgs.Head(), TExprNode::TPtr(item)));
            }
        }
    } else {
        YQL_ENSURE(outerBody.empty(), "Incompatible lambdas for fuse.");
    }

    return NewLambda(outer.Pos(), NewArguments(inner.Head().Pos(), std::move(newArgNodes)), std::move(newBody));
}

TExprNode::TPtr TExprContext::DeepCopy(const TExprNode& node, TExprContext& nodeCtx, TNodeOnNodeOwnedMap& deepClones,
    bool internStrings, bool copyTypes, bool copyResult, TCustomDeepCopier customCopier)
{
    const auto ins = deepClones.emplace(&node, nullptr);
    if (ins.second) {
        TExprNode::TListType children;
        children.reserve(node.ChildrenSize());

        if (customCopier && customCopier(node, children)) {
        } else {
            node.ForEachChild([&](const TExprNode& child) {
                children.emplace_back(DeepCopy(child, nodeCtx, deepClones, internStrings, copyTypes, copyResult, customCopier));
            });
        }

        ++NodeAllocationCounter;
        auto newNode = TExprNode::NewNode(AppendPosition(nodeCtx.GetPosition(node.Pos())), node.Type(),
            std::move(children), internStrings ? AppendString(node.Content()) : node.Content(), node.Flags(),
            AllocateNextUniqueId());

        if (copyTypes && node.GetTypeAnn()) {
            newNode->SetTypeAnn(node.GetTypeAnn());
        }

        if (copyResult && node.IsCallable() && node.HasResult()) {
            newNode->SetResult(nodeCtx.ShallowCopy(node.GetResult()));
        }

        ins.first->second = newNode;
        ExprNodes.emplace_back(ins.first->second.Get());
    }
    return ins.first->second;
}

TExprNode::TPtr TExprContext::WrapByCallableIf(bool condition, const TStringBuf& callable, TExprNode::TPtr&& node) {
    if (!condition) {
        return node;
    }
    const auto pos = node->Pos();
    return NewCallable(pos, callable, {std::move(node)});
}

TExprNode::TPtr TExprContext::SwapWithHead(const TExprNode& node) {
    return ChangeChild(node.Head(), 0U, ChangeChild(node, 0U, node.Head().HeadPtr()));
}

TConstraintSet TExprContext::MakeConstraintSet(const NYT::TNode& serializedConstraints) {
    const static std::unordered_map<std::string_view, std::function<const TConstraintNode*(TExprContext&, const NYT::TNode&)>> FACTORIES = {
        {TSortedConstraintNode::Name(),     std::mem_fn(&TExprContext::MakeConstraint<TSortedConstraintNode, const NYT::TNode&>)},
        {TChoppedConstraintNode::Name(),    std::mem_fn(&TExprContext::MakeConstraint<TChoppedConstraintNode, const NYT::TNode&>)},
        {TUniqueConstraintNode::Name(),     std::mem_fn(&TExprContext::MakeConstraint<TUniqueConstraintNode, const NYT::TNode&>)},
        {TDistinctConstraintNode::Name(),   std::mem_fn(&TExprContext::MakeConstraint<TDistinctConstraintNode, const NYT::TNode&>)},
        {TEmptyConstraintNode::Name(),      std::mem_fn(&TExprContext::MakeConstraint<TEmptyConstraintNode, const NYT::TNode&>)},
        {TVarIndexConstraintNode::Name(),   std::mem_fn(&TExprContext::MakeConstraint<TVarIndexConstraintNode, const NYT::TNode&>)},
        {TMultiConstraintNode::Name(),      std::mem_fn(&TExprContext::MakeConstraint<TMultiConstraintNode, const NYT::TNode&>)},
    };
    TConstraintSet res;
    YQL_ENSURE(serializedConstraints.IsMap(), "Unexpected node type with serialize constraints: " << serializedConstraints.GetType());
    for (const auto& [key, node]: serializedConstraints.AsMap()) {
        auto it = FACTORIES.find(key);
        YQL_ENSURE(it != FACTORIES.cend(), "Unsupported constraint construction: " << key);
        try {
            res.AddConstraint((it->second)(*this, node));
        } catch (...) {
            YQL_ENSURE(false, "Error while constructing constraint: " << CurrentExceptionMessage());
        }
    }
    return res;
}

TNodeException::TNodeException()
    : Pos_()
{
}

TNodeException::TNodeException(const TExprNode& node)
    : Pos_(node.Pos())
{
}

TNodeException::TNodeException(const TExprNode* node)
    : Pos_(node ? node->Pos() : TPositionHandle())
{
}

TNodeException::TNodeException(const TPositionHandle& pos)
    : Pos_(pos)
{
}

bool ValidateName(TPosition position, TStringBuf name, TStringBuf descr, TExprContext& ctx) {
    if (name.empty()) {
        ctx.AddError(TIssue(position,
            TStringBuilder() << "Empty " << descr << " name is not allowed"));
        return false;
    }

    if (!IsUtf8(name)) {
        ctx.AddError(TIssue(position, TStringBuilder() <<
            TString(descr).to_title() << " name must be a valid utf-8 byte sequence: " << TString{name}.Quote()));
        return false;
    }

    if (name.size() > 16_KB) {
        ctx.AddError(TIssue(position, TStringBuilder() <<
            TString(descr).to_title() << " name length must be less than " << 16_KB));
        return false;
    }

    return true;
}

bool ValidateName(TPositionHandle position, TStringBuf name, TStringBuf descr, TExprContext& ctx) {
    return ValidateName(ctx.GetPosition(position), name, descr, ctx);
}

bool TDataExprParamsType::Validate(TPosition position, TExprContext& ctx) const {
    if (GetSlot() != EDataSlot::Decimal) {
        ctx.AddError(TIssue(position, TStringBuilder() << "Only Decimal may contain parameters, but got: " << GetName()));
        return false;
    }

    const auto precision = FromString<ui8>(GetParamOne());

    if (!precision || precision > 35) {
        ctx.AddError(TIssue(position, TStringBuilder() << "Invalid decimal precision: " << GetParamOne()));
        return false;
    }

    const auto scale = FromString<ui8>(GetParamTwo());

    if (scale > precision) {
        ctx.AddError(TIssue(position, TStringBuilder() << "Invalid decimal parameters: (" << GetParamOne() << "," << GetParamTwo() << ")."));
        return false;
    }

    return true;
}

bool TDataExprParamsType::Validate(TPositionHandle position, TExprContext& ctx) const {
    return Validate(ctx.GetPosition(position), ctx);
}

bool TItemExprType::Validate(TPosition position, TExprContext& ctx) const {
    return ValidateName(position, Name, "member", ctx);
}

bool TItemExprType::Validate(TPositionHandle position, TExprContext& ctx) const {
    return Validate(ctx.GetPosition(position), ctx);
}

TStringBuf TItemExprType::GetCleanName(bool isVirtual) const {
    if (!isVirtual) {
        return Name;
    }

    YQL_ENSURE(Name.StartsWith(YqlVirtualPrefix));
    return Name.SubStr(YqlVirtualPrefix.size());
}

const TItemExprType* TItemExprType::GetCleanItem(bool isVirtual, TExprContext& ctx) const {
    if (!isVirtual) {
        return this;
    }

    YQL_ENSURE(Name.StartsWith(YqlVirtualPrefix));
    return ctx.MakeType<TItemExprType>(Name.SubStr(YqlVirtualPrefix.size()), ItemType);
}

bool TMultiExprType::Validate(TPosition position, TExprContext& ctx) const {
    if (Items.size() > Max<ui16>()) {
        ctx.AddError(TIssue(position, TStringBuilder() << "Too many elements: " << Items.size()));
        return false;
    }

    return true;
}

bool TMultiExprType::Validate(TPositionHandle position, TExprContext& ctx) const {
    return Validate(ctx.GetPosition(position), ctx);
}

bool TTupleExprType::Validate(TPosition position, TExprContext& ctx) const {
    if (Items.size() > Max<ui16>()) {
        ctx.AddError(TIssue(position, TStringBuilder() << "Too many tuple elements: " << Items.size()));
        return false;
    }

    return true;
}

bool TTupleExprType::Validate(TPositionHandle position, TExprContext& ctx) const {
    return Validate(ctx.GetPosition(position), ctx);
}

bool TStructExprType::Validate(TPosition position, TExprContext& ctx) const {
    if (Items.size() > Max<ui16>()) {
        ctx.AddError(TIssue(position, TStringBuilder() << "Too many struct members: " << Items.size()));
        return false;
    }

    TString lastName;
    for (auto& item : Items) {
        if (!item->Validate(position, ctx)) {
            return false;
        }

        if (item->GetName() == lastName) {
            ctx.AddError(TIssue(position, TStringBuilder() << "Duplicated member: " << lastName));
            return false;
        }

        lastName = item->GetName();
    }

    return true;
}

bool TStructExprType::Validate(TPositionHandle position, TExprContext& ctx) const {
    return Validate(ctx.GetPosition(position), ctx);
}

bool TVariantExprType::Validate(TPosition position, TExprContext& ctx) const {
    if (UnderlyingType->GetKind() == ETypeAnnotationKind::Tuple) {
        if (!UnderlyingType->Cast<TTupleExprType>()->GetSize()) {
            ctx.AddError(TIssue(position, TStringBuilder() << "Empty tuple is not allowed as underlying type"));
            return false;
        }
    }
    else if (UnderlyingType->GetKind() == ETypeAnnotationKind::Struct) {
        if (!UnderlyingType->Cast<TStructExprType>()->GetSize()) {
            ctx.AddError(TIssue(position, TStringBuilder() << "Empty struct is not allowed as underlying type"));
            return false;
        }
    }
    else {
        ctx.AddError(TIssue(position, TStringBuilder() << "Expected tuple or struct, but got:" << *UnderlyingType));
        return false;
    }

    return true;
}

bool TVariantExprType::Validate(TPositionHandle position, TExprContext& ctx) const {
    return Validate(ctx.GetPosition(position), ctx);
}

ui32 TVariantExprType::MakeFlags(const TTypeAnnotationNode* underlyingType) {
    switch (underlyingType->GetKind()) {
        case ETypeAnnotationKind::Tuple: {
            const auto tupleType = underlyingType->Cast<TTupleExprType>();
            auto ret = CombineFlags(tupleType->GetItems());
            if (tupleType->GetSize() > 1) {
                ret |= TypeHasManyValues;
            }
            return ret;
        }
        case ETypeAnnotationKind::Struct: {
            const auto structType = underlyingType->Cast<TStructExprType>();
            auto ret = CombineFlags(structType->GetItems());
            if (structType->GetSize() > 1) {
                ret |= TypeHasManyValues;
            }
            return ret;
        }
        default: break;
    }

    ythrow yexception() << "unexpected underlying type" << *underlyingType;
}


bool TDictExprType::Validate(TPosition position, TExprContext& ctx) const {
    if (KeyType->IsHashable() && KeyType->IsEquatable()) {
        return true;
    }

    if (KeyType->IsComparableInternal()) {
        return true;
    }

    ctx.AddError(TIssue(position, TStringBuilder() << "Expected hashable and equatable or internally comparable dict key type, but got: " << *KeyType));
    return false;
}

bool TDictExprType::Validate(TPositionHandle position, TExprContext& ctx) const {
    return Validate(ctx.GetPosition(position), ctx);
}

bool TCallableExprType::Validate(TPosition position, TExprContext& ctx) const {
    if (OptionalArgumentsCount > Arguments.size()) {
        ctx.AddError(TIssue(position, TStringBuilder() << "Too many optional arguments: " << OptionalArgumentsCount
            << ", function has only " << Arguments.size() << " arguments"));
        return false;
    }

    for (ui32 index = Arguments.size() - OptionalArgumentsCount; index < Arguments.size(); ++index) {
        auto type = Arguments[index].Type;
        if (type->GetKind() != ETypeAnnotationKind::Optional) {
            ctx.AddError(TIssue(position, TStringBuilder() << "Expected optional type for argument: " << (index + 1)
                << " because it's an optional argument, but got: " << *type));
            return false;
        }
    }

    bool startedNames = false;
    std::unordered_set<std::string_view> usedNames(Arguments.size());
    for (ui32 index = 0; index < Arguments.size(); ++index) {
        bool hasName = !Arguments[index].Name.empty();
        if (startedNames) {
            if (!hasName) {
                ctx.AddError(TIssue(position, TStringBuilder() << "Unexpected positional argument at position "
                    << (index + 1) << " just after named arguments"));
                return false;
            }
        } else {
            startedNames = hasName;
        }

        if (hasName) {
            if (!usedNames.insert(Arguments[index].Name).second) {
                ctx.AddError(TIssue(position, TStringBuilder() << "Duplication of named argument: " << Arguments[index].Name));
                return false;
            }
        }
    }

    return true;
}

bool TCallableExprType::Validate(TPositionHandle position, TExprContext& ctx) const {
    return Validate(ctx.GetPosition(position), ctx);
}

bool TTaggedExprType::Validate(TPosition position, TExprContext& ctx) const {
    return ValidateName(position, Tag, "tag", ctx);
}

bool TTaggedExprType::Validate(TPositionHandle position, TExprContext& ctx) const {
    return Validate(ctx.GetPosition(position), ctx);
}

const TString& TPgExprType::GetName() const {
    return NPg::LookupType(TypeId).Name;
}

ui32 TPgExprType::GetFlags(ui32 typeId) {
    auto descPtr = &NPg::LookupType(typeId);
    if (descPtr->ArrayTypeId == descPtr->TypeId) {
        auto elemType = descPtr->ElementTypeId;
        descPtr = &NPg::LookupType(elemType);
    }

    const auto& desc = *descPtr;
    ui32 ret = TypeHasManyValues | TypeHasOptional;
    if ((!desc.SendFuncId || !desc.ReceiveFuncId) && (!desc.OutFuncId || !desc.InFuncId)) {
        ret |= TypeNonPersistable;
    }

    if (!desc.LessProcId || !desc.CompareProcId) {
        ret |= TypeNonComparable;
    }

    if (!desc.EqualProcId || !desc.CompareProcId) {
        if (desc.TypeId != NPg::UnknownOid) {
            ret |= TypeNonEquatable;
        }
    }

    if (!desc.HashProcId) {
        ret |= TypeNonHashable;
    }

    static const std::unordered_set<std::string_view> PgSupportedPresort = {
        "bool"sv,
        "int2"sv,
        "int4"sv,
        "int8"sv,
        "float4"sv,
        "float8"sv,
        "bytea"sv,
        "varchar"sv,
        "text"sv,
        "cstring"sv
    };

    if (!PgSupportedPresort.contains(descPtr->Name)) {
        ret |= TypeNonPresortable;
    }

    return ret;
}

ui64 TPgExprType::GetPgExtensionsMask(ui32 typeId) {
    auto descPtr = &NPg::LookupType(typeId);
    return MakePgExtensionMask(descPtr->ExtensionIndex);
}

ui64 MakePgExtensionMask(ui32 extensionIndex) {
    if (!extensionIndex) {
        return 0;
    }
    
    YQL_ENSURE(extensionIndex <= 64);
    return 1ull << (extensionIndex - 1);
}

TExprContext::TExprContext(ui64 nextUniqueId)
    : StringPool(4096)
    , NextUniqueId(nextUniqueId)
    , Frozen(false)
    , PositionSet(
        16,
        [this](TPositionHandle p) { return GetHash(p); },
        [this](TPositionHandle a, TPositionHandle b) { return IsEqual(a, b); }
    )
{
    auto handle = AppendPosition(TPosition());
    YQL_ENSURE(handle.Handle == 0);
    IssueManager.SetWarningToErrorTreatMessage(
        "Treat warning as error mode enabled. "
        "To disable it use \"pragma warning(\"default\", <code>);\"");
    IssueManager.SetIssueCountLimit(100);
}

TPositionHandle TExprContext::AppendPosition(const TPosition& pos) {
    YQL_ENSURE(Positions.size() <= Max<ui32>(), "Too many positions");

    TPositionHandle handle;
    handle.Handle = (ui32)Positions.size();
    Positions.push_back(pos);

    auto inserted = PositionSet.insert(handle);
    if (inserted.second) {
        return handle;
    }

    Positions.pop_back();
    return *inserted.first;
}

TPosition TExprContext::GetPosition(TPositionHandle handle) const {
    YQL_ENSURE(handle.Handle < Positions.size(), "Unknown PositionHandle");
    return Positions[handle.Handle];
}

TExprContext::~TExprContext() {
    UnFreeze();
}

void TExprContext::Freeze() {
    for (auto& node : ExprNodes) {
        node->MarkFrozen();
    }

    Frozen = true;
}

void TExprContext::UnFreeze() {
    if (Frozen) {
        for (auto& node : ExprNodes) {
            node->MarkFrozen(false);
        }

        Frozen = false;
    }
}

void TExprContext::Reset() {
    YQL_ENSURE(!Frozen);

    IssueManager.Reset();
    Step.Reset();
    RepeatTransformCounter = 0;
}

bool TExprContext::IsEqual(TPositionHandle a, TPositionHandle b) const {
    YQL_ENSURE(a.Handle < Positions.size());
    YQL_ENSURE(b.Handle < Positions.size());
    return Positions[a.Handle] == Positions[b.Handle];
}

size_t TExprContext::GetHash(TPositionHandle p) const {
    YQL_ENSURE(p.Handle < Positions.size());

    const TPosition& pos = Positions[p.Handle];
    size_t h = ComputeHash(pos.File);
    h = CombineHashes(h, NumericHash(pos.Row));
    return CombineHashes(h, NumericHash(pos.Column));
}

std::string_view TExprContext::GetIndexAsString(ui32 index) {
    const auto it = Indexes.find(index);
    if (it != Indexes.cend()) {
        return it->second;
    }

    const auto& newBuf = AppendString(ToString(index));
    Indexes.emplace_hint(it, index, newBuf);
    return newBuf;
}

template<class T, typename... Args>
const T* MakeSinglethonType(TExprContext& ctx, Args&&... args) {
    auto& singleton = std::get<const T*>(ctx.SingletonTypeCache);
    if (!singleton)
        singleton = AddType<T>(ctx, T::MakeHash(args...), std::forward<Args>(args)...);
    return singleton;
}

const TVoidExprType* TMakeTypeImpl<TVoidExprType>::Make(TExprContext& ctx) {
    return MakeSinglethonType<TVoidExprType>(ctx);
}

const TNullExprType* TMakeTypeImpl<TNullExprType>::Make(TExprContext& ctx) {
    return MakeSinglethonType<TNullExprType>(ctx);
}

const TEmptyListExprType* TMakeTypeImpl<TEmptyListExprType>::Make(TExprContext& ctx) {
    return MakeSinglethonType<TEmptyListExprType>(ctx);
}

const TEmptyDictExprType* TMakeTypeImpl<TEmptyDictExprType>::Make(TExprContext& ctx) {
    return MakeSinglethonType<TEmptyDictExprType>(ctx);
}

const TUnitExprType* TMakeTypeImpl<TUnitExprType>::Make(TExprContext& ctx) {
    return MakeSinglethonType<TUnitExprType>(ctx);
}

const TWorldExprType* TMakeTypeImpl<TWorldExprType>::Make(TExprContext& ctx) {
    return MakeSinglethonType<TWorldExprType>(ctx);
}

const TGenericExprType* TMakeTypeImpl<TGenericExprType>::Make(TExprContext& ctx) {
    return MakeSinglethonType<TGenericExprType>(ctx);
}

const TItemExprType* TMakeTypeImpl<TItemExprType>::Make(TExprContext& ctx, const TStringBuf& name, const TTypeAnnotationNode* itemType) {
    const auto hash = TItemExprType::MakeHash(name, itemType);
    TItemExprType sample(hash, name, itemType);
    if (const auto found = FindType(sample, ctx))
        return found;

    auto nameStr = ctx.AppendString(name);
    return AddType<TItemExprType>(ctx, hash, nameStr, itemType);
}

const TListExprType* TMakeTypeImpl<TListExprType>::Make(TExprContext& ctx, const TTypeAnnotationNode* itemType) {
    const auto hash = TListExprType::MakeHash(itemType);
    TListExprType sample(hash, itemType);
    if (const auto found = FindType(sample, ctx))
        return found;
    return AddType<TListExprType>(ctx, hash, itemType);
}

const TOptionalExprType* TMakeTypeImpl<TOptionalExprType>::Make(TExprContext& ctx, const TTypeAnnotationNode* itemType) {
    const auto hash = TOptionalExprType::MakeHash(itemType);
    TOptionalExprType sample(hash, itemType);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TOptionalExprType>(ctx, hash, itemType);
}

const TVariantExprType* TMakeTypeImpl<TVariantExprType>::Make(TExprContext& ctx, const TTypeAnnotationNode* underlyingType) {
    const auto hash = TVariantExprType::MakeHash(underlyingType);
    TVariantExprType sample(hash, underlyingType);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TVariantExprType>(ctx, hash, underlyingType);
}

const TErrorExprType* TMakeTypeImpl<TErrorExprType>::Make(TExprContext& ctx, const TIssue& error) {
    const auto hash = TErrorExprType::MakeHash(error);
    TErrorExprType sample(hash, error);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TErrorExprType>(ctx, hash, error);
}

const TDictExprType* TMakeTypeImpl<TDictExprType>::Make(TExprContext& ctx, const TTypeAnnotationNode* keyType,
    const TTypeAnnotationNode* payloadType) {
    const auto hash = TDictExprType::MakeHash(keyType, payloadType);
    TDictExprType sample(hash, keyType, payloadType);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TDictExprType>(ctx, hash, keyType, payloadType);
}

const TTypeExprType* TMakeTypeImpl<TTypeExprType>::Make(TExprContext& ctx, const TTypeAnnotationNode* baseType) {
    const auto hash = TTypeExprType::MakeHash(baseType);
    TTypeExprType sample(hash, baseType);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TTypeExprType>(ctx, hash, baseType);
}

const TDataExprType* TMakeTypeImpl<TDataExprType>::Make(TExprContext& ctx, EDataSlot slot) {
    const auto hash = TDataExprType::MakeHash(slot);
    TDataExprType sample(hash, slot);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TDataExprType>(ctx, hash, slot);
}

const TPgExprType* TMakeTypeImpl<TPgExprType>::Make(TExprContext& ctx, ui32 typeId) {
    const auto hash = TPgExprType::MakeHash(typeId);
    TPgExprType sample(hash, typeId);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TPgExprType>(ctx, hash, typeId);
}

const TDataExprParamsType* TMakeTypeImpl<TDataExprParamsType>::Make(TExprContext& ctx, EDataSlot slot, const TStringBuf& one, const TStringBuf& two) {
    const auto hash = TDataExprParamsType::MakeHash(slot, one, two);
    TDataExprParamsType sample(hash, slot, one, two);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TDataExprParamsType>(ctx, hash, slot, ctx.AppendString(one), ctx.AppendString(two));
}

const TCallableExprType* TMakeTypeImpl<TCallableExprType>::Make(
    TExprContext& ctx, const TTypeAnnotationNode* returnType, const TVector<TCallableExprType::TArgumentInfo>& arguments,
    size_t optionalArgumentsCount, const TStringBuf& payload) {
    const auto hash = TCallableExprType::MakeHash(returnType, arguments, optionalArgumentsCount, payload);
    TCallableExprType sample(hash, returnType, arguments, optionalArgumentsCount, payload);
    if (const auto found = FindType(sample, ctx))
        return found;

    TVector<TCallableExprType::TArgumentInfo> newArgs;
    newArgs.reserve(arguments.size());
    for (const auto& x : arguments) {
        TCallableExprType::TArgumentInfo arg;
        arg.Type = x.Type;
        arg.Name = ctx.AppendString(x.Name);
        arg.Flags = x.Flags;
        newArgs.emplace_back(arg);
    }

    return AddType<TCallableExprType>(ctx, hash, returnType, newArgs, optionalArgumentsCount, ctx.AppendString(payload));
}

const TResourceExprType* TMakeTypeImpl<TResourceExprType>::Make(TExprContext& ctx, const TStringBuf& tag) {
    const auto hash = TResourceExprType::MakeHash(tag);
    TResourceExprType sample(hash, tag);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TResourceExprType>(ctx, hash, ctx.AppendString(tag));
}

const TTaggedExprType* TMakeTypeImpl<TTaggedExprType>::Make(
    TExprContext& ctx, const TTypeAnnotationNode* baseType, const TStringBuf& tag) {
    const auto hash = TTaggedExprType::MakeHash(baseType, tag);
    TTaggedExprType sample(hash, baseType, tag);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TTaggedExprType>(ctx, hash, baseType, ctx.AppendString(tag));
}

const TStructExprType* TMakeTypeImpl<TStructExprType>::Make(
    TExprContext& ctx, const TVector<const TItemExprType*>& items) {
    if (items.empty())
        return MakeSinglethonType<TStructExprType>(ctx, items);

    auto sortedItems = items;
    Sort(sortedItems, TStructExprType::TItemLess());
    const auto hash = TStructExprType::MakeHash(sortedItems);
    TStructExprType sample(hash, sortedItems);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TStructExprType>(ctx, hash, sortedItems);
}

const TMultiExprType* TMakeTypeImpl<TMultiExprType>::Make(TExprContext& ctx, const TTypeAnnotationNode::TListType& items) {
    if (items.empty())
        return MakeSinglethonType<TMultiExprType>(ctx, items);

    const auto hash = TMultiExprType::MakeHash(items);
    TMultiExprType sample(hash, items);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TMultiExprType>(ctx, hash, items);
}

const TTupleExprType* TMakeTypeImpl<TTupleExprType>::Make(TExprContext& ctx, const TTypeAnnotationNode::TListType& items) {
    if (items.empty())
        return MakeSinglethonType<TTupleExprType>(ctx, items);

    const auto hash = TTupleExprType::MakeHash(items);
    TTupleExprType sample(hash, items);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TTupleExprType>(ctx, hash, items);
}

const TStreamExprType* TMakeTypeImpl<TStreamExprType>::Make(TExprContext& ctx, const TTypeAnnotationNode* itemType) {
    const auto hash = TStreamExprType::MakeHash(itemType);
    TStreamExprType sample(hash, itemType);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TStreamExprType>(ctx, hash, itemType);
}

const TFlowExprType* TMakeTypeImpl<TFlowExprType>::Make(TExprContext& ctx, const TTypeAnnotationNode* itemType) {
    const auto hash = TFlowExprType::MakeHash(itemType);
    TFlowExprType sample(hash, itemType);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TFlowExprType>(ctx, hash, itemType);
}

const TBlockExprType* TMakeTypeImpl<TBlockExprType>::Make(TExprContext& ctx, const TTypeAnnotationNode* itemType) {
    const auto hash = TBlockExprType::MakeHash(itemType);
    TBlockExprType sample(hash, itemType);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TBlockExprType>(ctx, hash, itemType);
}

const TScalarExprType* TMakeTypeImpl<TScalarExprType>::Make(TExprContext& ctx, const TTypeAnnotationNode* itemType) {
    const auto hash = TScalarExprType::MakeHash(itemType);
    TScalarExprType sample(hash, itemType);
    if (const auto found = FindType(sample, ctx))
        return found;

    return AddType<TScalarExprType>(ctx, hash, itemType);
}

bool CompareExprTrees(const TExprNode*& one, const TExprNode*& two) {
    TArgumentsMap map;
    ui32 level = 0;
    TNodesPairSet visited;
    return CompareExpressions(one, two, map, level, visited);
}

bool CompareExprTreeParts(const TExprNode& one, const TExprNode& two, const TNodeMap<ui32>& argsMap) {
    TArgumentsMap map;
    ui32 level = 0;
    TNodesPairSet visited;
    map.reserve(argsMap.size());
    std::for_each(argsMap.cbegin(), argsMap.cend(), [&](const TNodeMap<ui32>::value_type& v){ map.emplace(v.first, std::make_pair(0U, v.second)); });
    auto l = &one, r = &two;
    return CompareExpressions(l, r, map, level, visited);
}

void GatherParents(const TExprNode& node, TParentsMap& parentsMap) {
    parentsMap.clear();
    TNodeSet visisted;
    GatherParentsImpl(node, parentsMap, visisted);
}

void CheckCounts(const TExprNode& root) {
    TRefCountsMap refCounts;
    CalculateReferences(root, refCounts);
    TNodeSet visited;
    CheckReferences(root, refCounts, visited);
}

TString SubstParameters(const TString& str, const TMaybe<NYT::TNode>& params, TSet<TString>* usedNames) {
    size_t pos = 0;
    try {
        TStringBuilder res;
        bool insideBrackets = false;
        TStringBuilder paramBuilder;
        for (char c : str) {
            if (c == '{') {
                if (insideBrackets) {
                    throw yexception() << "Unpexpected {";
                }

                insideBrackets = true;
                continue;
            }

            if (c == '}') {
                if (!insideBrackets) {
                    throw yexception() << "Unexpected }";
                }

                insideBrackets = false;
                TString param = paramBuilder;
                paramBuilder.clear();
                if (usedNames) {
                    usedNames->insert(param);
                }

                if (params) {
                    const auto& map = params->AsMap();
                    auto it = map.find(param);
                    if (it == map.end()) {
                        throw yexception() << "No such parameter: '" << param << "'";
                    }

                    const auto& value = it->second["Data"];
                    if (!value.IsString()) {
                        throw yexception() << "Parameter value must be a string";
                    }

                    res << value.AsString();
                }

                continue;
            }

            if (insideBrackets) {
                paramBuilder << c;
            }
            else {
                res << c;
            }

            ++pos;
        }

        if (insideBrackets) {
            throw yexception() << "Missing }";
        }

        return res;
    }
    catch (yexception& e) {
        throw yexception() << "Failed to substitute parameters into url: " << str << ", reason:" << e.what() << ", position: " << pos;
    }
}

const TTypeAnnotationNode* GetSeqItemType(const TTypeAnnotationNode* type) {
    if (!type)
        return nullptr;

    switch (type->GetKind()) {
        case ETypeAnnotationKind::List: return type->Cast<TListExprType>()->GetItemType();
        case ETypeAnnotationKind::Flow: return type->Cast<TFlowExprType>()->GetItemType();
        case ETypeAnnotationKind::Stream: return type->Cast<TStreamExprType>()->GetItemType();
        case ETypeAnnotationKind::Optional: return type->Cast<TOptionalExprType>()->GetItemType();
        default: break;
    }
    return nullptr;
}

const TTypeAnnotationNode& GetSeqItemType(const TTypeAnnotationNode& type) {
    if (const auto itemType = GetSeqItemType(&type))
        return *itemType;
    throw yexception() << "Impossible to get item type from " << type;
}

const TTypeAnnotationNode& RemoveOptionality(const TTypeAnnotationNode& type) {
    return ETypeAnnotationKind::Optional == type.GetKind() ? *type.Cast<TOptionalExprType>()->GetItemType() : type;
}

} // namespace NYql

template<>
void Out<NYql::TExprNode::EType>(class IOutputStream &o, NYql::TExprNode::EType x) {
#define YQL_EXPR_NODE_TYPE_MAP_TO_STRING_IMPL(name, ...) \
    case NYql::TExprNode::name: \
        o << #name; \
        return;

    switch (x) {
        YQL_EXPR_NODE_TYPE_MAP(YQL_EXPR_NODE_TYPE_MAP_TO_STRING_IMPL)
    default:
        o << static_cast<int>(x);
        return;
    }
}

template<>
void Out<NYql::TExprNode::EState>(class IOutputStream &o, NYql::TExprNode::EState x) {
#define YQL_EXPR_NODE_STATE_MAP_TO_STRING_IMPL(name, ...) \
    case NYql::TExprNode::EState::name: \
        o << #name; \
        return;

    switch (x) {
        YQL_EXPR_NODE_STATE_MAP(YQL_EXPR_NODE_STATE_MAP_TO_STRING_IMPL)
    default:
        o << static_cast<int>(x);
        return;
    }
}
