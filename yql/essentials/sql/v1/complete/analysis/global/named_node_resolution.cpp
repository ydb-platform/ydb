#include "named_node_resolution.h"

#include <util/generic/algorithm.h>
#include <util/generic/ptr.h>

namespace NSQLComplete {

namespace {

class TNamedNodes final: public INamedNodes {
public:
    const TNamedNode* Resolve(const TNamedNodeRef& ref) const override {
        const auto* parent = References_.FindPtr(ref);
        if (!parent) {
            return nullptr;
        }

        const auto* definition = Definitions_.FindPtr(*parent);
        if (!definition) {
            return nullptr;
        }

        return definition;
    }

    void Dump(IOutputStream& out) const override {
        struct TItem {
            TNamedNodeRef Ref;
            TMaybe<TNamedNodeRef> Def;
        };

        TVector<TItem> items(Reserve(Definitions_.size() + References_.size()));
        for (const auto& [ref, _] : Definitions_) {
            items.emplace_back(ref, TMaybe<TNamedNodeRef>{});
        }
        for (const auto& [ref, def] : References_) {
            items.emplace_back(ref, def);
        }

        SortBy(items, [](const TItem& x) {
            return std::tie(x.Ref.Position, x.Ref.Name);
        });

        for (const TItem& item : items) {
            if (auto def = item.Def) {
                out << item.Ref << " refers to " << *item.Def << '\n';
            } else {
                out << item.Ref << " definition\n";
            }
        }
    }

    void Define(const TNamedNodeRef& ref, TNamedNode value) {
        Definitions_[ref] = std::move(value);
        LatestDefinitions_[ref.Name] = ref;
    }

    void Reference(const TNamedNodeRef& ref) {
        if (const auto* it = LatestDefinitions_.FindPtr(ref.Name)) {
            References_[ref] = *it;
        }
    }

private:
    THashMap<TString, TNamedNodeRef> LatestDefinitions_;
    THashMap<TNamedNodeRef, TNamedNode> Definitions_;
    THashMap<TNamedNodeRef, TNamedNodeRef> References_;
};

class TVisitor: public SQLv1Antlr4BaseVisitor {
public:
    TVisitor(TNamedNodes* names, const TEnvironment* env)
        : SQLv1Antlr4BaseVisitor()
        , Names_(names)
        , Env_(env)
    {
    }

    std::any visitDeclare_stmt(SQLv1::Declare_stmtContext* ctx) override {
        auto ref = GetNamedNodeRef(ctx->bind_parameter());
        if (!ref || ref->Name == "_") {
            return {};
        }

        ref->Name.insert(0, "$");
        const NYT::TNode* node = Env_->Parameters.FindPtr(ref->Name);
        ref->Name.erase(0, 1);

        if (node) {
            Names_->Define(*ref, *node);
        } else {
            Names_->Define(*ref, std::monostate());
        }

        return {};
    }

    std::any visitImport_stmt(SQLv1::Import_stmtContext* ctx) override {
        VisitNullableDefining(ctx->named_bind_parameter_list());
        return {};
    }

    std::any visitDefine_action_or_subquery_stmt(SQLv1::Define_action_or_subquery_stmtContext* ctx) override {
        VisitNullableDefining(ctx->action_or_subquery_args());
        VisitNullable(ctx->define_action_or_subquery_body());
        VisitNullableDefining(ctx->bind_parameter());
        return {};
    }

    std::any visitNamed_nodes_stmt(SQLv1::Named_nodes_stmtContext* ctx) override {
        auto* list = ctx->bind_parameter_list();
        if (!list) {
            return {};
        }

        auto parameters = list->bind_parameter();
        if (1 < parameters.size()) {
            for (auto* parameter : parameters) {
                VisitNullableDefining(parameter);
            }
            return {};
        }

        auto* parameter = parameters[0];
        if (!parameter) {
            return {};
        }

        TMaybe<TNamedNodeRef> ref = GetNamedNodeRef(parameter);
        if (!ref || ref->Name == "_") {
            return {};
        }

        if (auto* expr = ctx->expr()) {
            visit(expr);
            Names_->Define(std::move(*ref), expr);
        } else if (auto* select = ctx->select_unparenthesized_stmt()) {
            visit(select);
            Names_->Define(std::move(*ref), std::monostate());
        }

        return {};
    }

    std::any visitFor_stmt(SQLv1::For_stmtContext* ctx) override {
        VisitNullableDefining(ctx->bind_parameter());
        for (auto* x : ctx->do_stmt()) {
            VisitNullable(x);
        }
        return {};
    }

    std::any visitLambda(SQLv1::LambdaContext* ctx) override {
        if (ctx->TOKEN_ARROW()) {
            VisitNullableDefining(ctx->smart_parenthesis());
        } else {
            VisitNullable(ctx->smart_parenthesis());
        }
        VisitNullable(ctx->expr());
        VisitNullable(ctx->lambda_body());
        return {};
    }

    std::any visitNamed_bind_parameter(SQLv1::Named_bind_parameterContext* ctx) override {
        VisitNullableDefining(ctx->bind_parameter(0));
        return {};
    }

    std::any visitBind_parameter(SQLv1::Bind_parameterContext* ctx) override {
        auto ref = GetNamedNodeRef(ctx);
        if (!ref || ref->Name == "_") {
            return {};
        }

        if (IsDefining_) {
            Names_->Define(std::move(*ref), std::monostate());
        } else {
            Names_->Reference(std::move(*ref));
        }

        return {};
    }

private:
    std::any VisitNullable(antlr4::tree::ParseTree* tree) {
        if (tree == nullptr) {
            return {};
        }

        return visit(tree);
    }

    void VisitNullableDefining(antlr4::tree::ParseTree* tree) {
        const bool old = std::exchange(IsDefining_, true);
        VisitNullable(tree);
        IsDefining_ = old;
    }

    TNamedNodes* Names_;
    const TEnvironment* Env_;
    bool IsDefining_ = false;
};

} // namespace

TMaybe<TNamedNodeRef> GetNamedNodeRef(SQLv1::Bind_parameterContext* ctx) {
    if (auto name = GetName(ctx)) {
        return TNamedNodeRef{
            .Name = TString(std::move(*name)),
            .Position = GetPosition(ctx),
        };
    }

    return Nothing();
}

TNamedNodes::TPtr ResolveNamedNodes(TParsedInput input, const TEnvironment& env) {
    TNamedNodes* names = new TNamedNodes();
    TVisitor(names, &env).visit(input.SqlQuery);
    return THolder(names);
}

} // namespace NSQLComplete

template <>
void Out<NSQLComplete::TNamedNodeRef>(IOutputStream& out, const NSQLComplete::TNamedNodeRef& value) {
    out << value.Position << ":" << value.Name;
}
