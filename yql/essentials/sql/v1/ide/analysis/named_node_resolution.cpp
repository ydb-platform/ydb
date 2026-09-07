#include "named_node_resolution.h"

#include "parse_tree.h"

#include <yql/essentials/sql/v1/ide/pure_ast/path_visitor.h>

#include <util/generic/scope.h>

namespace NSQLPureAST {

bool TNamedNodeRef::IsWildcard() const {
    return Name == "_";
}

TNamedNodeRef TNamedNodeRef::Wildcard(TPosition position) {
    return {.Name = "_", .Position = position};
}

bool operator<(const INamedNodeScope::TEntry& lhs, const INamedNodeScope::TEntry& rhs) {
    const auto key = [](const INamedNodeScope::TEntry& entry) {
        return std::visit([](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, TNamedNodeRef>) {
                return std::tuple(value.Position, 1U);
            } else if constexpr (std::is_same_v<T, INamedNodeDef::TPtr>) {
                return std::tuple(value->Decl().Position, 0U);
            } else if constexpr (std::is_same_v<T, INamedNodeScope::TPtr>) {
                return std::tuple(value->Position(), 2U);
            } else {
                static_assert(false, "Unhandled named-node scope entry");
            }
        }, entry);
    };

    const auto lhsKey = key(lhs);
    const auto rhsKey = key(rhs);
    if (lhsKey != rhsKey) {
        return lhsKey < rhsKey;
    }

    return std::visit([](const auto& left, const auto& right) {
        using TLeft = std::decay_t<decltype(left)>;
        using TRight = std::decay_t<decltype(right)>;
        if constexpr (!std::is_same_v<TLeft, TRight>) {
            Y_ENSURE(false, "(key(a) == key(b)) => (typeOf(a) == typeOf(b))");
            return false;
        } else if constexpr (std::is_same_v<TLeft, TNamedNodeRef>) {
            return left.Name < right.Name;
        } else if constexpr (std::is_same_v<TLeft, INamedNodeDef::TPtr>) {
            return left->Decl().Name < right->Decl().Name;
        } else if constexpr (std::is_same_v<TLeft, INamedNodeScope::TPtr>) {
            return std::owner_less<INamedNodeScope::TPtr>{}(left, right);
        } else {
            static_assert(false, "Unhandled named-node scope entry");
        }
    }, lhs, rhs);
}

namespace {

class TNamedNodeDef final: public INamedNodeDef {
public:
    using TPtr = std::shared_ptr<TNamedNodeDef>;

    TNamedNodeDef(TNamedNodeRef name, TNamedNode value)
        : Decl_(std::move(name))
        , Value_(std::move(value))
    {
    }

    const TNamedNodeRef& Decl() const override {
        return Decl_;
    }

    const TNamedNode& Value() const override {
        return Value_;
    }

    const TVector<TNamedNodeRef>& References() const override {
        return References_;
    }

    void AddReference(TNamedNodeRef ref) {
        References_.emplace_back(std::move(ref));
    }

private:
    TNamedNodeRef Decl_;
    TNamedNode Value_;
    TVector<TNamedNodeRef> References_;
};

class TNamedNodeScope final: public INamedNodeScope {
public:
    using TPtr = std::shared_ptr<TNamedNodeScope>;

    explicit TNamedNodeScope(
        TNamedNodeRef owner = {.Name = "_"},
        std::weak_ptr<TNamedNodeScope> parent = {},
        bool isIsolated = false)
        : Parent_(std::move(parent))
        , Owner_(std::move(owner))
        , IsIsolated_(isIsolated)
    {
    }

    const TNamedNodeRef& Owner() const override {
        return Owner_;
    }

    TPosition Position() const override {
        return Owner_.Position;
    }

    const TSet<INamedNodeScope::TEntry>& Entries() const override {
        return Entries_;
    }

    TNamedNodeDef::TPtr Lookup(TStringBuf name) const {
        if (const auto* def = Definitions_.FindPtr(name)) {
            return *def;
        }

        if (IsIsolated_) {
            return nullptr;
        }

        if (TPtr parent = Parent()) {
            return parent->Lookup(name);
        }

        return nullptr;
    }

    void Define(TNamedNodeDef::TPtr def) {
        Definitions_[def->Decl().Name] = def;
        Entries_.emplace(std::move(def));
    }

    void Reference(TNamedNodeRef ref) {
        Entries_.emplace(std::move(ref));
    }

    void AddChild(TPtr scope) {
        Entries_.emplace(std::move(scope));
    }

    TPtr Parent() const {
        return Parent_.lock();
    }

private:
    std::weak_ptr<TNamedNodeScope> Parent_;
    TNamedNodeRef Owner_;
    bool IsIsolated_;
    TSet<INamedNodeScope::TEntry> Entries_;
    THashMap<TString, TNamedNodeDef::TPtr> Definitions_;
};

class TNamedNodes final: public INamedNodes {
public:
    using TPtr = std::shared_ptr<TNamedNodes>;

    TNamedNodes()
        : Root_(std::make_shared<TNamedNodeScope>())
        , CurrentScope_(Root_)
    {
    }

    INamedNodeScope::TPtr TopLevel() const override {
        return Root_;
    }

    INamedNodeDef::TPtr Declaration(const TNamedNodeRef& ref) const override {
        if (const auto* declaration = Declarations_.FindPtr(ref)) {
            return *declaration;
        }
        return nullptr;
    }

    INamedNodeDef::TPtr Definition(const TNamedNodeRef& ref) const override {
        if (const auto* definition = Definitions_.FindPtr(ref)) {
            return *definition;
        }
        return nullptr;
    }

    void EnterIsolatedScope(TNamedNodeRef owner) {
        EnterScope(std::move(owner), /*isIsolated=*/true);
    }

    void EnterScope(TNamedNodeRef owner, bool isIsolated = false) {
        auto scope = std::make_shared<TNamedNodeScope>(std::move(owner), CurrentScope_, isIsolated);

        CurrentScope_->AddChild(scope);
        CurrentScope_ = std::move(scope);
    }

    void LeaveScope() {
        Y_ENSURE(CurrentScope_ != Root_, "Cannot leave the top-level named-node scope");
        CurrentScope_ = CurrentScope_->Parent();
        Y_ENSURE(CurrentScope_, "Named-node scope has no parent");
    }

    TNamedNodeDef::TPtr Define(TNamedNodeRef ref, TNamedNode value) {
        auto def = std::make_shared<TNamedNodeDef>(std::move(ref), std::move(value));
        CurrentScope_->Define(def);
        Declarations_.emplace(def->Decl(), def);
        return def;
    }

    void Reference(TNamedNodeRef ref) {
        TNamedNodeDef::TPtr def = CurrentScope_->Lookup(ref.Name);
        if (!def) {
            return;
        }

        def->AddReference(ref);
        CurrentScope_->Reference(ref);
        Definitions_.emplace(std::move(ref), std::move(def));
    }

private:
    TNamedNodeScope::TPtr Root_;
    TNamedNodeScope::TPtr CurrentScope_;
    THashMap<TNamedNodeRef, TNamedNodeDef::TPtr> Declarations_;
    THashMap<TNamedNodeRef, TNamedNodeDef::TPtr> Definitions_;
};

class TLambdaArgVisitor final: public TSQLv1PathVisitor {
public:
    std::any visitNeq_subexpr(SQLv1::Neq_subexprContext* ctx) override {
        auto ops = ctx->bit_subexpr();
        if (ops.size() != 1) {
            return {};
        }

        std::any b = VisitNullable(ops[0]);
        if (!b.has_value()) {
            return {};
        }

        if (ctx->double_question()) {
            return {};
        }

        return b;
    }

    std::any visitBind_parameter(SQLv1::Bind_parameterContext* ctx) override {
        return ctx;
    }
};

class TLambdaVisitor final: public TSQLv1PathVisitor {
public:
    std::any visitLambda(SQLv1::LambdaContext* ctx) override {
        if (!ctx->TOKEN_ARROW()) {
            return {};
        }

        std::any args = VisitNullable(ctx->smart_parenthesis());
        if (!args.has_value()) {
            return {};
        }

        return ctx;
    }

    std::any visitSmart_parenthesis(SQLv1::Smart_parenthesisContext* ctx) override {
        auto* subexpr = ctx->select_subexpr();
        if (!subexpr) {
            return ctx;
        }

        return VisitNullable(subexpr);
    }

    std::any visitSelect_subexpr(SQLv1::Select_subexprContext* ctx) override {
        if (ctx->cte_with_clause()) {
            return {};
        }

        return VisitNullable(ctx->select_subexpr_core());
    }

    std::any visitSelect_subexpr_core(SQLv1::Select_subexpr_coreContext* ctx) override {
        auto ops = ctx->select_subexpr_intersect();
        if (ops.size() != 1) {
            return {};
        }

        return VisitNullable(ops[0]);
    }

    std::any visitSelect_subexpr_intersect(SQLv1::Select_subexpr_intersectContext* ctx) override {
        auto ops = ctx->select_or_expr();
        if (ops.size() != 1) {
            return {};
        }

        return VisitNullable(ops[0]);
    }

    std::any visitSelect_or_expr(SQLv1::Select_or_exprContext* ctx) override {
        if (ctx->select_kind_partial()) {
            return {};
        }

        return VisitNullable(ctx->tuple_or_expr());
    }

    std::any visitTuple_or_expr(SQLv1::Tuple_or_exprContext* ctx) override {
        {
            if (!ctx->expr()) {
                return {};
            }

            auto b = TLambdaArgVisitor().visit(ctx->expr());
            if (!b.has_value()) {
                return {};
            }

            if (ctx->an_id_or_type()) {
                return {};
            }
        }

        for (auto* n : ctx->named_expr()) {
            if (!n) {
                return {};
            }

            auto b = TLambdaArgVisitor().visit(n);
            if (!b.has_value()) {
                return {};
            }

            if (n->an_id_or_type()) {
                return {};
            }
        }

        return ctx;
    }
};

class TBindParametersVisitor final: public TSQLv1BaseVisitor {
public:
    std::any visitBind_parameter(SQLv1::Bind_parameterContext* ctx) override {
        BindParameters_.emplace_back(ctx);
        return {};
    }

    static TVector<SQLv1::Bind_parameterContext*> Visit(antlr4::ParserRuleContext* ctx) {
        TBindParametersVisitor visitor;
        visitor.visit(ctx);
        return std::move(visitor.BindParameters_);
    }

private:
    TVector<SQLv1::Bind_parameterContext*> BindParameters_;
};

class TVisitor final: public TSQLv1BaseVisitor {
public:
    TVisitor(TNamedNodes::TPtr names, const TEnvironment* env)
        : TSQLv1BaseVisitor()
        , Names_(std::move(names))
        , Env_(env)
    {
        Y_ENSURE(env);
    }

    std::any visitNamed_nodes_stmt(SQLv1::Named_nodes_stmtContext* ctx) override {
        TNamedNode value = std::monostate();

        if (auto* expr = ctx->expr()) {
            value = expr;

            TMaybe<TNamedNodeRef> ref;
            if (auto* b = GetOnly(ctx->bind_parameter_list())) {
                ref = GetNamedNodeRef(b);
            }

            if (ref) {
                VisitMaybeLambda(expr, std::move(*ref));
            } else {
                visit(expr);
            }
        } else if (auto* select = ctx->select_unparenthesized_stmt()) {
            visit(select);
        }

        Define(ctx->bind_parameter_list(), std::move(value));
        return {};
    }

    std::any visitDeclare_stmt(SQLv1::Declare_stmtContext* ctx) override {
        Declare(ctx->bind_parameter());
        return {};
    }

    std::any visitImport_stmt(SQLv1::Import_stmtContext* ctx) override {
        Define(ctx->named_bind_parameter_list());
        return {};
    }

    std::any visitAction_or_subquery_args(SQLv1::Action_or_subquery_argsContext* ctx) override {
        for (auto* p : ctx->opt_bind_parameter()) {
            if (!p) {
                continue;
            }

            Define(p->bind_parameter(), std::monostate());
        }

        return {};
    }

    std::any visitDefine_action_or_subquery_body(SQLv1::Define_action_or_subquery_bodyContext* ctx) override {
        Names_->EnterScope(TNamedNodeRef::Wildcard(GetPosition(ctx)));
        Y_DEFER {
            Names_->LeaveScope();
        };

        VisitInline(ctx);
        return {};
    }

    std::any visitDefine_action_or_subquery_stmt(SQLv1::Define_action_or_subquery_stmtContext* ctx) override {
        TMaybe<TNamedNodeRef> ref = GetNamedNodeRef(ctx->bind_parameter());
        if (!ref) {
            ref = TNamedNodeRef::Wildcard(GetPosition(ctx));
        }

        {
            Names_->EnterScope(*ref);
            Y_DEFER {
                Names_->LeaveScope();
            };

            VisitNullable(ctx->action_or_subquery_args());
            VisitInline(ctx->define_action_or_subquery_body());
        }

        Define(std::move(*ref), std::monostate());

        return {};
    }

    std::any visitFor_stmt(SQLv1::For_stmtContext* ctx) override {
        VisitNullable(ctx->expr());

        {
            auto* then = ctx->do_stmt(0);

            TPosition position = then ? GetPosition(then) : GetPosition(ctx);

            Names_->EnterScope(TNamedNodeRef::Wildcard(position));
            Y_DEFER {
                Names_->LeaveScope();
            };

            Define(ctx->bind_parameter(), std::monostate());
            VisitInline(then);
        }

        {
            VisitNullable(ctx->do_stmt(1));
        }

        return {};
    }

    std::any visitCreate_view_stmt(SQLv1::Create_view_stmtContext* ctx) override {
        VisitNullable(ctx->simple_table_ref_core());
        VisitNullable(ctx->create_object_features());

        auto owner = TNamedNodeRef::Wildcard(GetPosition(ctx));

        if (auto* x = ctx->select_stmt()) {
            Names_->EnterScope(std::move(owner));
            Y_DEFER {
                Names_->LeaveScope();
            };

            visit(x);
            return {};
        }

        if (auto* x = ctx->define_action_or_subquery_body()) {
            Names_->EnterIsolatedScope(std::move(owner));
            Y_DEFER {
                Names_->LeaveScope();
            };

            VisitInline(x);
            return {};
        }

        return {};
    }

    std::any visitInline_action(SQLv1::Inline_actionContext* ctx) override {
        Names_->EnterScope(TNamedNodeRef::Wildcard(GetPosition(ctx)));
        Y_DEFER {
            Names_->LeaveScope();
        };

        VisitInline(ctx);
        return {};
    }

    std::any visitDo_stmt(SQLv1::Do_stmtContext* ctx) override {
        VisitNullable(ctx->call_action());
        VisitNullable(ctx->inline_action());
        return {};
    }

    std::any visitBind_parameter(SQLv1::Bind_parameterContext* ctx) override {
        Reference(ctx);
        return {};
    }

    std::any visitLambda(SQLv1::LambdaContext* ctx) override {
        VisitMaybeLambda(ctx, TNamedNodeRef::Wildcard(GetPosition(ctx)));
        return {};
    }

private:
    SQLv1::Bind_parameterContext* GetOnly(SQLv1::Bind_parameter_listContext* ctx) {
        if (!ctx) {
            return nullptr;
        }

        auto parameters = ctx->bind_parameter();
        if (parameters.size() != 1) {
            return nullptr;
        }

        return parameters[0];
    }

    void Define(SQLv1::Bind_parameter_listContext* ctx, TNamedNode value) {
        if (!ctx) {
            return;
        }

        if (auto* p = GetOnly(ctx)) {
            Define(p, std::move(value));
            return;
        }

        for (auto* p : ctx->bind_parameter()) {
            Define(p, std::monostate());
        }
    }

    void Declare(SQLv1::Bind_parameterContext* ctx) {
        TMaybe<TNamedNodeRef> ref = GetNamedNodeRef(ctx);
        if (!ref) {
            return;
        }

        if (ref->IsWildcard()) {
            return;
        }

        ref->Name.insert(0, "$");
        const NYT::TNode* node = Env_->Parameters.FindPtr(ref->Name);
        ref->Name.erase(0, 1);

        if (node) {
            Define(*ref, *node);
        } else {
            Define(*ref, std::monostate());
        }
    }

    void Define(SQLv1::Named_bind_parameter_listContext* ctx) {
        if (!ctx) {
            return;
        }

        for (auto* named : ctx->named_bind_parameter()) {
            auto* parameter = named->bind_parameter(0);
            if (auto* alias = named->bind_parameter(1)) {
                parameter = alias;
            }

            Define(parameter, std::monostate());
        }
    }

    TNamedNodeDef::TPtr Define(SQLv1::Bind_parameterContext* ctx, TNamedNode value) {
        TMaybe<TNamedNodeRef> ref = GetNamedNodeRef(ctx);
        if (!ref) {
            return nullptr;
        }

        return Define(std::move(*ref), std::move(value));
    }

    TNamedNodeDef::TPtr Define(TNamedNodeRef ref, TNamedNode value) {
        if (ref.IsWildcard()) {
            return nullptr;
        }

        return Names_->Define(std::move(ref), std::move(value));
    }

    void Reference(SQLv1::Bind_parameterContext* ctx) {
        TMaybe<TNamedNodeRef> ref = GetNamedNodeRef(ctx);
        if (!ref) {
            return;
        }

        Names_->Reference(std::move(*ref));
    }

    void VisitInline(SQLv1::Define_action_or_subquery_bodyContext* ctx) {
        if (!ctx) {
            return;
        }

        visitChildren(ctx);
    }

    void VisitInline(SQLv1::Inline_actionContext* ctx) {
        if (!ctx) {
            return;
        }

        VisitInline(ctx->define_action_or_subquery_body());
    }

    void VisitInline(SQLv1::Do_stmtContext* ctx) {
        if (!ctx) {
            return;
        }

        VisitNullable(ctx->call_action());
        VisitInline(ctx->inline_action());
    }

    void VisitMaybeLambda(antlr4::ParserRuleContext* ctx, TNamedNodeRef owner) {
        std::any maybeLambda = TLambdaVisitor().visit(ctx);
        if (!maybeLambda.has_value()) {
            visitChildren(ctx);
            return;
        }

        auto* lambda = std::any_cast<SQLv1::LambdaContext*>(&maybeLambda);
        if (!lambda) {
            visitChildren(ctx);
            return;
        }

        auto args = TBindParametersVisitor().Visit((*lambda)->smart_parenthesis());

        Names_->EnterScope(std::move(owner));
        Y_DEFER {
            Names_->LeaveScope();
        };

        for (auto* arg : args) {
            Define(arg, std::monostate());
        }

        VisitNullable((*lambda)->expr());
        VisitNullable((*lambda)->lambda_body());
    }

    const TNamedNodes::TPtr Names_;
    const TEnvironment* Env_;
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

INamedNodes::TPtr ResolveNamedNodes(IParseTree::TPtr input, const TEnvironment& env) {
    auto names = std::make_shared<TNamedNodes>();
    TVisitor(names, &env).visit(input->Root());
    return names;
}

} // namespace NSQLPureAST

template <>
void Out<NSQLPureAST::TNamedNodeRef>(IOutputStream& out, const NSQLPureAST::TNamedNodeRef& value) {
    out << value.Position << ":" << value.Name;
}
