#ifndef JINJA2CPP_SRC_AST_VISITOR_H
#define JINJA2CPP_SRC_AST_VISITOR_H

namespace jinja2
{
class IRendererBase;
class ExpressionEvaluatorBase;
class Statement;
class ForStatement;
class IfStatement;
class ElseBranchStatement;
class SetStatement;
class ParentBlockStatement;
class BlockStatement;
class ExtendsStatement;
class IncludeStatement;
class ImportStatement;
class MacroStatement;
class MacroCallStatement;
class ComposedRenderer;
class RawTextRenderer;
class ExpressionRenderer;

class StatementVisitor;

class VisitableStatement
{
public:
    virtual ~VisitableStatement() = default;

    virtual void ApplyVisitor(StatementVisitor* visitor) = 0;
    virtual void ApplyVisitor(StatementVisitor* visitor) const = 0;
};

#define VISITABLE_STATEMENT() \
    void ApplyVisitor(StatementVisitor* visitor) override {visitor->DoVisit(this);} \
    void ApplyVisitor(StatementVisitor* visitor) const override {visitor->DoVisit(this);} \

namespace detail
{
template<typename Base, typename Type>
class VisitorIfaceImpl : public Base
{
public:
    using Base::DoVisit;

    virtual void DoVisit(Type*) {}
    virtual void DoVisit(const Type*) {}
};

template<typename Type>
class VisitorIfaceImpl<void, Type>
{
public:
    virtual void DoVisit(Type*) {}
    virtual void DoVisit(const Type*) {}
};

template<typename Base, typename ... Types>
struct VisitorBaseImpl;

template<typename Base, typename T, typename ... Types>
struct VisitorBaseImpl<Base, T, Types...>
{
    using current_base = VisitorIfaceImpl<Base, T>;
    using base_type = typename VisitorBaseImpl<current_base, Types...>::base_type;
};

template<typename Base, typename T>
struct VisitorBaseImpl<Base, T>
{
    using base_type = VisitorIfaceImpl<Base, T>;
};


template<typename ... Types>
struct VisitorBase
{
    using type = typename VisitorBaseImpl<void, Types ...>::base_type;
};
} // namespace detail

template<typename ... Types>
using VisitorBase = typename detail::VisitorBase<Types...>::type;

class StatementVisitor : public VisitorBase<
    IRendererBase,
    Statement,
    ForStatement,
    IfStatement,
    ElseBranchStatement,
    SetStatement,
    ParentBlockStatement,
    BlockStatement,
    ExtendsStatement,
    IncludeStatement,
    ImportStatement,
    MacroStatement,
    MacroCallStatement,
    ComposedRenderer,
    RawTextRenderer,
    ExpressionRenderer>
{
public:
    void Visit(VisitableStatement* stmt)
    {
        stmt->ApplyVisitor(this);
    }
    void Visit(const VisitableStatement* stmt)
    {
        stmt->ApplyVisitor(this);
    }
};
} // namespace jinja2


#endif
