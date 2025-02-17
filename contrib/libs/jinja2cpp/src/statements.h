#ifndef JINJA2CPP_SRC_STATEMENTS_H
#define JINJA2CPP_SRC_STATEMENTS_H

#include "renderer.h"
#include "expression_evaluator.h"

#include <string>
#include <vector>

namespace jinja2
{
class Statement : public VisitableRendererBase
{
public:
    VISITABLE_STATEMENT();
};

template<typename T = Statement>
using StatementPtr = std::shared_ptr<T>;

template<typename CharT>
class TemplateImpl;

struct MacroParam
{
    std::string paramName;
    ExpressionEvaluatorPtr<> defaultValue;
};
inline bool operator==(const MacroParam& lhs, const MacroParam& rhs)
{
    if (lhs.paramName != rhs.paramName)
        return false;
    if (lhs.defaultValue != rhs.defaultValue)
        return false;
    return true;
}

using MacroParams = std::vector<MacroParam>;

class ForStatement : public Statement
{
public:
    VISITABLE_STATEMENT();

    ForStatement(std::vector<std::string> vars, ExpressionEvaluatorPtr<> expr, ExpressionEvaluatorPtr<> ifExpr, bool isRecursive)
        : m_vars(std::move(vars))
        , m_value(expr)
        , m_ifExpr(ifExpr)
        , m_isRecursive(isRecursive)
    {
    }

    void SetMainBody(RendererPtr renderer)
    {
        m_mainBody = std::move(renderer);
    }

    void SetElseBody(RendererPtr renderer)
    {
        m_elseBody = std::move(renderer);
    }

    void Render(OutStream& os, RenderContext& values) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const ForStatement*>(&other);
        if (!val)
            return false;
        if (m_vars != val->m_vars)
            return false;
        if (m_value != val->m_value)
            return false;
        if (m_ifExpr != val->m_ifExpr)
            return false;
        if (m_isRecursive != val->m_isRecursive)
            return false;
        if (m_mainBody != val->m_mainBody)
            return false;
        if (m_elseBody != val->m_elseBody)
            return false;
        return true;
    }

private:
  void RenderLoop(const InternalValue &loopVal, OutStream &os,
                  RenderContext &values, int level);
    ListAdapter CreateFilteredAdapter(const ListAdapter& loopItems, RenderContext& values) const;

private:
    std::vector<std::string> m_vars;
    ExpressionEvaluatorPtr<> m_value;
    ExpressionEvaluatorPtr<> m_ifExpr;
    bool m_isRecursive{};
    RendererPtr m_mainBody;
    RendererPtr m_elseBody;
};

class ElseBranchStatement;

class IfStatement : public Statement
{
public:
    VISITABLE_STATEMENT();

    IfStatement(ExpressionEvaluatorPtr<> expr)
        : m_expr(expr)
    {
    }

    void SetMainBody(RendererPtr renderer)
    {
        m_mainBody = std::move(renderer);
    }

    void AddElseBranch(StatementPtr<ElseBranchStatement> branch)
    {
        m_elseBranches.push_back(branch);
    }

    void Render(OutStream& os, RenderContext& values) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const IfStatement*>(&other);
        if (!val)
            return false;
        if (m_expr != val->m_expr)
            return false;
        if (m_mainBody != val->m_mainBody)
            return false;
        if (m_elseBranches != val->m_elseBranches)
            return false;
        return true;
    }
private:
    ExpressionEvaluatorPtr<> m_expr;
    RendererPtr m_mainBody;
    std::vector<StatementPtr<ElseBranchStatement>> m_elseBranches;
};


class ElseBranchStatement : public Statement
{
public:
    VISITABLE_STATEMENT();

    ElseBranchStatement(ExpressionEvaluatorPtr<> expr)
        : m_expr(expr)
    {
    }

    bool ShouldRender(RenderContext& values) const;
    void SetMainBody(RendererPtr renderer)
    {
        m_mainBody = std::move(renderer);
    }
    void Render(OutStream& os, RenderContext& values) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const ElseBranchStatement*>(&other);
        if (!val)
            return false;
        if (m_expr != val->m_expr)
            return false;
        if (m_mainBody != val->m_mainBody)
            return false;
        return true;
    }

private:
    ExpressionEvaluatorPtr<> m_expr;
    RendererPtr m_mainBody;
};

class SetStatement : public Statement
{
public:
    SetStatement(std::vector<std::string> fields)
        : m_fields(std::move(fields))
    {
    }

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const SetStatement*>(&other);
        if (!val)
            return false;
        if (m_fields != val->m_fields)
            return false;
        return true;
    }
protected:
    void AssignBody(InternalValue, RenderContext&);

private:
    const std::vector<std::string> m_fields;
};

class SetLineStatement final : public SetStatement
{
public:
    VISITABLE_STATEMENT();

    SetLineStatement(std::vector<std::string> fields, ExpressionEvaluatorPtr<> expr)
        : SetStatement(std::move(fields)), m_expr(std::move(expr))
    {
    }

    void Render(OutStream& os, RenderContext& values) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const SetLineStatement*>(&other);
        if (!val)
            return false;
        if (m_expr != val->m_expr)
            return false;
        return true;
    }
private:
    const ExpressionEvaluatorPtr<> m_expr;
};

class SetBlockStatement : public SetStatement
{
public:
    using SetStatement::SetStatement;

    void SetBody(RendererPtr renderer)
    {
        m_body = std::move(renderer);
    }

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const SetBlockStatement*>(&other);
        if (!val)
            return false;
        if (!SetStatement::IsEqual(*val))
            return false;
        if (m_body != val->m_body)
            return false;
        return true;
    }
protected:
    InternalValue RenderBody(RenderContext&);

private:
    RendererPtr m_body;
};

class SetRawBlockStatement final : public SetBlockStatement
{
public:
    VISITABLE_STATEMENT();

    using SetBlockStatement::SetBlockStatement;

    void Render(OutStream&, RenderContext&) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const SetRawBlockStatement*>(&other);
        if (!val)
            return false;
        if (!SetBlockStatement::IsEqual(*val))
            return false;
        return true;
    }
};

class SetFilteredBlockStatement final : public SetBlockStatement
{
public:
    VISITABLE_STATEMENT();

    explicit SetFilteredBlockStatement(std::vector<std::string> fields, ExpressionEvaluatorPtr<ExpressionFilter> expr)
        : SetBlockStatement(std::move(fields)), m_expr(std::move(expr))
    {
    }

    void Render(OutStream&, RenderContext&) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const SetFilteredBlockStatement*>(&other);
        if (!val)
            return false;
        if (!SetBlockStatement::IsEqual(*val))
            return false;
        if (m_expr != val->m_expr)
            return false;
        return true;
    }

private:
    const ExpressionEvaluatorPtr<ExpressionFilter> m_expr;
};

class ParentBlockStatement : public Statement
{
public:
    VISITABLE_STATEMENT();

    ParentBlockStatement(std::string name, bool isScoped)
        : m_name(std::move(name))
        , m_isScoped(isScoped)
    {
    }

    void SetMainBody(RendererPtr renderer)
    {
        m_mainBody = std::move(renderer);
    }
    void Render(OutStream &os, RenderContext &values) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const ParentBlockStatement*>(&other);
        if (!val)
            return false;
        if (m_name != val->m_name)
            return false;
        if (m_isScoped != val->m_isScoped)
            return false;
        if (m_mainBody != val->m_mainBody)
            return false;
        return true;
    }

private:
    std::string m_name;
    bool m_isScoped{};
    RendererPtr m_mainBody;
};

class BlockStatement : public Statement
{
public:
    VISITABLE_STATEMENT();

    BlockStatement(std::string name)
        : m_name(std::move(name))
    {
    }

    auto& GetName() const {return m_name;}

    void SetMainBody(RendererPtr renderer)
    {
        m_mainBody = std::move(renderer);
    }
    void Render(OutStream &os, RenderContext &values) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const BlockStatement*>(&other);
        if (!val)
            return false;
        if (m_name != val->m_name)
            return false;
        if (m_mainBody != val->m_mainBody)
            return false;
        return true;
    }
private:
    std::string m_name;
    RendererPtr m_mainBody;
};

class ExtendsStatement : public Statement
{
public:
    VISITABLE_STATEMENT();

    using BlocksCollection = std::unordered_map<std::string, StatementPtr<BlockStatement>>;

    ExtendsStatement(std::string name, bool isPath)
        : m_templateName(std::move(name))
        , m_isPath(isPath)
    {
    }

    void Render(OutStream &os, RenderContext &values) override;
    void AddBlock(StatementPtr<BlockStatement> block)
    {
        m_blocks[block->GetName()] = block;
    }
    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const ExtendsStatement*>(&other);
        if (!val)
            return false;
        if (m_templateName != val->m_templateName)
            return false;
        if (m_isPath != val->m_isPath)
            return false;
        if (m_blocks != val->m_blocks)
            return false;
        return true;
    }
private:
    std::string m_templateName;
    bool m_isPath{};
    BlocksCollection m_blocks;
    void DoRender(OutStream &os, RenderContext &values);
};

class IncludeStatement : public Statement
{
public:
    VISITABLE_STATEMENT();

    IncludeStatement(bool ignoreMissing, bool withContext)
        : m_ignoreMissing(ignoreMissing)
        , m_withContext(withContext)
    {}

    void SetIncludeNamesExpr(ExpressionEvaluatorPtr<> expr)
    {
        m_expr = std::move(expr);
    }

    void Render(OutStream& os, RenderContext& values) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const IncludeStatement*>(&other);
        if (!val)
            return false;
        if (m_ignoreMissing != val->m_ignoreMissing)
            return false;
        if (m_withContext != val->m_withContext)
            return false;
        if (m_expr != val->m_expr)
            return false;
        return true;
    }
private:
    bool m_ignoreMissing{};
    bool m_withContext{};
    ExpressionEvaluatorPtr<> m_expr;
};

class ImportStatement : public Statement
{
public:
    VISITABLE_STATEMENT();

    explicit ImportStatement(bool withContext)
        : m_withContext(withContext)
    {}

    void SetImportNameExpr(ExpressionEvaluatorPtr<> expr)
    {
        m_nameExpr = std::move(expr);
    }

    void SetNamespace(std::string name)
    {
        m_namespace = std::move(name);
    }

    void AddNameToImport(std::string name, std::string alias)
    {
        m_namesToImport[std::move(name)] = std::move(alias);
    }

    void Render(OutStream& os, RenderContext& values) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const ImportStatement*>(&other);
        if (!val)
            return false;
        if (m_namespace != val->m_namespace)
            return false;
        if (m_withContext != val->m_withContext)
            return false;
        if (m_namesToImport != val->m_namesToImport)
            return false;
        if (m_nameExpr != val->m_nameExpr)
            return false;
        if (m_renderer != val->m_renderer)
            return false;
        return true;
    }
private:
    void ImportNames(RenderContext& values, InternalValueMap& importedScope, const std::string& scopeName) const;

private:
    bool m_withContext{};
    RendererPtr m_renderer;
    ExpressionEvaluatorPtr<> m_nameExpr;
    std::optional<std::string> m_namespace;
    std::unordered_map<std::string, std::string> m_namesToImport;
};

class MacroStatement : public Statement
{
public:
    VISITABLE_STATEMENT();

    MacroStatement(std::string name, MacroParams params)
        : m_name(std::move(name))
        , m_params(std::move(params))
    {
    }

    void SetMainBody(RendererPtr renderer)
    {
        m_mainBody = std::move(renderer);
    }

    void Render(OutStream &os, RenderContext &values) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const MacroStatement*>(&other);
        if (!val)
            return false;
        if (m_name != val->m_name)
            return false;
        if (m_params != val->m_params)
            return false;
        if (m_mainBody != val->m_mainBody)
            return false;
        return true;
    }

protected:
    void InvokeMacroRenderer(const std::vector<ArgumentInfo>& params, const CallParams& callParams, OutStream& stream, RenderContext& context);
    void SetupCallArgs(const std::vector<ArgumentInfo>& argsInfo, const CallParams& callParams, RenderContext& context, InternalValueMap& callArgs, InternalValueMap& kwArgs, InternalValueList& varArgs);
    virtual void SetupMacroScope(InternalValueMap& scope);
    std::vector<ArgumentInfo> PrepareMacroParams(RenderContext& values);

protected:
    std::string m_name;
    MacroParams m_params;
    RendererPtr m_mainBody;
};

class MacroCallStatement : public MacroStatement
{
public:
    VISITABLE_STATEMENT();

    MacroCallStatement(std::string macroName, CallParamsInfo callParams, MacroParams callbackParams)
        : MacroStatement("$call$", std::move(callbackParams))
        , m_macroName(std::move(macroName))
        , m_callParams(std::move(callParams))
    {
    }

    void Render(OutStream &os, RenderContext &values) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const MacroCallStatement*>(&other);
        if (!val)
            return false;
        if (m_macroName != val->m_macroName)
            return false;
        if (m_callParams != val->m_callParams)
            return false;
        return true;
    }
protected:
    void SetupMacroScope(InternalValueMap& scope) override;

protected:
    std::string m_macroName;
    CallParamsInfo m_callParams;
};

class DoStatement : public Statement
{
public:
    VISITABLE_STATEMENT();

    DoStatement(ExpressionEvaluatorPtr<> expr) : m_expr(expr) {}

    void Render(OutStream &os, RenderContext &values) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const DoStatement*>(&other);
        if (!val)
            return false;
        if (m_expr != val->m_expr)
            return false;
        return true;
    }
private:
    ExpressionEvaluatorPtr<> m_expr;
};

class WithStatement : public Statement
{
public:
    VISITABLE_STATEMENT();

    void SetScopeVars(std::vector<std::pair<std::string, ExpressionEvaluatorPtr<>>> vars)
    {
        m_scopeVars = std::move(vars);
    }
    void SetMainBody(RendererPtr renderer)
    {
        m_mainBody = std::move(renderer);
    }

    void Render(OutStream &os, RenderContext &values) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const WithStatement*>(&other);
        if (!val)
            return false;
        if (m_scopeVars != val->m_scopeVars)
            return false;
        if (m_mainBody != val->m_mainBody)
            return false;
        return true;
    }
private:
    std::vector<std::pair<std::string, ExpressionEvaluatorPtr<>>> m_scopeVars;
    RendererPtr m_mainBody;
};

class FilterStatement : public Statement
{
public:
    VISITABLE_STATEMENT();

    explicit FilterStatement(ExpressionEvaluatorPtr<ExpressionFilter> expr)
      : m_expr(std::move(expr)) {}

    void SetBody(RendererPtr renderer)
    {
        m_body = std::move(renderer);
    }

    void Render(OutStream &, RenderContext &) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const FilterStatement*>(&other);
        if (!val)
            return false;
        if (m_expr != val->m_expr)
            return false;
        if (m_body != val->m_body)
            return false;
        return true;
    }
private:
    ExpressionEvaluatorPtr<ExpressionFilter> m_expr;
    RendererPtr m_body;
};

} // namespace jinja2

#endif // JINJA2CPP_SRC_STATEMENTS_H
