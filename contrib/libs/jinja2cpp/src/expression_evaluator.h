#ifndef JINJA2CPP_SRC_EXPRESSION_EVALUATOR_H
#define JINJA2CPP_SRC_EXPRESSION_EVALUATOR_H

#include "internal_value.h"
#include "render_context.h"

#include <jinja2cpp/utils/i_comparable.h>

#include <memory>
#include <limits>

namespace jinja2
{

enum
{
    InvalidFn = -1,
    RangeFn = 1,
    LoopCycleFn = 2
};

class ExpressionEvaluatorBase : public IComparable
{
public:
    virtual ~ExpressionEvaluatorBase() {}

    virtual InternalValue Evaluate(RenderContext& values) = 0;
    virtual void Render(OutStream& stream, RenderContext& values);
};

template<typename T = ExpressionEvaluatorBase>
using ExpressionEvaluatorPtr = std::shared_ptr<T>;
using Expression = ExpressionEvaluatorBase;

inline bool operator==(const ExpressionEvaluatorPtr<>& lhs, const ExpressionEvaluatorPtr<>& rhs)
{
    if (lhs && rhs && !lhs->IsEqual(*rhs))
        return false;
    if ((lhs && !rhs) || (!lhs && rhs))
        return false;
    return true;
}
inline bool operator!=(const ExpressionEvaluatorPtr<>& lhs, const ExpressionEvaluatorPtr<>& rhs)
{
    return !(lhs == rhs);
}

struct CallParams
{
    std::unordered_map<std::string, InternalValue> kwParams;
    std::vector<InternalValue> posParams;
};

inline bool operator==(const CallParams& lhs, const CallParams& rhs)
{
    if (lhs.kwParams != rhs.kwParams)
        return false;
    if (lhs.posParams != rhs.posParams)
        return false;
    return true;
}

inline bool operator!=(const CallParams& lhs, const CallParams& rhs)
{
    return !(lhs == rhs);
}

struct CallParamsInfo
{
    std::unordered_map<std::string, ExpressionEvaluatorPtr<>> kwParams;
    std::vector<ExpressionEvaluatorPtr<>> posParams;
};

inline bool operator==(const CallParamsInfo& lhs, const CallParamsInfo& rhs)
{
    if (lhs.kwParams != rhs.kwParams)
        return false;
    if (lhs.posParams != rhs.posParams)
        return false;
    return true;
}

inline bool operator!=(const CallParamsInfo& lhs, const CallParamsInfo& rhs)
{
    return !(lhs == rhs);
}

struct ArgumentInfo
{
    std::string name;
    bool mandatory = false;
    InternalValue defaultVal;

    ArgumentInfo(std::string argName, bool isMandatory = false, InternalValue def = InternalValue())
        : name(std::move(argName))
        , mandatory(isMandatory)
        , defaultVal(std::move(def))
    {
    }
};

inline bool operator==(const ArgumentInfo& lhs, const ArgumentInfo& rhs)
{
    if (lhs.name != rhs.name)
        return false;
    if (lhs.mandatory != rhs.mandatory)
        return false;
    if (!(lhs.defaultVal == rhs.defaultVal))
        return false;
    return true;
}

inline bool operator!=(const ArgumentInfo& lhs, const ArgumentInfo& rhs)
{
    return !(lhs == rhs);
}

struct ParsedArgumentsInfo
{
    std::unordered_map<std::string, ExpressionEvaluatorPtr<>> args;
    std::unordered_map<std::string, ExpressionEvaluatorPtr<>> extraKwArgs;
    std::vector<ExpressionEvaluatorPtr<>> extraPosArgs;

    ExpressionEvaluatorPtr<> operator[](const std::string& name) const
    {
        auto p = args.find(name);
        if (p == args.end())
            return ExpressionEvaluatorPtr<>();

        return p->second;
    }
};

inline bool operator==(const ParsedArgumentsInfo& lhs, const ParsedArgumentsInfo& rhs)
{
    if (lhs.args != rhs.args)
        return false;
    if (lhs.extraKwArgs != rhs.extraKwArgs)
        return false;
    if (lhs.extraPosArgs != rhs.extraPosArgs)
        return false;
    return true;
}

inline bool operator!=(const ParsedArgumentsInfo& lhs, const ParsedArgumentsInfo& rhs)
{
    return !(lhs == rhs);
}

struct ParsedArguments
{
    std::unordered_map<std::string, InternalValue> args;
    std::unordered_map<std::string, InternalValue> extraKwArgs;
    std::vector<InternalValue> extraPosArgs;

    InternalValue operator[](const std::string& name) const
    {
        auto p = args.find(name);
        if (p == args.end())
            return InternalValue();

        return p->second;
    }
};

inline bool operator==(const ParsedArguments& lhs, const ParsedArguments& rhs)
{
    if (lhs.args != rhs.args)
        return false;
    if (lhs.extraKwArgs != rhs.extraKwArgs)
        return false;
    if (lhs.extraPosArgs != rhs.extraPosArgs)
        return false;
    return true;
}

inline bool operator!=(const ParsedArguments& lhs, const ParsedArguments& rhs)
{
    return !(lhs == rhs);
}

class ExpressionFilter;
class IfExpression;

class FullExpressionEvaluator : public ExpressionEvaluatorBase
{
public:
    void SetExpression(ExpressionEvaluatorPtr<Expression> expr)
    {
        m_expression = std::move(expr);
    }
    void SetTester(ExpressionEvaluatorPtr<IfExpression> expr)
    {
        m_tester = std::move(expr);
    }
    InternalValue Evaluate(RenderContext& values) override;
    void Render(OutStream &stream, RenderContext &values) override;

    bool IsEqual(const IComparable& other) const override
    {
        const auto* eval = dynamic_cast<const FullExpressionEvaluator*>(&other);
        if (!eval)
            return false;
        if (m_expression != eval->m_expression)
            return false;
        if (m_tester != eval->m_tester)
            return false;
        return true;
    }
private:
    ExpressionEvaluatorPtr<Expression> m_expression;
    ExpressionEvaluatorPtr<IfExpression> m_tester;
};

class ValueRefExpression : public Expression
{
public:
    ValueRefExpression(std::string valueName)
        : m_valueName(std::move(valueName))
    {
    }
    InternalValue Evaluate(RenderContext& values) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const ValueRefExpression*>(&other);
        if (!value)
            return false;
        return m_valueName != value->m_valueName;
    }
private:
    std::string m_valueName;
};

class SubscriptExpression : public Expression
{
public:
    SubscriptExpression(ExpressionEvaluatorPtr<Expression> value)
        : m_value(value)
    {
    }
    InternalValue Evaluate(RenderContext& values) override;
    void AddIndex(ExpressionEvaluatorPtr<Expression> value)
    {
        m_subscriptExprs.push_back(value);
    }

    bool IsEqual(const IComparable& other) const override
    {
        auto* otherPtr = dynamic_cast<const SubscriptExpression*>(&other);
        if (!otherPtr)
            return false;
        if (m_value != otherPtr->m_value)
            return false;
        if (m_subscriptExprs != otherPtr->m_subscriptExprs)
            return false;
        return true;
    }

private:
    ExpressionEvaluatorPtr<Expression> m_value;
    std::vector<ExpressionEvaluatorPtr<Expression>> m_subscriptExprs;
};

class FilteredExpression : public Expression
{
public:
    explicit FilteredExpression(ExpressionEvaluatorPtr<Expression> expression, ExpressionEvaluatorPtr<ExpressionFilter> filter)
        : m_expression(std::move(expression))
        , m_filter(std::move(filter))
    {
    }
    InternalValue Evaluate(RenderContext&) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* otherPtr = dynamic_cast<const FilteredExpression*>(&other);
        if (!otherPtr)
            return false;
        if (m_expression != otherPtr->m_expression)
            return false;
        if (m_filter != otherPtr->m_filter)
            return false;
        return true;
    }

private:
    ExpressionEvaluatorPtr<Expression> m_expression;
    ExpressionEvaluatorPtr<ExpressionFilter> m_filter;
};

class ConstantExpression : public Expression
{
public:
    ConstantExpression(InternalValue constant)
        : m_constant(constant)
    {}
    InternalValue Evaluate(RenderContext&) override
    {
        return m_constant;
    }

    bool IsEqual(const IComparable& other) const override
    {
        auto* otherVal = dynamic_cast<const ConstantExpression*>(&other);
        if (!otherVal)
            return false;
        return m_constant == otherVal->m_constant;
    }
private:
    InternalValue m_constant;
};

class TupleCreator : public Expression
{
public:
    TupleCreator(std::vector<ExpressionEvaluatorPtr<>> exprs)
        : m_exprs(std::move(exprs))
    {
    }

    InternalValue Evaluate(RenderContext&) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const TupleCreator*>(&other);
        if (!val)
            return false;
        return m_exprs == val->m_exprs;
    }
private:
    std::vector<ExpressionEvaluatorPtr<>> m_exprs;
};
/*
class DictionaryCreator : public Expression
{
public:
    DictionaryCreator(std::unordered_map<std::string, ExpressionEvaluatorPtr<>> items)
        : m_items(std::move(items))
    {
    }

    InternalValue Evaluate(RenderContext&) override;

private:
    std::unordered_map<std::string, ExpressionEvaluatorPtr<>> m_items;
};*/

class DictCreator : public Expression
{
public:
    DictCreator(std::unordered_map<std::string, ExpressionEvaluatorPtr<>> exprs)
        : m_exprs(std::move(exprs))
    {
    }

    InternalValue Evaluate(RenderContext&) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const DictCreator*>(&other);
        if (!val)
            return false;
        return m_exprs == val->m_exprs;
    }
private:
    std::unordered_map<std::string, ExpressionEvaluatorPtr<>> m_exprs;
};

class UnaryExpression : public Expression
{
public:
    enum Operation
    {
        LogicalNot,
        UnaryPlus,
        UnaryMinus
    };

    UnaryExpression(Operation oper, ExpressionEvaluatorPtr<> expr)
        : m_oper(oper)
        , m_expr(expr)
    {}
    InternalValue Evaluate(RenderContext&) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const UnaryExpression*>(&other);
        if (!val)
            return false;
        if (m_oper != val->m_oper)
            return false;
        if (m_expr != val->m_expr)
            return false;
        return true;
    }

private:
    Operation m_oper;
    ExpressionEvaluatorPtr<> m_expr;
};

class IsExpression : public Expression
{
public:
    virtual ~IsExpression() {}

    struct ITester : IComparable
    {
        virtual ~ITester() {}
        virtual bool Test(const InternalValue& baseVal, RenderContext& context) = 0;
    };
    using TesterPtr = std::shared_ptr<ITester>;
    using TesterFactoryFn = std::function<TesterPtr(CallParamsInfo params)>;

    IsExpression(ExpressionEvaluatorPtr<> value, const std::string& tester, CallParamsInfo params);
    InternalValue Evaluate(RenderContext& context) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const IsExpression*>(&other);
        if (!val)
            return false;
        if (m_value != val->m_value)
            return false;
        if (m_tester != val->m_tester)
            return false;
        if (m_tester && val->m_tester && !m_tester->IsEqual(*val->m_tester))
            return false;
        return true;
    }

private:
    ExpressionEvaluatorPtr<> m_value;
    TesterPtr m_tester;
};

class BinaryExpression : public Expression
{
public:
    enum Operation
    {
        LogicalAnd,
        LogicalOr,
        LogicalEq,
        LogicalNe,
        LogicalGt,
        LogicalLt,
        LogicalGe,
        LogicalLe,
        In,
        Plus,
        Minus,
        Mul,
        Div,
        DivReminder,
        DivInteger,
        Pow,
        StringConcat
    };

    enum CompareType
    {
        Undefined = 0,
        CaseSensitive = 0,
        CaseInsensitive = 1
    };

    BinaryExpression(Operation oper, ExpressionEvaluatorPtr<> leftExpr, ExpressionEvaluatorPtr<> rightExpr);
    InternalValue Evaluate(RenderContext&) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const BinaryExpression*>(&other);
        if (!val)
            return false;
        if (m_oper != val->m_oper)
            return false;
        if (m_leftExpr != val->m_leftExpr)
            return false;
        if (m_rightExpr != val->m_rightExpr)
            return false;
        if (m_inTester && val->m_inTester && !m_inTester->IsEqual(*val->m_inTester))
            return false;
        if ((!m_inTester && val->m_inTester) || (m_inTester && !val->m_inTester))
            return false;
        return true;
    }
private:
    Operation m_oper;
    ExpressionEvaluatorPtr<> m_leftExpr;
    ExpressionEvaluatorPtr<> m_rightExpr;
    IsExpression::TesterPtr m_inTester;
};


class CallExpression : public Expression
{
public:
    virtual ~CallExpression() {}

    CallExpression(ExpressionEvaluatorPtr<> valueRef, CallParamsInfo params)
        : m_valueRef(std::move(valueRef))
        , m_params(std::move(params))
    {
    }

    InternalValue Evaluate(RenderContext &values) override;
    void Render(OutStream &stream, RenderContext &values) override;

    auto& GetValueRef() const {return m_valueRef;}
    auto& GetParams() const {return m_params;}

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const CallExpression*>(&other);
        if (!val)
            return false;
        if (m_valueRef != val->m_valueRef)
            return false;
        return m_params == val->m_params;
    }
private:
    InternalValue CallArbitraryFn(RenderContext &values);
    InternalValue CallGlobalRange(RenderContext &values);
    InternalValue CallLoopCycle(RenderContext &values);

private:
    ExpressionEvaluatorPtr<> m_valueRef;
    CallParamsInfo m_params;
};

class ExpressionFilter : public IComparable
{
public:
    virtual ~ExpressionFilter() {}

    struct IExpressionFilter : IComparable
    {
        virtual ~IExpressionFilter() {}
        virtual InternalValue Filter(const InternalValue& baseVal, RenderContext& context) = 0;
    };
    using ExpressionFilterPtr = std::shared_ptr<IExpressionFilter>;
    using FilterFactoryFn = std::function<ExpressionFilterPtr(CallParamsInfo params)>;

    ExpressionFilter(const std::string& filterName, CallParamsInfo params);

    InternalValue Evaluate(const InternalValue& baseVal, RenderContext& context);
    void SetParentFilter(std::shared_ptr<ExpressionFilter> parentFilter)
    {
        m_parentFilter = std::move(parentFilter);
    }
    bool IsEqual(const IComparable& other) const override
    {
        auto* valuePtr = dynamic_cast<const ExpressionFilter*>(&other);
        if (!valuePtr)
            return false;
        if (m_filter && valuePtr->m_filter && !m_filter->IsEqual(*valuePtr->m_filter))
            return false;
        if ((m_filter && !valuePtr->m_filter) || (!m_filter && !valuePtr->m_filter))
            return false;
        if (m_parentFilter != valuePtr->m_parentFilter)
            return false;
        return true;
    }


private:
    ExpressionFilterPtr m_filter;
    std::shared_ptr<ExpressionFilter> m_parentFilter;
};


class IfExpression : public IComparable
{
public:
    virtual ~IfExpression() {}

    IfExpression(ExpressionEvaluatorPtr<> testExpr, ExpressionEvaluatorPtr<> altValue)
        : m_testExpr(testExpr)
        , m_altValue(altValue)
    {
    }

    bool Evaluate(RenderContext& context);
    InternalValue EvaluateAltValue(RenderContext& context);

    void SetAltValue(ExpressionEvaluatorPtr<> altValue)
    {
        m_altValue = std::move(altValue);
    }

    bool IsEqual(const IComparable& other) const override
    {
        auto* valPtr = dynamic_cast<const IfExpression*>(&other);
        if (!valPtr)
            return false;
        if (m_testExpr != valPtr->m_testExpr)
            return false;
        if (m_altValue != valPtr->m_altValue)
            return false;
        return true;
    }

private:
    ExpressionEvaluatorPtr<> m_testExpr;
    ExpressionEvaluatorPtr<> m_altValue;
};

namespace helpers
{
ParsedArguments ParseCallParams(const std::initializer_list<ArgumentInfo>& argsInfo, const CallParams& params, bool& isSucceeded);
ParsedArguments ParseCallParams(const std::vector<ArgumentInfo>& args, const CallParams& params, bool& isSucceeded);
ParsedArgumentsInfo ParseCallParamsInfo(const std::initializer_list<ArgumentInfo>& argsInfo, const CallParamsInfo& params, bool& isSucceeded);
ParsedArgumentsInfo ParseCallParamsInfo(const std::vector<ArgumentInfo>& args, const CallParamsInfo& params, bool& isSucceeded);
CallParams EvaluateCallParams(const CallParamsInfo& info, RenderContext& context);
} // namespace helpers
} // namespace jinja2

#endif // JINJA2CPP_SRC_EXPRESSION_EVALUATOR_H
