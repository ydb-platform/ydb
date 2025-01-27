#ifndef JINJA2CPP_SRC_TESTERS_H
#define JINJA2CPP_SRC_TESTERS_H

#include "expression_evaluator.h"
#include "function_base.h"
#include "jinja2cpp/value.h"
#include "render_context.h"

#include <memory>
#include <functional>

namespace jinja2
{
using TesterPtr = std::shared_ptr<IsExpression::ITester>;
using TesterParams = CallParamsInfo;

extern TesterPtr CreateTester(std::string testerName, CallParamsInfo params);

namespace testers
{

class TesterBase : public FunctionBase, public IsExpression::ITester
{
};

class Comparator : public TesterBase
{
public:
    Comparator(TesterParams params, BinaryExpression::Operation op);

    bool Test(const InternalValue& baseVal, RenderContext& context) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const Comparator*>(&other);
        if (!val)
            return false;
        return m_op == val->m_op;
    }
private:
    BinaryExpression::Operation m_op;
};

class StartsWith : public IsExpression::ITester
{
public:
    StartsWith(TesterParams);

    bool Test(const InternalValue& baseVal, RenderContext& context) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const StartsWith*>(&other);
        if (!val)
            return false;
        return m_stringEval == val->m_stringEval;
    }
private:
    ExpressionEvaluatorPtr<> m_stringEval;
};

class ValueTester : public TesterBase
{
public:
    enum Mode
    {
        IsDefinedMode,
        IsEvenMode,
        IsInMode,
        IsIterableMode,
        IsLowerMode,
        IsMappingMode,
        IsNumberMode,
        IsOddMode,
        IsSequenceMode,
        IsStringMode,
        IsUndefinedMode,
        IsUpperMode
    };

    ValueTester(TesterParams params, Mode mode);

    bool Test(const InternalValue& baseVal, RenderContext& context) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const ValueTester*>(&other);
        if (!val)
            return false;
        return m_mode == val->m_mode;
    }
private:
    Mode m_mode;
};

class UserDefinedTester : public TesterBase
{
public:
    UserDefinedTester(std::string filterName, TesterParams params);

    bool Test(const InternalValue& baseVal, RenderContext& context) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const UserDefinedTester*>(&other);
        if (!val)
            return false;
        return m_testerName == val->m_testerName && m_callParams == val->m_callParams;
    }
private:
    std::string m_testerName;
    TesterParams m_callParams;
};
} // namespace testers
} // namespace jinja2

#endif // JINJA2CPP_SRC_TESTERS_H
