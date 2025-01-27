#ifndef JINJA2CPP_SRC_FUNCTION_BASE_H
#define JINJA2CPP_SRC_FUNCTION_BASE_H

#include "expression_evaluator.h"
#include "internal_value.h"

namespace jinja2
{
class FunctionBase
{
public:
    bool operator==(const FunctionBase& other) const
    {
        return m_args == other.m_args;
    }
    bool operator!=(const FunctionBase& other) const
    {
        return !(*this == other);
    }
protected:
    bool ParseParams(const std::initializer_list<ArgumentInfo>& argsInfo, const CallParamsInfo& params);
    InternalValue GetArgumentValue(const std::string& argName, RenderContext& context, InternalValue defVal = InternalValue());

protected:
    ParsedArgumentsInfo m_args;
};

//bool operator==(const FunctionBase& lhs, const FunctionBase& rhs)
//{
//    return
//}

inline bool FunctionBase::ParseParams(const std::initializer_list<ArgumentInfo>& argsInfo, const CallParamsInfo& params)
{
    bool result = true;
    m_args = helpers::ParseCallParamsInfo(argsInfo, params, result);

    return result;
}

inline InternalValue FunctionBase::GetArgumentValue(const std::string& argName, RenderContext& context, InternalValue defVal)
{
    auto argExpr = m_args[argName];
    return argExpr ? argExpr->Evaluate(context) : std::move(defVal);
}

} // namespace jinja2

#endif // JINJA2CPP_SRC_FUNCTION_BASE_H
