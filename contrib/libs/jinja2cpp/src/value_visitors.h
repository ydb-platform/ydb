#ifndef JINJA2CPP_SRC_VALUE_VISITORS_H
#define JINJA2CPP_SRC_VALUE_VISITORS_H

#include "expression_evaluator.h"
#include "helpers.h"
#include "jinja2cpp/value.h"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/optional.hpp>
#include <fmt/format.h>
#include <fmt/xchar.h>

#include <iostream>
#include <cmath>
#include <limits>
#include <utility>
#include <typeinfo>

namespace jinja2
{

namespace detail
{

template<typename V>
struct RecursiveUnwrapper
{
    V* m_visitor{};

    RecursiveUnwrapper(V* v)
        : m_visitor(v)
    {}


    template<typename T>
    static const auto& UnwrapRecursive(const T& arg)
    {
        return arg; // std::forward<T>(arg);
    }

    template<typename T>
    static auto& UnwrapRecursive(const RecursiveWrapper<T>& arg)
    {
        return arg.GetValue();
    }

//    template<typename T>
//   static auto& UnwrapRecursive(RecursiveWrapper<T>& arg)
//    {
//        return arg.GetValue();
//    }

    template<typename ... Args>
    auto operator()(const Args& ... args) const
    {
        assert(m_visitor != nullptr);
        return (*m_visitor)(UnwrapRecursive(args)...);
    }
};

template<typename Fn>
auto ApplyUnwrapped(const InternalValueData& val, Fn&& fn)
{
    auto valueRef = GetIf<ValueRef>(&val);
    auto targetString = GetIf<TargetString>(&val);
    auto targetSV = GetIf<TargetStringView>(&val);
    // auto internalValueRef = GetIf<InternalValueRef>(&val);

    if (valueRef != nullptr)
        return fn(valueRef->get().data());
    else if (targetString != nullptr)
        return fn(*targetString);
    else if (targetSV != nullptr)
        return fn(*targetSV);
//    else if (internalValueRef != nullptr)
//        return fn(internalValueRef->get());

    return fn(val);
}
} // namespace detail

template<typename V, typename ... Args>
auto Apply(const InternalValue& val, Args&& ... args)
{
    return detail::ApplyUnwrapped(val.GetData(), [&args...](auto& val) {
        auto v = V(args...);
        return std::visit(detail::RecursiveUnwrapper<V>(&v), val);
    });
}

template<typename V, typename ... Args>
auto Apply2(const InternalValue& val1, const InternalValue& val2, Args&& ... args)
{
    return detail::ApplyUnwrapped(val1.GetData(), [&val2, &args...](auto& uwVal1) {
        return detail::ApplyUnwrapped(val2.GetData(), [&uwVal1, &args...](auto& uwVal2) {
            auto v = V(args...);
            return std::visit(detail::RecursiveUnwrapper<V>(&v), uwVal1, uwVal2);
        });
    });
}

bool ConvertToBool(const InternalValue& val);

namespace visitors
{
template<typename R = InternalValue>
struct BaseVisitor
{
    R operator() (const GenericMap&) const
    {
        assert(false);
        return R();
    }

    R operator() (const GenericList&) const
    {
        assert(false);
        return R();
    }

    R operator() (const ValueRef&) const
    {
        assert(false);
        return R();
    }

    R operator() (const TargetString&) const
    {
        assert(false);
        return R();
    }

    template<typename T>
    R operator() (T&&) const
    {
        return R();
    }

    template<typename T, typename U>
    R operator() (T&&, U&&) const
    {
        return R();
    }
};


template<typename CharT>
struct ValueRendererBase
{
    ValueRendererBase(std::basic_string<CharT>& os)
        : m_os(&os)
    {
    }

    template<typename T>
    void operator()(const T& val) const;
    void operator()(double val) const;
    void operator()(const std::basic_string_view<CharT>& val) const
    {
        m_os->append(val.begin(), val.end());
    }
    void operator()(const std::basic_string<CharT>& val) const
    {
        m_os->append(val.begin(), val.end());
    }

    void operator()(const EmptyValue&) const {}
    void operator()(const ValuesList&) const {}
    void operator()(const ValuesMap&) const {}
    void operator()(const GenericMap&) const {}
    void operator()(const GenericList&) const {}
    void operator()(const MapAdapter&) const {}
    void operator()(const ListAdapter&) const {}
    void operator()(const ValueRef&) const {}
    void operator()(const TargetString&) const {}
    void operator()(const TargetStringView&) const {}
    void operator()(const KeyValuePair&) const {}
    void operator()(const Callable&) const {}
    void operator()(const UserCallable&) const {}
    void operator()(const std::shared_ptr<IRendererBase>) const {}
    template<typename T>
    void operator()(const boost::recursive_wrapper<T>&) const {}
    template<typename T>
    void operator()(const RecWrapper<T>&) const {}

    auto GetOs() const { return std::back_inserter(*m_os); }

    std::basic_string<CharT>* m_os;
};

template<>
template<typename T>
void ValueRendererBase<char>::operator()(const T& val) const
{
    fmt::format_to(GetOs(), "{}", val);
}

template<>
template<typename T>
void ValueRendererBase<wchar_t>::operator()(const T& val) const
{
    fmt::format_to(GetOs(), L"{}", val);
}

template<>
inline void ValueRendererBase<char>::operator()(double val) const
{
    fmt::format_to(GetOs(), "{:.8g}", val);
}

template<>
inline void ValueRendererBase<wchar_t>::operator()(double val) const
{
    fmt::format_to(GetOs(), L"{:.8g}", val);
}

struct InputValueConvertor
{
    using result_t = boost::optional<InternalValue>;

    InputValueConvertor(bool byValue, bool allowStringRef)
        : m_byValue(byValue)
        , m_allowStringRef(allowStringRef)
    {
    }

    template<typename ChT>
    result_t operator()(const std::basic_string<ChT>& val) const
    {
        if (m_allowStringRef)
            return result_t(TargetStringView(std::basic_string_view<ChT>(val)));

        return result_t(TargetString(val));
    }

    template<typename ChT>
    result_t operator()(std::basic_string<ChT>& val) const
    {
        return result_t(TargetString(std::move(val)));
    }

    result_t operator()(const ValuesList& vals) const
    {
        if (m_byValue)
        {
            ValuesList newVals(vals);
            return result_t(InternalValue(ListAdapter::CreateAdapter(std::move(newVals))));
        }

        return result_t(InternalValue(ListAdapter::CreateAdapter(vals)));
    }

    result_t operator() (ValuesList& vals) const
    {
        return result_t(InternalValue(ListAdapter::CreateAdapter(std::move(vals))));
    }

    result_t operator() (const GenericList& vals) const
    {
        if (m_byValue)
        {
            GenericList newVals(vals);
            return result_t(InternalValue(ListAdapter::CreateAdapter(std::move(newVals))));
        }

        return result_t(InternalValue(ListAdapter::CreateAdapter(vals)));
    }

    result_t operator() (GenericList& vals) const
    {
        return result_t(InternalValue(ListAdapter::CreateAdapter(std::move(vals))));
    }

    result_t operator()(const ValuesMap& vals) const
    {
        if (m_byValue)
        {
            ValuesMap newVals(vals);
            return result_t(CreateMapAdapter(std::move(newVals)));
        }

        return result_t(CreateMapAdapter(vals));
    }

    result_t operator()(ValuesMap& vals) const { return result_t(CreateMapAdapter(std::move(vals))); }

    result_t operator()(const GenericMap& vals) const
    {
        if (m_byValue)
        {
            GenericMap newVals(vals);
            return result_t(CreateMapAdapter(std::move(newVals)));
        }

        return result_t(CreateMapAdapter(vals));
    }

    result_t operator()(GenericMap& vals) const { return result_t(CreateMapAdapter(std::move(vals))); }

    result_t operator()(const UserCallable& val) const { return ConvertUserCallable(val); }

    result_t operator()(UserCallable& val) const { return ConvertUserCallable(std::move(val)); }

    template<typename T>
    result_t operator()(const RecWrapper<T>& val) const
    {
        return this->operator()(const_cast<const T&>(*val));
    }

    template<typename T>
    result_t operator()(RecWrapper<T>& val) const
    {
        return this->operator()(*val);
    }

    template<typename T>
    result_t operator()(const T& val) const
    {
        return result_t(InternalValue(val));
    }

    static result_t ConvertUserCallable(const UserCallable& val);

    bool m_byValue{};
    bool m_allowStringRef{};
};

template<typename CharT>
struct ValueRenderer;

template<>
struct ValueRenderer<char> : ValueRendererBase<char>
{
    ValueRenderer(std::string& os)
        : ValueRendererBase<char>::ValueRendererBase<char>(os)
    {
    }

    using ValueRendererBase<char>::operator ();
    void operator()(const std::wstring& str) const
    {
        (*m_os) += ConvertString<std::string>(str);
    }
    void operator()(const std::wstring_view& str) const
    {
        (*m_os) += ConvertString<std::string>(str);
    }
    void operator() (bool val) const
    {
        m_os->append(val ? "true" : "false");
    }
};

template<>
struct ValueRenderer<wchar_t> : ValueRendererBase<wchar_t>
{
    ValueRenderer(std::wstring& os)
        : ValueRendererBase<wchar_t>::ValueRendererBase<wchar_t>(os)
    {
    }

    using ValueRendererBase<wchar_t>::operator ();
    void operator()(const std::string& str) const
    {
        (*m_os) += ConvertString<std::wstring>(str);
    }
    void operator()(const std::string_view& str) const
    {
        (*m_os) += ConvertString<std::wstring>(str);
    }
    void operator() (bool val) const
    {
        // fmt::format_to(GetOs(), L"{}", (const wchar_t*)(val ? "true" : "false"));
        m_os->append(val ? L"true" : L"false");
    }
};

struct UnaryOperation : BaseVisitor<InternalValue>
{
    using BaseVisitor::operator ();

    UnaryOperation(UnaryExpression::Operation oper)
        : m_oper(oper)
    {
    }

    InternalValue operator() (int64_t val) const
    {
        InternalValue result;
        switch (m_oper)
        {
        case jinja2::UnaryExpression::LogicalNot:
            result = val ? false : true;
            break;
        case jinja2::UnaryExpression::UnaryPlus:
            result = +val;
            break;
        case jinja2::UnaryExpression::UnaryMinus:
            result = -val;
            break;
        }

        return result;
    }

    InternalValue operator() (double val) const
    {
        InternalValue result;
        switch (m_oper)
        {
        case jinja2::UnaryExpression::LogicalNot:
            result = fabs(val) > std::numeric_limits<double>::epsilon() ? false : true;
            break;
        case jinja2::UnaryExpression::UnaryPlus:
            result = +val;
            break;
        case jinja2::UnaryExpression::UnaryMinus:
            result = -val;
            break;
        }

        return result;
    }

    InternalValue operator() (bool val) const
    {
        InternalValue result;
        switch (m_oper)
        {
        case jinja2::UnaryExpression::LogicalNot:
            result = !val;
            break;
        default:
            break;
        }

        return result;
    }

    InternalValue operator() (const MapAdapter&) const
    {
        InternalValue result;
        switch (m_oper)
        {
        case jinja2::UnaryExpression::LogicalNot:
            result = true;
            break;
        default:
            break;
        }

        return result;
    }

    InternalValue operator() (const ListAdapter&) const
    {
        InternalValue result;
        switch (m_oper)
        {
        case jinja2::UnaryExpression::LogicalNot:
            result = true;
            break;
        default:
            break;
        }

        return result;
    }

    template<typename CharT>
    InternalValue operator() (const std::basic_string<CharT>& val) const
    {
        InternalValue result;
        switch (m_oper)
        {
        case jinja2::UnaryExpression::LogicalNot:
            result = val.empty();
            break;
        default:
            break;
        }

        return result;
    }

    template<typename CharT>
    InternalValue operator() (const std::basic_string_view<CharT>& val) const
    {
        InternalValue result;
        switch (m_oper)
        {
        case jinja2::UnaryExpression::LogicalNot:
            result = val.empty();
            break;
        default:
            break;
        }

        return result;
    }

    InternalValue operator() (const EmptyValue&) const
    {
        InternalValue result;
        switch (m_oper)
        {
        case jinja2::UnaryExpression::LogicalNot:
            result = true;
            break;
        default:
            break;
        }

        return result;
    }

    UnaryExpression::Operation m_oper;
};

struct BinaryMathOperation : BaseVisitor<>
{
    using BaseVisitor::operator ();
    using ResultType = InternalValue;
    // InternalValue operator() (int, int) const {return InternalValue();}

    bool AlmostEqual(double x, double y) const
    {
        return std::abs(x - y) <= std::numeric_limits<double>::epsilon() * std::abs(x + y) * 6
               || std::abs(x - y) < std::numeric_limits<double>::min();
    }

    BinaryMathOperation(BinaryExpression::Operation oper, BinaryExpression::CompareType compType = BinaryExpression::CaseSensitive)
        : m_oper(oper)
        , m_compType(compType)
    {
    }

    ResultType operator() (double left, double right) const
    {
        ResultType result = 0.0;
        switch (m_oper)
        {
        case jinja2::BinaryExpression::Plus:
            result = left + right;
            break;
        case jinja2::BinaryExpression::Minus:
            result = left - right;
            break;
        case jinja2::BinaryExpression::Mul:
            result = left * right;
            break;
        case jinja2::BinaryExpression::Div:
            result = left / right;
            break;
        case jinja2::BinaryExpression::DivReminder:
            result = std::remainder(left, right);
            break;
        case jinja2::BinaryExpression::DivInteger:
        {
            double val = left / right;
            result = val < 0 ? ceil(val) : floor(val);
            break;
        }
        case jinja2::BinaryExpression::Pow:
            result = pow(left, right);
            break;
        case jinja2::BinaryExpression::LogicalEq:
            result = AlmostEqual(left, right);
            break;
        case jinja2::BinaryExpression::LogicalNe:
            result = !AlmostEqual(left, right);
            break;
        case jinja2::BinaryExpression::LogicalGt:
            result = left > right;
            break;
        case jinja2::BinaryExpression::LogicalLt:
            result = left < right;
            break;
        case jinja2::BinaryExpression::LogicalGe:
            result = left > right || AlmostEqual(left, right);
            break;
        case jinja2::BinaryExpression::LogicalLe:
            result = left < right || AlmostEqual(left, right);
            break;
        default:
            break;
        }

        return result;
    }

    ResultType operator() (int64_t left, int64_t right) const
    {
        ResultType result;
        switch (m_oper)
        {
        case jinja2::BinaryExpression::Plus:
            result = left + right;
            break;
        case jinja2::BinaryExpression::Minus:
            result = left - right;
            break;
        case jinja2::BinaryExpression::Mul:
            result = left * right;
            break;
        case jinja2::BinaryExpression::DivInteger:
            result = left / right;
            break;
        case jinja2::BinaryExpression::Div:
        case jinja2::BinaryExpression::DivReminder:
        case jinja2::BinaryExpression::Pow:
            result = this->operator ()(static_cast<double>(left), static_cast<double>(right));
            break;
        case jinja2::BinaryExpression::LogicalEq:
            result = left == right;
            break;
        case jinja2::BinaryExpression::LogicalNe:
            result = left != right;
            break;
        case jinja2::BinaryExpression::LogicalGt:
            result = left > right;
            break;
        case jinja2::BinaryExpression::LogicalLt:
            result = left < right;
            break;
        case jinja2::BinaryExpression::LogicalGe:
            result = left >= right;
            break;
        case jinja2::BinaryExpression::LogicalLe:
            result = left <= right;
            break;
        default:
            break;
        }

        return result;
    }

    ResultType operator() (int64_t left, double right) const
    {
        return this->operator ()(static_cast<double>(left), static_cast<double>(right));
    }

    ResultType operator() (double left, int64_t right) const
    {
        return this->operator ()(static_cast<double>(left), static_cast<double>(right));
    }

    template<typename CharT>
    ResultType operator() (const std::basic_string<CharT> &left, const std::basic_string<CharT> &right) const
    {
        return ProcessStrings(std::basic_string_view<CharT>(left), std::basic_string_view<CharT>(right));
    }

    template<typename CharT1, typename CharT2>
    std::enable_if_t<!std::is_same<CharT1, CharT2>::value, ResultType> operator() (const std::basic_string<CharT1>& left, const std::basic_string<CharT2>& right) const
    {
        auto rightStr = ConvertString<std::basic_string<CharT1>>(right);
        return ProcessStrings(std::basic_string_view<CharT1>(left), std::basic_string_view<CharT1>(rightStr));
    }

    template<typename CharT>
    ResultType operator() (const std::basic_string_view<CharT> &left, const std::basic_string<CharT> &right) const
    {
        return ProcessStrings(left, std::basic_string_view<CharT>(right));
    }

    template<typename CharT1, typename CharT2>
    std::enable_if_t<!std::is_same<CharT1, CharT2>::value, ResultType> operator() (const std::basic_string_view<CharT1>& left, const std::basic_string<CharT2>& right) const
    {
        auto rightStr = ConvertString<std::basic_string<CharT1>>(right);
        return ProcessStrings(left, std::basic_string_view<CharT1>(rightStr));
    }

    template<typename CharT>
    ResultType operator() (const std::basic_string<CharT> &left, const std::basic_string_view<CharT> &right) const
    {
        return ProcessStrings(std::basic_string_view<CharT>(left), right);
    }

    template<typename CharT1, typename CharT2>
    std::enable_if_t<!std::is_same<CharT1, CharT2>::value, ResultType> operator() (const std::basic_string<CharT1>& left, const std::basic_string_view<CharT2>& right) const
    {
        auto rightStr = ConvertString<std::basic_string<CharT1>>(right);
        return ProcessStrings(std::basic_string_view<CharT1>(left), std::basic_string_view<CharT1>(rightStr));
    }

    template<typename CharT>
    ResultType operator() (const std::basic_string_view<CharT> &left, const std::basic_string_view<CharT> &right) const
    {
        return ProcessStrings(left, right);
    }

    template<typename CharT1, typename CharT2>
    std::enable_if_t<!std::is_same<CharT1, CharT2>::value, ResultType> operator() (const std::basic_string_view<CharT1>& left, const std::basic_string_view<CharT2>& right) const
    {
        auto rightStr = ConvertString<std::basic_string<CharT1>>(right);
        return ProcessStrings(left, std::basic_string_view<CharT1>(rightStr));
    }

    template<typename CharT>
    ResultType operator() (const std::basic_string<CharT> &left, int64_t right) const
    {
        return RepeatString(std::basic_string_view<CharT>(left), right);
    }

    template<typename CharT>
    ResultType operator() (const std::basic_string_view<CharT> &left, int64_t right) const
    {
        return RepeatString(left, right);
    }

    template<typename CharT>
    ResultType RepeatString(const std::basic_string_view<CharT>& left, const int64_t right) const
    {
        using string = std::basic_string<CharT>;
        ResultType result;

        if(m_oper == jinja2::BinaryExpression::Mul)
        {
            string str;
            for (int i = 0; i < right; ++i)
                str.append(left.begin(), left.end());
            result = TargetString(std::move(str));
        }
        return result;
    }

    template<typename CharT>
    ResultType ProcessStrings(const std::basic_string_view<CharT>& left, const std::basic_string_view<CharT>& right) const
    {
        using string = std::basic_string<CharT>;
        ResultType result;

        switch (m_oper)
        {
        case jinja2::BinaryExpression::Plus:
        {
            auto str = string(left.begin(), left.end());
            str.append(right.begin(), right.end());
            result = TargetString(std::move(str));
            break;
        }
        case jinja2::BinaryExpression::LogicalEq:
            result = m_compType == BinaryExpression::CaseSensitive ? left == right : boost::iequals(left, right);
            break;
        case jinja2::BinaryExpression::LogicalNe:
            result = m_compType == BinaryExpression::CaseSensitive ? left != right : !boost::iequals(left, right);
            break;
        case jinja2::BinaryExpression::LogicalGt:
            result = m_compType == BinaryExpression::CaseSensitive ? left > right : boost::lexicographical_compare(right, left, boost::algorithm::is_iless());
            break;
        case jinja2::BinaryExpression::LogicalLt:
            result = m_compType == BinaryExpression::CaseSensitive ? left < right : boost::lexicographical_compare(left, right, boost::algorithm::is_iless());
            break;
        case jinja2::BinaryExpression::LogicalGe:
            if (m_compType == BinaryExpression::CaseSensitive)
            {
                result = left >= right;
            }
            else
            {
                result = boost::iequals(left, right) ? true : boost::lexicographical_compare(right, left, boost::algorithm::is_iless());
            }
            break;
        case jinja2::BinaryExpression::LogicalLe:
            if (m_compType == BinaryExpression::CaseSensitive)
            {
                result = left <= right;
            }
            else
            {
                result = boost::iequals(left, right) ? true : boost::lexicographical_compare(left, right, boost::algorithm::is_iless());
            }
            break;
        default:
            break;
        }

        return result;
    }

    ResultType operator() (const KeyValuePair& left, const KeyValuePair& right) const
    {
        ResultType result;
        switch (m_oper)
        {
        case jinja2::BinaryExpression::LogicalEq:
            result = ConvertToBool(this->operator ()(left.key, right.key)) && ConvertToBool(Apply2<BinaryMathOperation>(left.value, right.value, BinaryExpression::LogicalEq, m_compType));
            break;
        case jinja2::BinaryExpression::LogicalNe:
            result = ConvertToBool(this->operator ()(left.key, right.key)) || ConvertToBool(Apply2<BinaryMathOperation>(left.value, right.value, BinaryExpression::LogicalNe, m_compType));
            break;
        default:
            break;
        }

        return result;
    }

    ResultType operator() (const ListAdapter& left, const ListAdapter& right) const
    {
        ResultType result;
        if (m_oper == jinja2::BinaryExpression::Plus)
        {
            InternalValueList values;
            values.reserve(left.GetSize().value_or(0) + right.GetSize().value_or(0));
            for (auto& v : left)
                values.push_back(v);
            for (auto& v : right)
                values.push_back(v);
            result = ListAdapter::CreateAdapter(std::move(values));
        }

        return result;
    }

    ResultType operator() (const ListAdapter& left, int64_t right) const
    {
        ResultType result;
        if (right >= 0 && m_oper == jinja2::BinaryExpression::Mul)
        {
            InternalValueList values;
            values.reserve(left.GetSize().value_or(0));
            for (auto& v : left)
                values.push_back(v);
            auto listSize = values.size() * right;
            result = ListAdapter::CreateAdapter(static_cast<size_t>(listSize),
                                                [size = values.size(), values = std::move(values)](size_t idx) { return values[idx % size]; });
        }

        return result;
    }

    ResultType operator() (bool left, bool right) const
    {
        ResultType result;
        switch (m_oper)
        {
        case jinja2::BinaryExpression::LogicalEq:
            result = left == right;
            break;
        case jinja2::BinaryExpression::LogicalNe:
            result = left != right;
            break;
        case jinja2::BinaryExpression::LogicalLt:
            result = (left ? 1 : 0) < (right ? 1 : 0);
            break;
        default:
            break;
        }

        return result;
    }

    ResultType operator() (EmptyValue, EmptyValue) const
    {
        ResultType result;
        switch (m_oper)
        {
        case jinja2::BinaryExpression::LogicalEq:
            result = true;
            break;
        case jinja2::BinaryExpression::LogicalNe:
            result = false;
            break;
        default:
            break;
        }

        return result;
    }

    template<typename T>
    ResultType operator() (EmptyValue, T&&) const
    {
        ResultType result;
        switch (m_oper)
        {
        case jinja2::BinaryExpression::LogicalEq:
            result = false;
            break;
        case jinja2::BinaryExpression::LogicalNe:
            result = true;
            break;
        default:
            break;
        }

        return result;
    }

    template<typename T>
    ResultType operator() (T&&, EmptyValue) const
    {
        ResultType result;
        switch (m_oper)
        {
        case jinja2::BinaryExpression::LogicalEq:
            result = false;
            break;
        case jinja2::BinaryExpression::LogicalNe:
            result = true;
            break;
        default:
            break;
        }

        return result;
    }

    BinaryExpression::Operation m_oper;
    BinaryExpression::CompareType m_compType;
};

struct BooleanEvaluator : BaseVisitor<bool>
{
    using BaseVisitor::operator ();

    bool operator() (int64_t val) const
    {
        return val != 0;
    }

    bool operator() (double val) const
    {
        return fabs(val) < std::numeric_limits<double>::epsilon();
    }

    bool operator() (bool val) const
    {
        return val;
    }

    template<typename CharT>
    bool operator()(const std::basic_string<CharT>& str) const
    {
        return !str.empty();
    }

    template<typename CharT>
    bool operator()(const std::basic_string_view<CharT>& str) const
    {
        return !str.empty();
    }

    bool operator() (const MapAdapter& val) const
    {
        return val.GetSize() != 0ULL;
    }

    bool operator() (const ListAdapter& val) const
    {
        return val.GetSize() != 0ULL;
    }

    bool operator() (const EmptyValue&) const
    {
        return false;
    }
};

template<typename TargetType>
struct NumberEvaluator
{
    NumberEvaluator(TargetType def = 0) : m_def(def)
    {}

    TargetType operator ()(int64_t val) const
    {
        return static_cast<TargetType>(val);
    }
    TargetType operator ()(double val) const
    {
        return static_cast<TargetType>(val);
    }
    TargetType operator ()(bool val) const
    {
        return static_cast<TargetType>(val);
    }
    template<typename U>
    TargetType operator()(U&&) const
    {
        return m_def;
    }

    TargetType m_def;
};

using IntegerEvaluator = NumberEvaluator<int64_t>;
using DoubleEvaluator = NumberEvaluator<double>;


struct StringJoiner : BaseVisitor<TargetString>
{
    using BaseVisitor::operator ();

    template<typename CharT>
    TargetString operator() (EmptyValue, const std::basic_string<CharT>& str) const
    {
        return str;
    }

    template<typename CharT>
    TargetString operator() (EmptyValue, const std::basic_string_view<CharT>& str) const
    {
        return std::basic_string<CharT>(str.begin(), str.end());
    }

    template<typename CharT>
    TargetString operator() (const std::basic_string<CharT>& left, const std::basic_string<CharT>& right) const
    {
        return left + right;
    }

    template<typename CharT1, typename CharT2>
    std::enable_if_t<!std::is_same<CharT1, CharT2>::value, TargetString> operator() (const std::basic_string<CharT1>& left, const std::basic_string<CharT2>& right) const
    {
        return left + ConvertString<std::basic_string<CharT1>>(right);
    }

    template<typename CharT>
    TargetString operator() (std::basic_string<CharT> left, const std::basic_string_view<CharT>& right) const
    {
        left.append(right.begin(), right.end());
        return std::move(left);
    }

    template<typename CharT1, typename CharT2>
    std::enable_if_t<!std::is_same<CharT1, CharT2>::value, TargetString> operator() (std::basic_string<CharT1> left, const std::basic_string_view<CharT2>& right) const
    {
        auto r = ConvertString<std::basic_string<CharT1>>(right);
        left.append(r.begin(), r.end());
        return std::move(left);
    }
};

template<typename Fn>
struct StringConverterImpl : public BaseVisitor<decltype(std::declval<Fn>()(std::declval<std::string_view>()))>
{
    using R = decltype(std::declval<Fn>()(std::string_view()));
    using BaseVisitor<R>::operator ();

    StringConverterImpl(const Fn& fn) : m_fn(fn) {}

    template<typename CharT>
    R operator()(const std::basic_string<CharT>& str) const
    {
        return m_fn(std::basic_string_view<CharT>(str));
    }

    template<typename CharT>
    R operator()(const std::basic_string_view<CharT>& str) const
    {
        return m_fn(str);
    }

    const Fn& m_fn;
};

template<typename CharT>
struct SameStringGetter : public visitors::BaseVisitor<nonstd::expected<void, std::basic_string<CharT>>>
{
    using ResultString = std::basic_string<CharT>;
    using OtherString = std::conditional_t<std::is_same<CharT, char>::value, std::wstring, std::string>;
    using ResultStringView = std::basic_string_view<CharT>;
    using OtherStringView = std::conditional_t<std::is_same<CharT, char>::value, std::wstring_view, std::string_view>;
    using Result = nonstd::expected<void, ResultString>;
    using BaseVisitor<Result>::operator ();

    Result operator()(const ResultString& str) const
    {
        return nonstd::make_unexpected(str);
    }

    Result operator()(const ResultStringView& str) const
    {
        return nonstd::make_unexpected(ResultString(str.begin(), str.end()));
    }

    Result operator()(const OtherString& str) const
    {
        return nonstd::make_unexpected(ConvertString<ResultString>(str));
    }

    Result operator()(const OtherStringView& str) const
    {
        return nonstd::make_unexpected(ConvertString<ResultString>(str));
    }
};

} // namespace visitors

inline bool ConvertToBool(const InternalValue& val)
{
    return Apply<visitors::BooleanEvaluator>(val);
}

inline int64_t ConvertToInt(const InternalValue& val, int64_t def = 0)
{
    return Apply<visitors::IntegerEvaluator>(val, def);
}

inline double ConvertToDouble(const InternalValue& val, double def = 0)
{
    return Apply<visitors::DoubleEvaluator>(val, def);
}

template<template<typename> class Cvt = visitors::StringConverterImpl, typename Fn>
auto ApplyStringConverter(const InternalValue& str, Fn&& fn)
{
    return Apply<Cvt<Fn>>(str, std::forward<Fn>(fn));
}

template<typename CharT>
auto GetAsSameString(const std::basic_string<CharT>&, const InternalValue& val)
{
    using Result = std::optional<std::basic_string<CharT>>;
    auto result = Apply<visitors::SameStringGetter<CharT>>(val);
    if (!result)
        return Result(result.error());

    return Result();
}

template<typename CharT>
auto GetAsSameString(const std::basic_string_view<CharT>&, const InternalValue& val)
{
    using Result = std::optional<std::basic_string<CharT>>;
    auto result = Apply<visitors::SameStringGetter<CharT>>(val);
    if (!result)
        return Result(result.error());

    return Result();
}

inline bool operator==(const InternalValueData& lhs, const InternalValueData& rhs)
{
    InternalValue cmpRes;
    cmpRes = Apply2<visitors::BinaryMathOperation>(lhs, rhs, BinaryExpression::LogicalEq);
    return ConvertToBool(cmpRes);
}

inline bool operator!=(const InternalValueData& lhs, const InternalValueData& rhs)
{
    return !(lhs == rhs);
}

} // namespace jinja2

#endif // JINJA2CPP_SRC_VALUE_VISITORS_H
