#include "filters.h"

#include "binding/rapid_json_serializer.h"
#include "generic_adapters.h"
#include "out_stream.h"
#include "testers.h"
#include "value_helpers.h"
#include "value_visitors.h"

#include <algorithm>
#include <numeric>
#include <random>
#include <sstream>
#include <string>

using namespace std::string_literals;

namespace jinja2
{

template<typename F>
struct FilterFactory
{
    static FilterPtr Create(FilterParams params) { return std::make_shared<F>(std::move(params)); }

    template<typename... Args>
    static ExpressionFilter::FilterFactoryFn MakeCreator(Args&&... args)
    {
        return [args...](FilterParams params) { return std::make_shared<F>(std::move(params), args...); };
    }
};

std::unordered_map<std::string, ExpressionFilter::FilterFactoryFn> s_filters = {
    { "abs", FilterFactory<filters::ValueConverter>::MakeCreator(filters::ValueConverter::AbsMode) },
    { "applymacro", &FilterFactory<filters::ApplyMacro>::Create },
    { "attr", &FilterFactory<filters::Attribute>::Create },
    { "batch", FilterFactory<filters::Slice>::MakeCreator(filters::Slice::BatchMode) },
    { "camelize", FilterFactory<filters::StringConverter>::MakeCreator(filters::StringConverter::CamelMode) },
    { "capitalize", FilterFactory<filters::StringConverter>::MakeCreator(filters::StringConverter::CapitalMode) },
    { "center", FilterFactory<filters::StringConverter>::MakeCreator(filters::StringConverter::CenterMode) },
    { "default", &FilterFactory<filters::Default>::Create },
    { "d", &FilterFactory<filters::Default>::Create },
    { "dictsort", &FilterFactory<filters::DictSort>::Create },
    { "escape", FilterFactory<filters::StringConverter>::MakeCreator(filters::StringConverter::EscapeHtmlMode) },
    { "escapecpp", FilterFactory<filters::StringConverter>::MakeCreator(filters::StringConverter::EscapeCppMode) },
    { "first", FilterFactory<filters::SequenceAccessor>::MakeCreator(filters::SequenceAccessor::FirstItemMode) },
    { "float", FilterFactory<filters::ValueConverter>::MakeCreator(filters::ValueConverter::ToFloatMode) },
    { "format", FilterFactory<filters::StringFormat>::Create },
    { "groupby", &FilterFactory<filters::GroupBy>::Create },
    { "int", FilterFactory<filters::ValueConverter>::MakeCreator(filters::ValueConverter::ToIntMode) },
    { "join", &FilterFactory<filters::Join>::Create },
    { "last", FilterFactory<filters::SequenceAccessor>::MakeCreator(filters::SequenceAccessor::LastItemMode) },
    { "length", FilterFactory<filters::SequenceAccessor>::MakeCreator(filters::SequenceAccessor::LengthMode) },
    { "list", FilterFactory<filters::ValueConverter>::MakeCreator(filters::ValueConverter::ToListMode) },
    { "lower", FilterFactory<filters::StringConverter>::MakeCreator(filters::StringConverter::LowerMode) },
    { "map", &FilterFactory<filters::Map>::Create },
    { "max", FilterFactory<filters::SequenceAccessor>::MakeCreator(filters::SequenceAccessor::MaxItemMode) },
    { "min", FilterFactory<filters::SequenceAccessor>::MakeCreator(filters::SequenceAccessor::MinItemMode) },
    { "pprint", &FilterFactory<filters::PrettyPrint>::Create },
    { "random", FilterFactory<filters::SequenceAccessor>::MakeCreator(filters::SequenceAccessor::RandomMode) },
    { "reject", FilterFactory<filters::Tester>::MakeCreator(filters::Tester::RejectMode) },
    { "rejectattr", FilterFactory<filters::Tester>::MakeCreator(filters::Tester::RejectAttrMode) },
    { "replace", FilterFactory<filters::StringConverter>::MakeCreator(filters::StringConverter::ReplaceMode) },
    { "round", FilterFactory<filters::ValueConverter>::MakeCreator(filters::ValueConverter::RoundMode) },
    { "reverse", FilterFactory<filters::SequenceAccessor>::MakeCreator(filters::SequenceAccessor::ReverseMode) },
    { "select", FilterFactory<filters::Tester>::MakeCreator(filters::Tester::SelectMode) },
    { "selectattr", FilterFactory<filters::Tester>::MakeCreator(filters::Tester::SelectAttrMode) },
    { "slice", FilterFactory<filters::Slice>::MakeCreator(filters::Slice::SliceMode) },
    { "sort", &FilterFactory<filters::Sort>::Create },
    { "striptags", FilterFactory<filters::StringConverter>::MakeCreator(filters::StringConverter::StriptagsMode) },
    { "sum", FilterFactory<filters::SequenceAccessor>::MakeCreator(filters::SequenceAccessor::SumItemsMode) },
    { "title", FilterFactory<filters::StringConverter>::MakeCreator(filters::StringConverter::TitleMode) },
    { "tojson", FilterFactory<filters::Serialize>::MakeCreator(filters::Serialize::JsonMode) },
    { "toxml", FilterFactory<filters::Serialize>::MakeCreator(filters::Serialize::XmlMode) },
    { "toyaml", FilterFactory<filters::Serialize>::MakeCreator(filters::Serialize::YamlMode) },
    { "trim", FilterFactory<filters::StringConverter>::MakeCreator(filters::StringConverter::TrimMode) },
    { "truncate", FilterFactory<filters::StringConverter>::MakeCreator(filters::StringConverter::TruncateMode) },
    { "unique", FilterFactory<filters::SequenceAccessor>::MakeCreator(filters::SequenceAccessor::UniqueItemsMode) },
    { "upper", FilterFactory<filters::StringConverter>::MakeCreator(filters::StringConverter::UpperMode) },
    { "urlencode", FilterFactory<filters::StringConverter>::MakeCreator(filters::StringConverter::UrlEncodeMode) },
    { "wordcount", FilterFactory<filters::StringConverter>::MakeCreator(filters::StringConverter::WordCountMode) },
    { "wordwrap", FilterFactory<filters::StringConverter>::MakeCreator(filters::StringConverter::WordWrapMode) },
    { "underscorize", FilterFactory<filters::StringConverter>::MakeCreator(filters::StringConverter::UnderscoreMode) },
    { "xmlattr", &FilterFactory<filters::XmlAttrFilter>::Create }
};

extern FilterPtr CreateFilter(std::string filterName, CallParamsInfo params)
{
    auto p = s_filters.find(filterName);
    if (p == s_filters.end())
        return std::make_shared<filters::UserDefinedFilter>(std::move(filterName), std::move(params));

    return p->second(std::move(params));
}

namespace filters
{

Join::Join(FilterParams params)
{
    ParseParams({ { "d", false, std::string() }, { "attribute" } }, params);
}

InternalValue Join::Filter(const InternalValue& baseVal, RenderContext& context)
{
    InternalValue attrName = GetArgumentValue("attribute", context);

    bool isConverted = false;
    ListAdapter values = ConvertToList(baseVal, attrName, isConverted);

    if (!isConverted)
        return InternalValue();

    bool isFirst = true;
    InternalValue result;
    InternalValue delimiter = m_args["d"]->Evaluate(context);
    for (const InternalValue& val : values)
    {
        if (isFirst)
            isFirst = false;
        else
            result = Apply2<visitors::StringJoiner>(result, delimiter);

        result = Apply2<visitors::StringJoiner>(result, val);
    }

    return result;
}

Sort::Sort(FilterParams params)
{
    ParseParams({ { "reverse", false, InternalValue(false) }, { "case_sensitive", false, InternalValue(false) }, { "attribute", false } }, params);
}

InternalValue Sort::Filter(const InternalValue& baseVal, RenderContext& context)
{
    InternalValue attrName = GetArgumentValue("attribute", context);
    InternalValue isReverseVal = GetArgumentValue("reverse", context, InternalValue(false));
    InternalValue isCsVal = GetArgumentValue("case_sensitive", context, InternalValue(false));

    bool isConverted = false;
    ListAdapter origValues = ConvertToList(baseVal, isConverted);
    if (!isConverted)
        return InternalValue();
    InternalValueList values = origValues.ToValueList();

    BinaryExpression::Operation oper = ConvertToBool(isReverseVal) ? BinaryExpression::LogicalGt : BinaryExpression::LogicalLt;
    BinaryExpression::CompareType compType = ConvertToBool(isCsVal) ? BinaryExpression::CaseSensitive : BinaryExpression::CaseInsensitive;

    std::sort(values.begin(), values.end(), [&attrName, oper, compType, &context](auto& val1, auto& val2) {
        InternalValue cmpRes;
        if (IsEmpty(attrName))
            cmpRes = Apply2<visitors::BinaryMathOperation>(val1, val2, oper, compType);
        else
            cmpRes = Apply2<visitors::BinaryMathOperation>(Subscript(val1, attrName, &context), Subscript(val2, attrName, &context), oper, compType);

        return ConvertToBool(cmpRes);
    });

    return ListAdapter::CreateAdapter(std::move(values));
}

Attribute::Attribute(FilterParams params)
{
    ParseParams({ { "name", true }, { "default", false } }, params);
}

InternalValue Attribute::Filter(const InternalValue& baseVal, RenderContext& context)
{
    const auto attrNameVal = GetArgumentValue("name", context);
    const auto result = Subscript(baseVal, attrNameVal, &context);
    if (result.IsEmpty())
        return GetArgumentValue("default", context);
    return result;
}

Default::Default(FilterParams params)
{
    ParseParams({ { "default_value", false, InternalValue(""s) }, { "boolean", false, InternalValue(false) } }, params);
}

InternalValue Default::Filter(const InternalValue& baseVal, RenderContext& context)
{
    InternalValue defaultVal = GetArgumentValue("default_value", context);
    InternalValue conditionResult = GetArgumentValue("boolean", context);

    if (IsEmpty(baseVal))
        return defaultVal;

    if (ConvertToBool(conditionResult) && !ConvertToBool(baseVal))
        return defaultVal;

    return baseVal;
}

DictSort::DictSort(FilterParams params)
{
    ParseParams({ { "case_sensitive", false }, { "by", false, "key"s }, { "reverse", false } }, params);
}

InternalValue DictSort::Filter(const InternalValue& baseVal, RenderContext& context)
{
    const MapAdapter* map = GetIf<MapAdapter>(&baseVal);
    if (map == nullptr)
        return InternalValue();

    InternalValue isReverseVal = GetArgumentValue("reverse", context);
    InternalValue isCsVal = GetArgumentValue("case_sensitive", context);
    InternalValue byVal = GetArgumentValue("by", context);

    bool (*comparator)(const KeyValuePair& left, const KeyValuePair& right);

    if (AsString(byVal) == "key") // Sort by key
    {
        if (ConvertToBool(isCsVal))
        {
            comparator = [](const KeyValuePair& left, const KeyValuePair& right) { return left.key < right.key; };
        }
        else
        {
            comparator = [](const KeyValuePair& left, const KeyValuePair& right) {
                return boost::lexicographical_compare(left.key, right.key, boost::algorithm::is_iless());
            };
        }
    }
    else if (AsString(byVal) == "value")
    {
        if (ConvertToBool(isCsVal))
        {
            comparator = [](const KeyValuePair& left, const KeyValuePair& right) {
                return ConvertToBool(
                  Apply2<visitors::BinaryMathOperation>(left.value, right.value, BinaryExpression::LogicalLt, BinaryExpression::CaseSensitive));
            };
        }
        else
        {
            comparator = [](const KeyValuePair& left, const KeyValuePair& right) {
                return ConvertToBool(
                  Apply2<visitors::BinaryMathOperation>(left.value, right.value, BinaryExpression::LogicalLt, BinaryExpression::CaseInsensitive));
            };
        }
    }
    else
        return InternalValue();

    std::vector<KeyValuePair> tempVector;
    tempVector.reserve(map->GetSize());
    for (auto& key : map->GetKeys())
    {
        auto val = map->GetValueByName(key);
        tempVector.push_back(KeyValuePair{ key, val });
    }

    if (ConvertToBool(isReverseVal))
        std::sort(tempVector.begin(), tempVector.end(), [comparator](auto& l, auto& r) { return comparator(r, l); });
    else
        std::sort(tempVector.begin(), tempVector.end(), [comparator](auto& l, auto& r) { return comparator(l, r); });

    InternalValueList resultList;
    for (auto& tmpVal : tempVector)
    {
        auto resultVal = InternalValue(std::move(tmpVal));
        if (baseVal.ShouldExtendLifetime())
            resultVal.SetParentData(baseVal);
        resultList.push_back(std::move(resultVal));
    }

    return InternalValue(ListAdapter::CreateAdapter(std::move(resultList)));
}

GroupBy::GroupBy(FilterParams params)
{
    ParseParams({ { "attribute", true } }, params);
}

InternalValue GroupBy::Filter(const InternalValue& baseVal, RenderContext& context)
{
    bool isConverted = false;
    ListAdapter list = ConvertToList(baseVal, isConverted);

    if (!isConverted)
        return InternalValue();

    InternalValue attrName = GetArgumentValue("attribute", context);

    auto equalComparator = [](auto& val1, auto& val2) {
        InternalValue cmpRes = Apply2<visitors::BinaryMathOperation>(val1, val2, BinaryExpression::LogicalEq, BinaryExpression::CaseSensitive);

        return ConvertToBool(cmpRes);
    };

    struct GroupInfo
    {
        InternalValue grouper;
        InternalValueList items;
    };

    std::vector<GroupInfo> groups;

    for (auto& item : list)
    {
        auto attr = Subscript(item, attrName, &context);
        auto p = std::find_if(groups.begin(), groups.end(), [&equalComparator, &attr](auto& i) { return equalComparator(i.grouper, attr); });
        if (p == groups.end())
            groups.push_back(GroupInfo{ attr, { item } });
        else
            p->items.push_back(item);
    }

    InternalValueList result;
    for (auto& g : groups)
    {
        InternalValueMap groupItem{ { "grouper", std::move(g.grouper) }, { "list", ListAdapter::CreateAdapter(std::move(g.items)) } };
        result.push_back(CreateMapAdapter(std::move(groupItem)));
    }

    return ListAdapter::CreateAdapter(std::move(result));
}

ApplyMacro::ApplyMacro(FilterParams params)
{
    ParseParams({ { "macro", true } }, params);
    m_mappingParams.kwParams = m_args.extraKwArgs;
    m_mappingParams.posParams = m_args.extraPosArgs;
}

InternalValue ApplyMacro::Filter(const InternalValue& baseVal, RenderContext& context)
{
    InternalValue macroName = GetArgumentValue("macro", context);
    if (IsEmpty(macroName))
        return InternalValue();

    bool macroFound = false;
    auto macroValPtr = context.FindValue(AsString(macroName), macroFound);
    if (!macroFound)
        return InternalValue();

    const Callable* callable = GetIf<Callable>(&macroValPtr->second);
    if (callable == nullptr || callable->GetKind() != Callable::Macro)
        return InternalValue();

    CallParams tmpCallParams = helpers::EvaluateCallParams(m_mappingParams, context);
    CallParams callParams;
    callParams.kwParams = std::move(tmpCallParams.kwParams);
    callParams.posParams.reserve(tmpCallParams.posParams.size() + 1);
    callParams.posParams.push_back(baseVal);
    for (auto& p : tmpCallParams.posParams)
        callParams.posParams.push_back(std::move(p));

    InternalValue result;
    if (callable->GetType() == Callable::Type::Expression)
    {
        result = callable->GetExpressionCallable()(callParams, context);
    }
    else
    {
        TargetString resultStr;
        auto stream = context.GetRendererCallback()->GetStreamOnString(resultStr);
        callable->GetStatementCallable()(callParams, stream, context);
        result = std::move(resultStr);
    }

    return result;
}

Map::Map(FilterParams params)
{
    ParseParams({ { "filter", true } }, MakeParams(std::move(params)));
    m_mappingParams.kwParams = m_args.extraKwArgs;
    m_mappingParams.posParams = m_args.extraPosArgs;
}

FilterParams Map::MakeParams(FilterParams params)
{
    if (!params.posParams.empty() || params.kwParams.empty() || params.kwParams.size() > 2)
    {
        return params;
    }

    const auto attributeIt = params.kwParams.find("attribute");
    if (attributeIt == params.kwParams.cend())
    {
        return params;
    }

    FilterParams result;
    result.kwParams["name"] = attributeIt->second;
    result.kwParams["filter"] = std::make_shared<ConstantExpression>("attr"s);

    const auto defaultIt = params.kwParams.find("default");
    if (defaultIt != params.kwParams.cend())
        result.kwParams["default"] = defaultIt->second;

    return result;
}

InternalValue Map::Filter(const InternalValue& baseVal, RenderContext& context)
{
    InternalValue filterName = GetArgumentValue("filter", context);
    if (IsEmpty(filterName))
        return InternalValue();

    auto filter = CreateFilter(AsString(filterName), m_mappingParams);
    if (!filter)
        return InternalValue();

    bool isConverted = false;
    auto list = ConvertToList(baseVal, isConverted);
    if (!isConverted)
        return InternalValue();

    InternalValueList resultList;
    resultList.reserve(list.GetSize().value_or(0));
    std::transform(list.begin(), list.end(), std::back_inserter(resultList), [filter, &context](auto& val) { return filter->Filter(val, context); });

    return ListAdapter::CreateAdapter(std::move(resultList));
}
Random::Random(FilterParams params) {}

InternalValue Random::Filter(const InternalValue&, RenderContext&)
{
    return InternalValue();
}

SequenceAccessor::SequenceAccessor(FilterParams params, SequenceAccessor::Mode mode)
    : m_mode(mode)
{
    switch (mode)
    {
        case FirstItemMode:
            break;
        case LastItemMode:
            break;
        case LengthMode:
            break;
        case MaxItemMode:
        case MinItemMode:
            ParseParams({ { "case_sensitive", false, InternalValue(false) }, { "attribute", false } }, params);
            break;
        case RandomMode:
        case ReverseMode:
            break;
        case SumItemsMode:
            ParseParams({ { "attribute", false }, { "start", false } }, params);
            break;
        case UniqueItemsMode:
            ParseParams({ { "attribute", false } }, params);
            break;
    }
}

InternalValue SequenceAccessor::Filter(const InternalValue& baseVal, RenderContext& context)
{
    InternalValue result;

    bool isConverted = false;
    ListAdapter list = ConvertToList(baseVal, isConverted);

    if (!isConverted)
        return result;

    InternalValue attrName = GetArgumentValue("attribute", context);
    InternalValue isCsVal = GetArgumentValue("case_sensitive", context, InternalValue(false));

    BinaryExpression::CompareType compType = ConvertToBool(isCsVal) ? BinaryExpression::CaseSensitive : BinaryExpression::CaseInsensitive;

    auto lessComparator = [&attrName, &compType, &context](auto& val1, auto& val2) {
        InternalValue cmpRes;

        if (IsEmpty(attrName))
            cmpRes = Apply2<visitors::BinaryMathOperation>(val1, val2, BinaryExpression::LogicalLt, compType);
        else
            cmpRes = Apply2<visitors::BinaryMathOperation>(
              Subscript(val1, attrName, &context), Subscript(val2, attrName, &context), BinaryExpression::LogicalLt, compType);

        return ConvertToBool(cmpRes);
    };

    const auto& listSize = list.GetSize();

    switch (m_mode)
    {
        case FirstItemMode:
            if (listSize)
                result = list.GetValueByIndex(0);
            else
            {
                auto it = list.begin();
                if (it != list.end())
                    result = *it;
            }
            break;
        case LastItemMode:
            if (listSize)
                result = list.GetValueByIndex(listSize.value() - 1);
            else
            {
                auto it = list.begin();
                auto end = list.end();
                for (; it != end; ++it)
                    result = *it;
            }
            break;
        case LengthMode:
            if (listSize)
                result = static_cast<int64_t>(listSize.value());
            else
                result = static_cast<int64_t>(std::distance(list.begin(), list.end()));
            break;
        case RandomMode:
        {
            std::random_device rd;
            std::mt19937 gen(rd());
            if (listSize)
            {
                std::uniform_int_distribution<> dis(0, static_cast<int>(listSize.value()) - 1);
                result = list.GetValueByIndex(dis(gen));
            }
            else
            {
                auto it = list.begin();
                auto end = list.end();
                size_t count = 0;
                for (; it != end; ++it, ++count)
                {
                    bool doCopy = count == 0 || std::uniform_int_distribution<size_t>(0, count)(gen) == 0;
                    if (doCopy)
                        result = *it;
                }
            }
            break;
        }
        case MaxItemMode:
        {
            auto b = list.begin();
            auto e = list.end();
            auto p = std::max_element(list.begin(), list.end(), lessComparator);
            result = p != e ? *p : InternalValue();
            break;
        }
        case MinItemMode:
        {
            auto b = list.begin();
            auto e = list.end();
            auto p = std::min_element(b, e, lessComparator);
            result = p != e ? *p : InternalValue();
            break;
        }
        case ReverseMode:
        {
            if (listSize)
            {
                auto size = listSize.value();
                InternalValueList resultList(size);
                for (std::size_t n = 0; n < size; ++n)
                    resultList[size - n - 1] = list.GetValueByIndex(n);
                result = ListAdapter::CreateAdapter(std::move(resultList));
            }
            else
            {
                InternalValueList resultList;
                auto it = list.begin();
                auto end = list.end();
                for (; it != end; ++it)
                    resultList.push_back(*it);

                std::reverse(resultList.begin(), resultList.end());
                result = ListAdapter::CreateAdapter(std::move(resultList));
            }

            break;
        }
        case SumItemsMode:
        {
            ListAdapter l1;
            ListAdapter* actualList;
            if (IsEmpty(attrName))
            {
                actualList = &list;
            }
            else
            {
                l1 = list.ToSubscriptedList(attrName, true);
                actualList = &l1;
            }
            InternalValue start = GetArgumentValue("start", context);
            InternalValue resultVal = std::accumulate(actualList->begin(), actualList->end(), start, [](const InternalValue& cur, const InternalValue& val) {
                if (IsEmpty(cur))
                    return val;

                return Apply2<visitors::BinaryMathOperation>(cur, val, BinaryExpression::Plus);
            });

            result = std::move(resultVal);
            break;
        }
        case UniqueItemsMode:
        {
            InternalValueList resultList;

            struct Item
            {
                InternalValue val;
                int64_t idx;
            };
            std::vector<Item> items;

            int idx = 0;
            for (auto& v : list)
                items.push_back(Item{ IsEmpty(attrName) ? v : Subscript(v, attrName, &context), idx++ });

            std::stable_sort(items.begin(), items.end(), [&compType](auto& i1, auto& i2) {
                auto cmpRes = Apply2<visitors::BinaryMathOperation>(i1.val, i2.val, BinaryExpression::LogicalLt, compType);

                return ConvertToBool(cmpRes);
            });

            auto end = std::unique(items.begin(), items.end(), [&compType](auto& i1, auto& i2) {
                auto cmpRes = Apply2<visitors::BinaryMathOperation>(i1.val, i2.val, BinaryExpression::LogicalEq, compType);

                return ConvertToBool(cmpRes);
            });
            items.erase(end, items.end());

            std::stable_sort(items.begin(), items.end(), [](auto& i1, auto& i2) { return i1.idx < i2.idx; });

            for (auto& i : items)
                resultList.push_back(list.GetValueByIndex(i.idx));

            result = ListAdapter::CreateAdapter(std::move(resultList));
            break;
        }
    }

    return result;
}
Slice::Slice(FilterParams params, Slice::Mode mode)
    : m_mode{ mode }
{
    if (m_mode == BatchMode)
        ParseParams({ { "linecount"s, true }, { "fill_with"s, false } }, params);
    else
        ParseParams({ { "slices"s, true }, { "fill_with"s, false } }, params);
}

InternalValue Slice::Filter(const InternalValue& baseVal, RenderContext& context)
{
    if (m_mode == BatchMode)
        return Batch(baseVal, context);

    InternalValue result;

    bool isConverted = false;
    ListAdapter list = ConvertToList(baseVal, isConverted);

    if (!isConverted)
        return result;

    InternalValue sliceLengthValue = GetArgumentValue("slices", context);
    int64_t sliceLength = ConvertToInt(sliceLengthValue);
    InternalValue fillWith = GetArgumentValue("fill_with", context);

    InternalValueList resultList;
    InternalValueList sublist;
    int sublistItemIndex = 0;
    for (auto& item : list)
    {
        if (sublistItemIndex == 0)
            sublist.clear();
        if (sublistItemIndex == sliceLength)
        {
            resultList.push_back(ListAdapter::CreateAdapter(std::move(sublist)));
            sublist.clear();
            sublistItemIndex %= sliceLength;
        }
        sublist.push_back(item);
        ++sublistItemIndex;
    }
    if (!IsEmpty(fillWith))
    {
        while (sublistItemIndex++ < sliceLength)
            sublist.push_back(fillWith);
    }
    if (sublistItemIndex > 0)
        resultList.push_back(ListAdapter::CreateAdapter(std::move(sublist)));

    return InternalValue(ListAdapter::CreateAdapter(std::move(resultList)));
}

InternalValue Slice::Batch(const InternalValue& baseVal, RenderContext& context)
{
    auto linecount_value = ConvertToInt(GetArgumentValue("linecount", context));
    InternalValue fillWith = GetArgumentValue("fill_with", context);

    if (linecount_value <= 0)
        return InternalValue();
    auto linecount = static_cast<std::size_t>(linecount_value);

    bool isConverted = false;
    auto list = ConvertToList(baseVal, isConverted);
    if (!isConverted)
        return InternalValue();

    auto elementsCount = list.GetSize().value_or(0);
    if (!elementsCount)
        return InternalValue();

    InternalValueList resultList;
    resultList.reserve(linecount);

    const auto remainder = elementsCount % linecount;
    const auto columns = elementsCount / linecount + (remainder > 0 ? 1 : 0);
    for (std::size_t line = 0, idx = 0; line < linecount; ++line)
    {
        const auto elems = columns - (remainder && line >= remainder ? 1 : 0);
        InternalValueList row;
        row.reserve(columns);
        std::fill_n(std::back_inserter(row), columns, fillWith);

        for (std::size_t column = 0; column < elems; ++column)
            row[column] = list.GetValueByIndex(idx++);

        resultList.push_back(ListAdapter::CreateAdapter(std::move(row)));
    }
    return ListAdapter::CreateAdapter(std::move(resultList));
}

StringFormat::StringFormat(FilterParams params)
{
    ParseParams({}, params);
    m_params.kwParams = std::move(m_args.extraKwArgs);
    m_params.posParams = std::move(m_args.extraPosArgs);
}

Tester::Tester(FilterParams params, Tester::Mode mode)
    : m_mode(mode)
{
    FilterParams newParams;

    if ((mode == RejectMode || mode == SelectMode) && params.kwParams.empty() && params.posParams.empty())
    {
        m_noParams = true;
        return;
    }

    if (mode == RejectMode || mode == SelectMode)
        ParseParams({ { "tester", false } }, params);
    else
        ParseParams({ { "attribute", true }, { "tester", false } }, params);

    m_testingParams.kwParams = std::move(m_args.extraKwArgs);
    m_testingParams.posParams = std::move(m_args.extraPosArgs);
}

InternalValue Tester::Filter(const InternalValue& baseVal, RenderContext& context)
{
    InternalValue testerName = GetArgumentValue("tester", context);
    InternalValue attrName = GetArgumentValue("attribute", context);

    TesterPtr tester;

    if (!IsEmpty(testerName))
    {
        tester = CreateTester(AsString(testerName), m_testingParams);

        if (!tester)
            return InternalValue();
    }

    bool isConverted = false;
    auto list = ConvertToList(baseVal, isConverted);
    if (!isConverted)
        return InternalValue();

    InternalValueList resultList;
    resultList.reserve(list.GetSize().value_or(0));
    std::copy_if(list.begin(), list.end(), std::back_inserter(resultList), [this, tester, attrName, &context](auto& val) {
        InternalValue attrVal;
        bool isAttr = !IsEmpty(attrName);
        if (isAttr)
            attrVal = Subscript(val, attrName, &context);

        bool result = false;
        if (tester)
            result = tester->Test(isAttr ? attrVal : val, context);
        else
            result = ConvertToBool(isAttr ? attrVal : val);

        return (m_mode == SelectMode || m_mode == SelectAttrMode) ? result : !result;
    });

    return ListAdapter::CreateAdapter(std::move(resultList));
}

ValueConverter::ValueConverter(FilterParams params, ValueConverter::Mode mode)
    : m_mode(mode)
{
    switch (mode)
    {
        case ToFloatMode:
            ParseParams({ { "default"s, false } }, params);
            break;
        case ToIntMode:
            ParseParams({ { "default"s, false }, { "base"s, false, static_cast<int64_t>(10) } }, params);
            break;
        case ToListMode:
        case AbsMode:
            break;
        case RoundMode:
            ParseParams({ { "precision"s, false }, { "method"s, false, "common"s } }, params);
            break;
    }
}

struct ConverterParams
{
    ValueConverter::Mode mode;
    InternalValue defValule;
    InternalValue base;
    InternalValue prec;
    InternalValue roundMethod;
};

struct ValueConverterImpl : visitors::BaseVisitor<>
{
    using BaseVisitor::operator();

    ValueConverterImpl(ConverterParams params)
        : m_params(std::move(params))
    {
    }

    InternalValue operator()(int64_t val) const
    {
        InternalValue result;
        switch (m_params.mode)
        {
            case ValueConverter::ToFloatMode:
                result = InternalValue(static_cast<double>(val));
                break;
            case ValueConverter::AbsMode:
                result = InternalValue(static_cast<int64_t>(std::abs(val)));
                break;
            case ValueConverter::ToIntMode:
            case ValueConverter::RoundMode:
                result = InternalValue(static_cast<int64_t>(val));
                break;
            default:
                break;
        }

        return result;
    }

    InternalValue operator()(double val) const
    {
        InternalValue result;
        switch (m_params.mode)
        {
            case ValueConverter::ToFloatMode:
                result = static_cast<double>(val);
                break;
            case ValueConverter::ToIntMode:
                result = static_cast<int64_t>(val);
                break;
            case ValueConverter::AbsMode:
                result = InternalValue(fabs(val));
                break;
            case ValueConverter::RoundMode:
            {
                auto method = AsString(m_params.roundMethod);
                auto prec = GetAs<int64_t>(m_params.prec);
                double pow10 = std::pow(10, static_cast<int>(prec));
                val *= pow10;
                if (method == "ceil")
                    val = val < 0 ? std::floor(val) : std::ceil(val);
                else if (method == "floor")
                    val = val > 0 ? std::floor(val) : std::ceil(val);
                else if (method == "common")
                    val = std::round(val);
                result = InternalValue(val / pow10);
                break;
            }
            default:
                break;
        }

        return result;
    }

    static double ConvertToDouble(const char* buff, bool& isConverted)
    {
        char* endBuff = nullptr;
        double dblVal = strtod(buff, &endBuff);
        isConverted = *endBuff == 0;
        return dblVal;
    }

    static double ConvertToDouble(const wchar_t* buff, bool& isConverted)
    {
        wchar_t* endBuff = nullptr;
        double dblVal = wcstod(buff, &endBuff);
        isConverted = *endBuff == 0;
        return dblVal;
    }

    static long long ConvertToInt(const char* buff, int base, bool& isConverted)
    {
        char* endBuff = nullptr;
        long long intVal = strtoll(buff, &endBuff, base);
        isConverted = *endBuff == 0;
        return intVal;
    }

    static long long ConvertToInt(const wchar_t* buff, int base, bool& isConverted)
    {
        wchar_t* endBuff = nullptr;
        long long intVal = wcstoll(buff, &endBuff, base);
        isConverted = *endBuff == 0;
        return intVal;
    }

    template<typename CharT>
    InternalValue operator()(const std::basic_string<CharT>& val) const
    {
        InternalValue result;
        switch (m_params.mode)
        {
            case ValueConverter::ToFloatMode:
            {
                bool converted = false;
                double dblVal = ConvertToDouble(val.c_str(), converted);

                if (!converted)
                    result = m_params.defValule;
                else
                    result = dblVal;
                break;
            }
            case ValueConverter::ToIntMode:
            {
                int base = static_cast<int>(GetAs<int64_t>(m_params.base));
                bool converted = false;
                long long intVal = ConvertToInt(val.c_str(), base, converted);

                if (!converted)
                    result = m_params.defValule;
                else
                    result = static_cast<int64_t>(intVal);
                break;
            }
            case ValueConverter::ToListMode:
                result = ListAdapter::CreateAdapter(val.size(), [str = val](size_t idx) { return InternalValue(TargetString(str.substr(idx, 1))); });
            default:
                break;
        }

        return result;
    }

    template<typename CharT>
    InternalValue operator()(const std::basic_string_view<CharT>& val) const
    {
        InternalValue result;
        switch (m_params.mode)
        {
            case ValueConverter::ToFloatMode:
            {
                bool converted = false;
                std::basic_string<CharT> str(val.begin(), val.end());
                double dblVal = ConvertToDouble(str.c_str(), converted);

                if (!converted)
                    result = m_params.defValule;
                else
                    result = static_cast<double>(dblVal);
                break;
            }
            case ValueConverter::ToIntMode:
            {
                int base = static_cast<int>(GetAs<int64_t>(m_params.base));
                bool converted = false;
                std::basic_string<CharT> str(val.begin(), val.end());
                long long intVal = ConvertToInt(str.c_str(), base, converted);

                if (!converted)
                    result = m_params.defValule;
                else
                    result = static_cast<int64_t>(intVal);
                break;
            }
            case ValueConverter::ToListMode:
                result = ListAdapter::CreateAdapter(val.size(), [str = val](size_t idx) { return InternalValue(str.substr(idx, 1)); });
            default:
                break;
        }

        return result;
    }

    InternalValue operator()(const ListAdapter& val) const
    {
        if (m_params.mode == ValueConverter::ToListMode)
            return InternalValue(val);

        return InternalValue();
    }

    InternalValue operator()(const MapAdapter& val) const
    {
        if (m_params.mode != ValueConverter::ToListMode)
            return InternalValue();

        auto keys = val.GetKeys();
        auto num_keys = keys.size();
        return ListAdapter::CreateAdapter(num_keys, [values = std::move(keys)](size_t idx) { return InternalValue(values[idx]); });
    }

    template<typename T>
    static T GetAs(const InternalValue& val, T defValue = 0)
    {
        ConverterParams params;
        params.mode = ValueConverter::ToIntMode;
        params.base = static_cast<int64_t>(10);
        InternalValue intVal = Apply<ValueConverterImpl>(val, params);
        const T* result = GetIf<int64_t>(&intVal);
        if (result == nullptr)
            return defValue;

        return *result;
    }

    ConverterParams m_params;
};

InternalValue ValueConverter::Filter(const InternalValue& baseVal, RenderContext& context)
{
    ConverterParams params;
    params.mode = m_mode;
    params.defValule = GetArgumentValue("default", context);
    params.base = GetArgumentValue("base", context);
    params.prec = GetArgumentValue("precision", context);
    params.roundMethod = GetArgumentValue("method", context);
    auto result = Apply<ValueConverterImpl>(baseVal, params);
    if (baseVal.ShouldExtendLifetime())
        result.SetParentData(baseVal);

    return result;
}

UserDefinedFilter::UserDefinedFilter(std::string filterName, FilterParams params)
    : m_filterName(std::move(filterName))
{
    ParseParams({ { "*args" }, { "**kwargs" } }, params);
    m_callParams.kwParams = m_args.extraKwArgs;
    m_callParams.posParams = m_args.extraPosArgs;
}

InternalValue UserDefinedFilter::Filter(const InternalValue& baseVal, RenderContext& context)
{
    bool filterFound = false;
    auto filterValPtr = context.FindValue(m_filterName, filterFound);
    if (!filterFound)
        throw std::runtime_error("Can't find filter '" + m_filterName + "'");

    const Callable* callable = GetIf<Callable>(&filterValPtr->second);
    if (callable == nullptr || callable->GetKind() != Callable::UserCallable)
        return InternalValue();

    CallParams tmpCallParams = helpers::EvaluateCallParams(m_callParams, context);
    CallParams callParams;
    callParams.kwParams = std::move(tmpCallParams.kwParams);
    callParams.posParams.reserve(tmpCallParams.posParams.size() + 1);
    callParams.posParams.push_back(baseVal);
    for (auto& p : tmpCallParams.posParams)
        callParams.posParams.push_back(std::move(p));

    InternalValue result;
    if (callable->GetType() != Callable::Type::Expression)
        return InternalValue();

    return callable->GetExpressionCallable()(callParams, context);
}

} // namespace filters
} // namespace jinja2
