#ifndef JINJA2CPP_SRC_FILTERS_H
#define JINJA2CPP_SRC_FILTERS_H

#include "expression_evaluator.h"
#include "function_base.h"
#include "jinja2cpp/value.h"
#include "render_context.h"

#include <memory>
#include <functional>

namespace jinja2
{
using FilterPtr = std::shared_ptr<ExpressionFilter::IExpressionFilter>;
using FilterParams = CallParamsInfo;

extern FilterPtr CreateFilter(std::string filterName, CallParamsInfo params);

namespace filters
{
class FilterBase : public FunctionBase, public ExpressionFilter::IExpressionFilter
{
};

class ApplyMacro : public FilterBase
{
public:
    ApplyMacro(FilterParams params);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const ApplyMacro*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        if (m_mappingParams != value->m_mappingParams)
            return false;
        return true;
    }
private:
    FilterParams m_mappingParams;
};

class Attribute : public  FilterBase
{
public:
    Attribute(FilterParams params);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const Attribute*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        return true;
    }
};

class Default : public  FilterBase
{
public:
    Default(FilterParams params);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const Default*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        return true;
    }
};

class DictSort : public  FilterBase
{
public:
    DictSort(FilterParams params);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const DictSort*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        return true;
    }
};

class GroupBy : public FilterBase
{
public:
    GroupBy(FilterParams params);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const GroupBy*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        return true;
    }
};

class Join : public FilterBase
{
public:
    Join(FilterParams params);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const Join*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        return true;
    }
};

class Map : public FilterBase
{
public:
    Map(FilterParams params);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const Map*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        if (m_mappingParams != value->m_mappingParams)
            return false;
        return true;
    }
private:
    static FilterParams MakeParams(FilterParams);

    FilterParams m_mappingParams;
};

class PrettyPrint : public FilterBase
{
public:
    PrettyPrint(FilterParams params);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const PrettyPrint*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        return true;
    }
};

class Random : public FilterBase
{
public:
    Random(FilterParams params);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const Random*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        return true;
    }
};

class SequenceAccessor : public  FilterBase
{
public:
    enum Mode
    {
        FirstItemMode,
        LastItemMode,
        LengthMode,
        MaxItemMode,
        MinItemMode,
        RandomMode,
        ReverseMode,
        SumItemsMode,
        UniqueItemsMode,

    };

    SequenceAccessor(FilterParams params, Mode mode);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const SequenceAccessor*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        if (m_mode != value->m_mode)
            return false;
        return true;
    }
private:
    Mode m_mode;
};

class Serialize : public  FilterBase
{
public:
    enum Mode
    {
        JsonMode,
        XmlMode,
        YamlMode
    };

    Serialize(FilterParams params, Mode mode);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const Serialize*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        if (m_mode != value->m_mode)
            return false;
        return true;
    }
private:
    Mode m_mode;
};

class Slice : public  FilterBase
{
public:
    enum Mode
    {
        BatchMode,
        SliceMode,
    };

    Slice(FilterParams params, Mode mode);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const Slice*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        if (m_mode != value->m_mode)
            return false;
        return true;
    }
private:
    InternalValue Batch(const InternalValue& baseVal, RenderContext& context);

    Mode m_mode;
};

class Sort : public  FilterBase
{
public:
    Sort(FilterParams params);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const Sort*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        return true;
    }
};

class StringConverter : public  FilterBase
{
public:
    enum Mode
    {
        CapitalMode,
        CamelMode,
        EscapeCppMode,
        EscapeHtmlMode,
        LowerMode,
        ReplaceMode,
        StriptagsMode,
        TitleMode,
        TrimMode,
        TruncateMode,
        UpperMode,
        WordCountMode,
        WordWrapMode,
        UnderscoreMode,
        UrlEncodeMode,
        CenterMode
    };

    StringConverter(FilterParams params, Mode mode);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const StringConverter*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        if (m_mode != value->m_mode)
            return false;
        return true;
    }
private:
    Mode m_mode;
};

class StringFormat : public  FilterBase
{
public:
    StringFormat(FilterParams params);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const StringFormat*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        if (m_params != value->m_params)
            return false;
        return true;
    }

private:
    FilterParams m_params;
};

class Tester : public  FilterBase
{
public:
    enum Mode
    {
        RejectMode,
        RejectAttrMode,
        SelectMode,
        SelectAttrMode,
    };

    Tester(FilterParams params, Mode mode);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const Tester*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        if (m_mode != value->m_mode)
            return false;
        if (m_testingParams != value->m_testingParams)
            return false;
        return true;
    }
private:
    Mode m_mode;
    FilterParams m_testingParams;
    bool m_noParams = false;
};

class ValueConverter : public  FilterBase
{
public:
    enum Mode
    {
        ToFloatMode,
        ToIntMode,
        ToListMode,
        AbsMode,
        RoundMode,
    };

    ValueConverter(FilterParams params, Mode mode);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const ValueConverter*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        return m_mode == value->m_mode;
    }
private:
    Mode m_mode;
};

class XmlAttrFilter : public FilterBase
{
public:
    explicit XmlAttrFilter(FilterParams params);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;
    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const XmlAttrFilter*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        return true;
    }
};

class UserDefinedFilter : public FilterBase
{
public:
    UserDefinedFilter(std::string filterName, FilterParams params);

    InternalValue Filter(const InternalValue& baseVal, RenderContext& context) override;

    bool IsEqual(const IComparable& other) const override
    {
        auto* value = dynamic_cast<const UserDefinedFilter*>(&other);
        if (!value)
            return false;
        if (m_args != value->m_args)
            return false;
        if (m_filterName != value->m_filterName)
            return false;
        if (m_callParams != m_callParams)
            return false;
        return true;
    }

private:
    std::string m_filterName;
    FilterParams m_callParams;
};

} // namespace filters
} // namespace jinja2

#endif // JINJA2CPP_SRC_FILTERS_H
