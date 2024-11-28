#ifndef JINJA2CPP_SRC_RENDER_CONTEXT_H
#define JINJA2CPP_SRC_RENDER_CONTEXT_H

#include "internal_value.h"
#include <jinja2cpp/error_info.h>
#include <jinja2cpp/utils/i_comparable.h>

#include <contrib/restricted/expected-lite/include/nonstd/expected.hpp>

#include <list>
#include <deque>

namespace jinja2
{
template<typename CharT>
class TemplateImpl;

struct IRendererCallback : IComparable
{
    virtual ~IRendererCallback() {}
    virtual TargetString GetAsTargetString(const InternalValue& val) = 0;
    virtual OutStream GetStreamOnString(TargetString& str) = 0;
    virtual std::variant<EmptyValue,
        nonstd::expected<std::shared_ptr<TemplateImpl<char>>, ErrorInfo>,
        nonstd::expected<std::shared_ptr<TemplateImpl<wchar_t>>, ErrorInfoW>> LoadTemplate(const std::string& fileName) const = 0;
    virtual std::variant<EmptyValue,
        nonstd::expected<std::shared_ptr<TemplateImpl<char>>, ErrorInfo>,
        nonstd::expected<std::shared_ptr<TemplateImpl<wchar_t>>, ErrorInfoW>> LoadTemplate(const InternalValue& fileName) const = 0;
    virtual void ThrowRuntimeError(ErrorCode code, ValuesList extraParams) = 0;
};

class RenderContext
{
public:
    RenderContext(const InternalValueMap& extValues, const InternalValueMap& globalValues, IRendererCallback* rendererCallback)
        : m_rendererCallback(rendererCallback)
    {
        m_externalScope = &extValues;
        m_globalScope = &globalValues;
        EnterScope();
        (*m_currentScope)["self"] = CreateMapAdapter(InternalValueMap());
    }

    RenderContext(const RenderContext& other)
        : m_rendererCallback(other.m_rendererCallback)
        , m_externalScope(other.m_externalScope)
        , m_globalScope(other.m_globalScope)
        , m_boundScope(other.m_boundScope)
        , m_scopes(other.m_scopes)
    {
        m_currentScope = &m_scopes.back();
    }

    InternalValueMap& EnterScope()
    {
        m_scopes.push_back(InternalValueMap());
        m_currentScope = &m_scopes.back();
        return *m_currentScope;
    }

    void ExitScope()
    {
        m_scopes.pop_back();
        if (!m_scopes.empty())
            m_currentScope = &m_scopes.back();
        else
            m_currentScope = nullptr;
    }

    auto FindValue(const std::string& val, bool& found) const
    {
        auto finder = [&val, &found](auto& map) mutable
        {
            auto p = map.find(val);
            if (p != map.end())
                found = true;

            return p;
        };

        if (m_boundScope)
        {
            auto valP = finder(*m_boundScope);
            if (found)
                return valP;
        }

        for (auto p = m_scopes.rbegin(); p != m_scopes.rend(); ++ p)
        {
            auto valP = finder(*p);
            if (found)
                return valP;
        }

        auto valP = finder(*m_externalScope);
        if (found)
            return valP;

        return finder(*m_globalScope);
    }

    auto& GetCurrentScope() const
    {
        return *m_currentScope;
    }

    auto& GetCurrentScope()
    {
        return *m_currentScope;
    }
    auto& GetGlobalScope()
    {
        return m_scopes.front();
    }
    auto GetRendererCallback()
    {
        return m_rendererCallback;
    }
    RenderContext Clone(bool includeCurrentContext) const
    {
        if (!includeCurrentContext)
            return RenderContext(m_emptyScope, *m_globalScope, m_rendererCallback);

        return RenderContext(*this);
    }

    void BindScope(InternalValueMap* scope)
    {
        m_boundScope = scope;
    }

    bool IsEqual(const RenderContext& other) const
    {
        if (!IsEqual(m_rendererCallback, other.m_rendererCallback))
            return false;
        if (!IsEqual(this->m_currentScope, other.m_currentScope))
            return false;
        if (!IsEqual(m_externalScope, other.m_externalScope))
            return false;
        if (!IsEqual(m_globalScope, other.m_globalScope))
            return false;
        if (!IsEqual(m_boundScope, other.m_boundScope))
            return false;
        if (m_emptyScope != other.m_emptyScope)
            return false;
        if (m_scopes != other.m_scopes)
            return false;
        return true;
    }

private:

    bool IsEqual(const IRendererCallback* lhs, const IRendererCallback* rhs) const
    {
        if (lhs && rhs)
            return lhs->IsEqual(*rhs);
        if ((!lhs && rhs) || (lhs && !rhs))
            return false;
        return true;
    }

    bool IsEqual(const InternalValueMap* lhs, const InternalValueMap* rhs) const
    {
        if (lhs && rhs)
            return *lhs == *rhs;
        if ((!lhs && rhs) || (lhs && !rhs))
            return false;
        return true;
    }

private:
    IRendererCallback* m_rendererCallback{};
    InternalValueMap* m_currentScope{};
    const InternalValueMap* m_externalScope{};
    const InternalValueMap* m_globalScope{};
    const InternalValueMap* m_boundScope{};
    InternalValueMap m_emptyScope;
    std::deque<InternalValueMap> m_scopes;
};
} // namespace jinja2

#endif // JINJA2CPP_SRC_RENDER_CONTEXT_H
