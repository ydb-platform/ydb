#pragma once

#include <util/generic/string.h>

#include <algorithm>
#include <cstring>
#include <utility>

namespace NActors::NStructuredLog {

class TKeyName {
public:
    TKeyName() = default;
    TKeyName(const TKeyName&) = default;
    TKeyName(TKeyName&&) = default;

    template <unsigned N>
    constexpr TKeyName(const char (&name)[N])
        : CompileTime(name)
        , CompileTimeLength(N - 1 /* zero char */)
    {}
    TKeyName(const char* name, std::size_t length);
    TKeyName(const TString& name);
    TKeyName(const TStringBuf& name);
    TKeyName(TString&& name);

    const char* GetData() const;

    std::size_t GetLength() const;

    TString ToString() const;

    bool operator==(const TKeyName& value) const;

    bool operator!=(const TKeyName& value) const;

    bool operator<(const TKeyName& value) const;

    bool operator>(const TKeyName& value) const;

    bool operator>(const TString& value) const;

    TKeyName& operator=(const TKeyName& value) = default;
    TKeyName& operator=(TKeyName&& value) = default;

    bool IsEmpty() const;

protected:
    const char* CompileTime{nullptr};
    std::size_t CompileTimeLength{0};
    TString RunTime;

    enum class ECompareResult {
        Less,
        Equal,
        Greater,
    };

    static ECompareResult Compare(const char* a, std::size_t lengthA, const char* b, std::size_t lengthB);

    static ECompareResult Compare(const TKeyName& a, const TKeyName& b);
};

}  // namespace NActors::NStructuredLog
