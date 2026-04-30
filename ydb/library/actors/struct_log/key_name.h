#pragma once
#include <util/generic/string.h>

#include <algorithm>
#include <cstring>
#include <utility>

namespace NKikimr::NStructuredLog {

class TKeyName {
public:

    TKeyName() = default;
    TKeyName(const TKeyName&) = default;
    TKeyName(TKeyName&&) = default;

    template<unsigned N>
    constexpr TKeyName(const char(&name)[N]) : CompileTime(name), CompileTimeLength(N - 1 /* zero char */) {}
    TKeyName(const char* name, std::size_t length) : CompileTime(name), CompileTimeLength(length) {}
    TKeyName(const TString& name) : RunTime(name) {}
    TKeyName(const TStringBuf& name) : RunTime(name) {}
    TKeyName(TString&& name) : RunTime(std::move(name)) {}

    const char* GetData() const {
        if (CompileTime != nullptr) {
            return CompileTime;
        } else {
            return RunTime.c_str();
        }
    }

    std::size_t GetLength() const {
        if (CompileTime != nullptr) {
            return CompileTimeLength;
        } else {
            return RunTime.size();
        }
    }

    TString ToString() const {
        return TString(GetData(), GetLength());
    }

    bool operator==(const TKeyName& value) const {
        return Compare(*this, value) == ECompareResult::Equal;
    }

    bool operator!=(const TKeyName& value) const {
        return !(*this == value);
    }

    bool operator<(const TKeyName& value) const {
        return Compare(*this, value) == ECompareResult::Less;
    }

    bool operator>(const TKeyName& value) const {
        return Compare(*this, value) == ECompareResult::Great;
    }

    bool operator>(const TString& value) const {
        ECompareResult cmp;
        if (CompileTime != nullptr) {
            cmp = Compare(CompileTime, CompileTimeLength, value.c_str(), value.size());
        } else {
            cmp = Compare(RunTime.c_str(), RunTime.size(), value.c_str(), value.size());
        }
        return cmp == ECompareResult::Great;
    }

    TKeyName& operator=(const TKeyName& value) = default;
    TKeyName& operator=(TKeyName&& value) = default;

    bool IsEmpty() const {
        if (CompileTime != nullptr) {
            return CompileTime[0] == 0;
        }
        return RunTime.empty();
    }

protected:
    const char* CompileTime{nullptr};
    std::size_t CompileTimeLength{0};
    TString RunTime;

    enum class ECompareResult {
        Less,
        Equal,
        Great,
    };

    static ECompareResult Compare(const char* a, std::size_t lengthA, const char* b, std::size_t lengthB) {
        auto minLength = std::min(lengthA, lengthB);
        int result = strncmp(a, b, minLength);
        if (result < 0) {
            return ECompareResult::Less;
        } else if (result > 0) {
            return ECompareResult::Great;
        }

        if (minLength < lengthB) {
            return ECompareResult::Less;
        } else if (minLength < lengthA) {
            return ECompareResult::Great;
        }

        return ECompareResult::Equal;
    }

    static ECompareResult Compare(const TKeyName& a, const TKeyName& b) {
        return Compare(a.GetData(), a.GetLength(), b.GetData(), b.GetLength());
    }
};

}
