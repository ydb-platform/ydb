#pragma once
#include <util/generic/string.h>

#include <utility>

namespace NKikimr::NStructLog {

class TKeyName {
public:

    TKeyName() = default;
    TKeyName(const TKeyName&) = default;
    TKeyName(TKeyName&&) = default;

    template<unsigned N>
    constexpr TKeyName(const char(&name)[N]) : CompileTime(name) {}
    TKeyName(const TString& name) : RunTime(name) {}
    TKeyName(TString&& name) : RunTime(std::move(name)) {}

    constexpr operator const char* () const {
        if (CompileTime != nullptr) {
            return CompileTime;
        } else {
            return RunTime.c_str();
        }
    }

    bool operator==(const TKeyName& value) const {
        if(CompileTime != nullptr && value.CompileTime != nullptr) {
            return CompileTime == value.CompileTime || strcmp(CompileTime, value.CompileTime) == 0;
        }
        if(CompileTime != nullptr && value.CompileTime == nullptr) {
            return strcmp(CompileTime, value.RunTime.c_str()) == 0;
        }
        if(CompileTime == nullptr && value.CompileTime != nullptr) {
            return strcmp(RunTime.c_str(), value.CompileTime) == 0;
        }
        return RunTime == value.RunTime;
    }

    bool operator!=(const TKeyName& value) const {
        return !(*this == value);
    }

    bool operator<(const TKeyName& value) const {
        if(CompileTime != nullptr && value.CompileTime != nullptr) {
            if (CompileTime == value.CompileTime) {
                return false;
            }
            return strcmp(CompileTime, value.CompileTime) < 0;
        }
        if(CompileTime != nullptr && value.CompileTime == nullptr) {
            return strcmp(CompileTime, value.RunTime.c_str()) < 0;
        }
        if(CompileTime == nullptr && value.CompileTime != nullptr) {
            return strcmp(RunTime.c_str(), value.CompileTime) < 0;
        }
        return RunTime < value.RunTime;
    }

    bool operator>(const TKeyName& value) const {
        if(CompileTime != nullptr && value.CompileTime != nullptr) {
            if (CompileTime == value.CompileTime) {
                return false;
            }
            return strcmp(CompileTime, value.CompileTime) > 0;
        }
        if(CompileTime != nullptr && value.CompileTime == nullptr) {
            return strcmp(CompileTime, value.RunTime.c_str()) > 0;
        }
        if(CompileTime == nullptr && value.CompileTime != nullptr) {
            return strcmp(RunTime.c_str(), value.CompileTime) > 0;
        }
        return RunTime > value.RunTime;
    }

    bool operator>(const TString& value) const {
        if(CompileTime != nullptr) {
            return strcmp(CompileTime, value.c_str()) > 0;
        }
        return RunTime > value;
    }

    TKeyName& operator=(const TKeyName& value) = default;
    TKeyName& operator=(TKeyName&& value) = default;

    bool empty() const {
        if (CompileTime != nullptr) {
            return CompileTime[0] == 0;
        }
        return RunTime.empty();
    }
protected:
    const char* CompileTime{nullptr};
    TString RunTime;
};

}