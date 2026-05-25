#include "key_name.h"

namespace NActors::NStructuredLog {

TKeyName::TKeyName(const char* name, std::size_t length)
    : CompileTime(name)
    , CompileTimeLength(length)
{}

TKeyName::TKeyName(const TString& name)
    : RunTime(name)
{}

TKeyName::TKeyName(const TStringBuf& name)
    : RunTime(name)
{}

TKeyName::TKeyName(TString&& name)
    : RunTime(std::move(name))
{}

const char* TKeyName::GetData() const {
    if (CompileTime != nullptr) {
        return CompileTime;
    } else {
        return RunTime.c_str();
    }
}

std::size_t TKeyName::GetLength() const {
    if (CompileTime != nullptr) {
        return CompileTimeLength;
    } else {
        return RunTime.size();
    }
}

TString TKeyName::ToString() const {
    return TString(GetData(), GetLength());
}

bool TKeyName::operator==(const TKeyName& value) const {
    return Compare(*this, value) == ECompareResult::Equal;
}

bool TKeyName::operator!=(const TKeyName& value) const {
    return !(*this == value);
}

bool TKeyName::operator<(const TKeyName& value) const {
    return Compare(*this, value) == ECompareResult::Less;
}

bool TKeyName::operator>(const TKeyName& value) const {
    return Compare(*this, value) == ECompareResult::Greater;
}

bool TKeyName::operator>(const TString& value) const {
    ECompareResult cmp;
    if (CompileTime != nullptr) {
        cmp = Compare(CompileTime, CompileTimeLength, value.c_str(), value.size());
    } else {
        cmp = Compare(RunTime.c_str(), RunTime.size(), value.c_str(), value.size());
    }
    return cmp == ECompareResult::Greater;
}

bool TKeyName::IsEmpty() const {
    if (CompileTime != nullptr) {
        return CompileTime[0] == 0;
    }
    return RunTime.empty();
}

TKeyName::ECompareResult TKeyName::Compare(const char* a, std::size_t lengthA, const char* b, std::size_t lengthB) {
    const auto minLength = std::min(lengthA, lengthB);
    const int result = strncmp(a, b, minLength);
    if (result < 0) {
        return ECompareResult::Less;
    } else if (result > 0) {
        return ECompareResult::Greater;
    }

    if (minLength < lengthB) {
        return ECompareResult::Less;
    } else if (minLength < lengthA) {
        return ECompareResult::Greater;
    }

    return ECompareResult::Equal;
}

TKeyName::ECompareResult TKeyName::Compare(const TKeyName& a, const TKeyName& b) {
    return Compare(a.GetData(), a.GetLength(), b.GetData(), b.GetLength());
}

}  // namespace NActors::NStructuredLog
