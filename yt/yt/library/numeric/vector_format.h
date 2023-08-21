#pragma once

#include <util/string/builder.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TString ToString(const std::vector<T>& vec)
{
    ::TStringBuilder outputStream;
    outputStream << "[";
    for (size_t index = 0; index < vec.size(); ++index) {
        outputStream << vec[index];
        if (index + 1 < vec.size()) {
            outputStream << ", ";
        }
    }
    outputStream << "]";
    return std::move(outputStream);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
