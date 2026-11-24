#pragma once

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/generic/ylimits.h>

namespace NYql::NAbi {

template <class TMessage>
TMessage ParseMessage(const char* data, size_t size) {
    Y_ENSURE(data || !size, "Null ABI message with nonzero size");
    Y_ENSURE(size <= Max<int>(), "ABI message exceeds protobuf size limit");
    TMessage result;
    Y_ENSURE(result.ParseFromArray(data ? data : "", static_cast<int>(size)), "Invalid ABI protobuf");
    return result;
}

template <class TMessage>
TString SerializeMessage(const TMessage& message) {
    TString result;
    Y_ENSURE(message.SerializeToString(&result), "Failed to serialize ABI protobuf");
    return result;
}

} // namespace NYql::NAbi
