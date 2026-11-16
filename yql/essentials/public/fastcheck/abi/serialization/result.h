#pragma once

#include "message.h"

#include <util/generic/scope.h>

namespace NYql::NFastCheck::NAbi {

template <class TMessage, class THandle, class TFree, class TOutput, class TError>
TMessage ReadResult(THandle* handle, TFree free, TOutput output, TError error) {
    Y_DEFER {
        free(handle);
    };
    size_t size = 0;
    const char* data = error(handle, &size);
    Y_ENSURE(!size, TStringBuf(data, size));
    data = output(handle, &size);
    return ParseMessage<TMessage>(data, size);
}

} // namespace NYql::NFastCheck::NAbi
