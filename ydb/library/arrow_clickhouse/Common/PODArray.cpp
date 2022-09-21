// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#include <Common/PODArray.h>

namespace CH
{

/// Used for left padding of PODArray when empty
const char empty_pod_array[empty_pod_array_size]{};

template class PODArray<UInt8, 4096, Allocator<false>, 15, 16>;
template class PODArray<UInt16, 4096, Allocator<false>, 15, 16>;
template class PODArray<UInt32, 4096, Allocator<false>, 15, 16>;
template class PODArray<UInt64, 4096, Allocator<false>, 15, 16>;

template class PODArray<Int8, 4096, Allocator<false>, 15, 16>;
template class PODArray<Int16, 4096, Allocator<false>, 15, 16>;
template class PODArray<Int32, 4096, Allocator<false>, 15, 16>;
template class PODArray<Int64, 4096, Allocator<false>, 15, 16>;

}
