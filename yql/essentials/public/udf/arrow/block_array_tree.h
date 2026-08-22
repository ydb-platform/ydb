#pragma once

#include <arrow/array/data.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/generic/deque.h>

#include <memory>

namespace NYql::NUdf {

struct TBlockArrayTree {
    using Ptr = std::shared_ptr<TBlockArrayTree>;
    std::deque<std::shared_ptr<arrow::ArrayData>> Payload;
    std::vector<TBlockArrayTree::Ptr> Children;
};

arrow::Datum ToChunkedArray(TBlockArrayTree& tree, arrow::MemoryPool* pool);

} // namespace NYql::NUdf
