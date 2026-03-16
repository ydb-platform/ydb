#pragma once
#include <utility>
#include <cstddef>


namespace DB_CHDB
{

/// Find the address and size of the mapped memory region pointed by ptr.
/// Throw exception if not found.
std::pair<void *, size_t> getMappedArea(void * ptr);

}
