
#include "wavm_private_imports.h"

#include <ydb/library/wasm/api/memory_pool.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

TWebAssemblyMemoryPool::~TWebAssemblyMemoryPool()
{
    try {
        Clear();
    } catch (WAVM::Runtime::Exception* exception) {
        // In the case when free inside the vm results in an error
        // (for example, if the user's code causes the allocator to malfunction),
        // we catch the exception and skip the error, since we are already in the destructor.
        WAVM::Runtime::destroyException(exception);
    } catch (...) {
        // FreeBytes converts traps to THROW_ERROR_EXCEPTION; never throw from dtor.
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
