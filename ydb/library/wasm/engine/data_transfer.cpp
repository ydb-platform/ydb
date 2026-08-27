
#include "wavm_private_imports.h"

#include <ydb/library/wasm/api/data_transfer.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

TCopyGuard::~TCopyGuard()
{
    if (Compartment_ != nullptr && CopiedOffset_ != 0) {
        try {
            Compartment_->FreeBytes(CopiedOffset_);
        } catch (WAVM::Runtime::Exception* exception) {
            // In the case when free inside the vm results in an error
            // (for example, if the user's code causes the allocator to malfunction),
            // we catch the exception and skip the error, since we are already in the destructor.
            WAVM::Runtime::destroyException(exception);
        } catch (...) {
            // FreeBytes converts traps to THROW_ERROR_EXCEPTION; never throw from dtor.
        }
    }
}

TGuestBuffer::~TGuestBuffer()
{
    if (Compartment_ != nullptr && Offset_ != 0) {
        try {
            Compartment_->FreeBytes(Offset_);
        } catch (WAVM::Runtime::Exception* exception) {
            WAVM::Runtime::destroyException(exception);
        } catch (...) {
            // FreeBytes may throw; never throw from dtor.
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
