#pragma once

#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/public.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Owns and controls the classic-compatible facade of the NBS2 frontend.
// With the next steps it will grow and take new functionality
// like a sessions components and RDMA target.
class TNbsFrontendRuntime final
{
public:
    TNbsFrontendRuntime();

    // Opens the frontend admission gate.
    void Start();

    // Closes the frontend admission gate.
    void Stop();

    // Returns the shared classic-compatible facade.
    [[nodiscard]] NCloud::NBlockStore::IBlockStorePtr GetBlockStore() const;

private:
    NCloud::NBlockStore::IBlockStorePtr BlockStore;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
