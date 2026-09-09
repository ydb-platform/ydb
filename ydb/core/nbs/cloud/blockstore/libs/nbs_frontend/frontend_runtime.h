#pragma once

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/public.h>

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
    [[nodiscard]] NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStorePtr
    GetBlockStore() const;

private:
    NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStorePtr BlockStore;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
