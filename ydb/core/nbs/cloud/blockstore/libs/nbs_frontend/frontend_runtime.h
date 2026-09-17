#pragma once

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/public.h>

#include <library/cpp/logger/log.h>

namespace NKikimrBlockStore {
class TVolumeConfig;
}

namespace NYdb::NBS::NBlockStore {

class TFrontendState;

////////////////////////////////////////////////////////////////////////////////

// Owns and controls the classic-compatible facade of the NBS2 frontend.
// With the next steps it will grow and take new functionality
// like a sessions components and RDMA target.
class TNbsFrontendRuntime final
{
public:
    explicit TNbsFrontendRuntime(TLog log);

    // Opens the frontend admission gate.
    void Start();

    // Closes the frontend admission gate and revokes the active session.
    void Stop();

    // Returns the shared classic-compatible facade.
    [[nodiscard]] NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStorePtr
    GetBlockStore() const;

    // Publishes partition metadata and returns the token used to revoke it.
    TResultOrError<TString> RegisterVolume(
        const NKikimrBlockStore::TVolumeConfig& volumeConfig);

    // Revokes a matching registration without affecting a newer instance.
    void UnregisterVolume(const TString& registrationId);

private:
    const std::shared_ptr<TFrontendState> FrontendState;
    NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStorePtr BlockStore;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
