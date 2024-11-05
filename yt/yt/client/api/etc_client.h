#pragma once

#include "client_common.h"

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node_directory.pb.h>

#include <yt/yt_proto/yt/client/hive/proto/cluster_directory.pb.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TGetClusterMetaOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    bool PopulateNodeDirectory = false;
    bool PopulateClusterDirectory = false;
    bool PopulateMediumDirectory = false;
    bool PopulateCellDirectory = false;
    bool PopulateMasterCacheNodeAddresses = false;
    bool PopulateTimestampProviderAddresses = false;
    bool PopulateFeatures = false;
    bool PopulateUserDirectory = false;
};

struct TClusterMeta
{
    std::shared_ptr<NNodeTrackerClient::NProto::TNodeDirectory> NodeDirectory;
    std::shared_ptr<NHiveClient::NProto::TClusterDirectory> ClusterDirectory;
    std::shared_ptr<NChunkClient::NProto::TMediumDirectory> MediumDirectory;
    std::shared_ptr<NObjectClient::NProto::TUserDirectory> UserDirectory;
    std::vector<std::string> MasterCacheNodeAddresses;
    std::vector<std::string> TimestampProviderAddresses;
    NYTree::IMapNodePtr Features;
};

struct TCheckClusterLivenessOptions
    : public TTimeoutOptions
{
    //! Checks Cypress root availability.
    bool CheckCypressRoot = false;
    //! Checks secondary master cells generic availability.
    bool CheckSecondaryMasterCells = false;
    //! Unless null checks tablet cell bundle health.
    std::optional<TString> CheckTabletCellBundle;

    bool IsCheckTrivial() const;

    // NB: For testing purposes.
    bool operator==(const TCheckClusterLivenessOptions& other) const;
};

////////////////////////////////////////////////////////////////////////////////

struct IEtcClientBase
{
    virtual ~IEtcClientBase() = default;

    virtual TFuture<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IEtcClient
{
    virtual ~IEtcClient() = default;

    virtual TFuture<TClusterMeta> GetClusterMeta(
        const TGetClusterMetaOptions& options = {}) = 0;

    virtual TFuture<void> CheckClusterLiveness(
        const TCheckClusterLivenessOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

