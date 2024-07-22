#pragma once

#include "public.h"

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/copyable_atomic.h>

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

//! Network-related node information.
class TNodeDescriptor
{
public:
    TNodeDescriptor();
    TNodeDescriptor(const TNodeDescriptor& other) = default;
    TNodeDescriptor(TNodeDescriptor&& other) = default;
    explicit TNodeDescriptor(const TString& defaultAddress);
    explicit TNodeDescriptor(const std::optional<TString>& defaultAddress);
    explicit TNodeDescriptor(
        TAddressMap addresses,
        std::optional<TString> host = std::nullopt,
        std::optional<TString> rack = std::nullopt,
        std::optional<TString> dc = std::nullopt,
        const std::vector<TString>& tags = {},
        std::optional<TInstant> lastSeenTime = {});

    TNodeDescriptor& operator=(const TNodeDescriptor& other) = default;
    TNodeDescriptor& operator=(TNodeDescriptor&& other) = default;

    bool IsNull() const;

    const TAddressMap& Addresses() const;

    const TString& GetDefaultAddress() const;

    const TString& GetAddressOrThrow(const TNetworkPreferenceList& networks) const;

    std::optional<TString> FindAddress(const TNetworkPreferenceList& networks) const;

    const std::optional<TString>& GetHost() const;
    const std::optional<TString>& GetRack() const;
    const std::optional<TString>& GetDataCenter() const;

    const std::vector<TString>& GetTags() const;

    //! GetLastSeenTime returns last instant when node was seen online on some master.
    /*!
     *  Might be used for cheap and dirty availability check.
     *  This field is not persisted.
     */
    std::optional<TInstant> GetLastSeenTime() const;
    void UpdateLastSeenTime(TInstant at) const;

    void Persist(const TStreamPersistenceContext& context);

    friend void SerializeFragment(const TNodeDescriptor& descriptor, NYson::IYsonConsumer* consumer);
    friend void DeserializeFragment(TNodeDescriptor& descriptor, NYTree::INodePtr node);

private:
    TAddressMap Addresses_;
    TString DefaultAddress_;
    std::optional<TString> Host_;
    std::optional<TString> Rack_;
    std::optional<TString> DataCenter_;
    std::vector<TString> Tags_;

    // Not persisted.
    mutable TCopyableAtomic<TCpuInstant> LastSeenTime_;
};

const TString& NullNodeAddress();
const TNodeDescriptor& NullNodeDescriptor();

////////////////////////////////////////////////////////////////////////////////

bool operator == (const TNodeDescriptor& lhs, const TNodeDescriptor& rhs);
bool operator == (const TNodeDescriptor& lhs, const NProto::TNodeDescriptor& rhs);

void FormatValue(TStringBuilderBase* builder, const TNodeDescriptor& descriptor, TStringBuf spec);

// Accessors for some well-known addresses.
std::optional<TString> FindDefaultAddress(const TAddressMap& addresses);
const TString& GetDefaultAddress(const TAddressMap& addresses);
const TString& GetDefaultAddress(const NProto::TAddressMap& addresses);

const TString& GetAddressOrThrow(const TAddressMap& addresses, const TNetworkPreferenceList& networks);
std::optional<TString> FindAddress(const TAddressMap& addresses, const TNetworkPreferenceList& networks);

const TAddressMap& GetAddressesOrThrow(const TNodeAddressMap& nodeAddresses, EAddressType type);

//! Keep the items in this particular order: the further the better.
DEFINE_ENUM(EAddressLocality,
    (None)
    (SameDataCenter)
    (SameRack)
    (SameHost)
);

EAddressLocality ComputeAddressLocality(const TNodeDescriptor& first, const TNodeDescriptor& second);

namespace NProto {

void ToProto(NNodeTrackerClient::NProto::TAddressMap* protoAddresses, const NNodeTrackerClient::TAddressMap& addresses);
void FromProto(NNodeTrackerClient::TAddressMap* addresses, const NNodeTrackerClient::NProto::TAddressMap& protoAddresses);

void ToProto(NNodeTrackerClient::NProto::TNodeAddressMap* proto, const NNodeTrackerClient::TNodeAddressMap& nodeAddresses);
void FromProto(NNodeTrackerClient::TNodeAddressMap* nodeAddresses, const NNodeTrackerClient::NProto::TNodeAddressMap& proto);

void ToProto(NNodeTrackerClient::NProto::TNodeDescriptor* protoDescriptor, const NNodeTrackerClient::TNodeDescriptor& descriptor);
void FromProto(NNodeTrackerClient::TNodeDescriptor* descriptor, const NNodeTrackerClient::NProto::TNodeDescriptor& protoDescriptor);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient

template <>
struct THash<NYT::NNodeTrackerClient::TNodeDescriptor>
{
    size_t operator()(const NYT::NNodeTrackerClient::TNodeDescriptor& value) const;
};

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

//! Caches node descriptors obtained by fetch requests.
/*!
 *  \note
 *  Thread affinity: thread-safe
 */
class TNodeDirectory
    : public TRefCounted
{
public:
    void MergeFrom(const NProto::TNodeDirectory& source);
    void MergeFrom(const TNodeDirectoryPtr& source);
    void DumpTo(NProto::TNodeDirectory* destination);
    void Serialize(NYson::IYsonConsumer* consumer) const;

    void AddDescriptor(TNodeId id, const TNodeDescriptor& descriptor);

    const TNodeDescriptor* FindDescriptor(TNodeId id) const;
    const TNodeDescriptor& GetDescriptor(TNodeId id) const;
    TFuture<const TNodeDescriptor*> GetAsyncDescriptor(TNodeId id);
    const TNodeDescriptor& GetDescriptor(NChunkClient::TChunkReplica replica) const;
    std::vector<TNodeDescriptor> GetDescriptors(const NChunkClient::TChunkReplicaList& replicas) const;
    std::vector<std::pair<NNodeTrackerClient::TNodeId, TNodeDescriptor>> GetAllDescriptors() const;

    const TNodeDescriptor* FindDescriptor(const TString& address);
    const TNodeDescriptor& GetDescriptor(const TString& address);

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<TNodeId, const TNodeDescriptor*> IdToDescriptor_;
    THashMap<TString, const TNodeDescriptor*> AddressToDescriptor_;
    THashSet<TNodeDescriptor> Descriptors_;

    THashMap<TNodeId, TPromise<const TNodeDescriptor*>> IdToPromise_;

    bool CheckNodeDescriptor(TNodeId id, const TNodeDescriptor& descriptor);
    void DoAddDescriptor(TNodeId id, const TNodeDescriptor& descriptor);
    bool CheckNodeDescriptor(TNodeId id, const NProto::TNodeDescriptor& descriptor);
    void DoAddDescriptor(TNodeId id, const NProto::TNodeDescriptor& protoDescriptor);
    void DoCaptureAndAddDescriptor(TNodeId id, TNodeDescriptor&& descriptorHolder);

    void OnDescriptorAdded(TNodeId id, const TNodeDescriptor* descriptor);
};

void Serialize(const TNodeDirectory& nodeDirectory, NYson::IYsonConsumer* consumer);

DEFINE_REFCOUNTED_TYPE(TNodeDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
