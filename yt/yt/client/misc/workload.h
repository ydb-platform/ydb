#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// This distribution of weights corresponds to the logic of the GetBasicPriority method.
static const TEnumIndexedArray<EWorkloadCategory, double> DefaultFairShareWorkloadCategoryWeights = {
    {EWorkloadCategory::Idle, 0},

    {EWorkloadCategory::SystemReplication, 1},
    {EWorkloadCategory::SystemMerge, 1},
    {EWorkloadCategory::SystemReincarnation, 1},
    {EWorkloadCategory::SystemTabletCompaction, 1},
    {EWorkloadCategory::SystemTabletPartitioning, 1},
    {EWorkloadCategory::SystemTabletPreload, 1},
    {EWorkloadCategory::SystemTabletReplication, 1},
    {EWorkloadCategory::SystemTabletStoreFlush, 1},
    {EWorkloadCategory::SystemArtifactCacheDownload, 1},
    {EWorkloadCategory::UserBatch, 1},

    {EWorkloadCategory::SystemRepair, 2},
    {EWorkloadCategory::SystemTabletSnapshot, 2},

    {EWorkloadCategory::UserInteractive, 3},
    {EWorkloadCategory::UserDynamicStoreRead, 3},
    {EWorkloadCategory::SystemTabletRecovery, 3},

    {EWorkloadCategory::UserRealtime, 4},
    {EWorkloadCategory::SystemTabletLogging, 4},
};

////////////////////////////////////////////////////////////////////////////////

struct TWorkloadDescriptor
{
    TWorkloadDescriptor(
        EWorkloadCategory category = EWorkloadCategory::Idle,
        int band = 0,
        TInstant instant = {},
        std::vector<TString> annotations = {},
        std::optional<NConcurrency::TFairShareThreadPoolTag> compressionFairShareTag = {});

    //! The type of the workload defining its basic priority.
    EWorkloadCategory Category;

    //! The relative importance of the workload (among others within the category).
    //! Zero is the default value.
    //! Larger is better.
    int Band;

    //! The time instant when this workload has been initiated.
    //! #EWorkloadCategory::UserBatch workloads rely on this value for FIFO ordering.
    TInstant Instant;

    //! Arbitrary client-supplied strings to be logged at server-side.
    std::vector<TString> Annotations;

    //! If present, invoker from fair share thread pool will be used for decompression.
    std::optional<NConcurrency::TFairShareThreadPoolTag> CompressionFairShareTag;

    //! Updates the instant field with the current time.
    TWorkloadDescriptor SetCurrentInstant() const;

    //! Computes the aggregated priority.
    //! Larger is better.
    i64 GetPriority() const;

    bool operator==(const TWorkloadDescriptor& other) const = default;
};

i64 GetBasicPriority(EWorkloadCategory category);

IInvokerPtr GetCompressionInvoker(const TWorkloadDescriptor& workloadDescriptor);

bool IsSystemWorkloadCategory(EWorkloadCategory category);

void FormatValue(
    TStringBuilderBase* builder,
    const TWorkloadDescriptor& descriptor,
    TStringBuf spec);

void ToProto(NYT::NProto::TWorkloadDescriptor* protoDescriptor, const TWorkloadDescriptor& descriptor);
void FromProto(TWorkloadDescriptor* descriptor, const NYT::NProto::TWorkloadDescriptor& protoDescriptor);

void Serialize(const TWorkloadDescriptor& descriptor, NYson::IYsonConsumer* consumer);
void Deserialize(TWorkloadDescriptor& descriptor, NYTree::INodePtr node);
void Deserialize(TWorkloadDescriptor& descriptor, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
