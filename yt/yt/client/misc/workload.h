#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT {

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
};

i64 GetBasicPriority(EWorkloadCategory category);

IInvokerPtr GetCompressionInvoker(const TWorkloadDescriptor& workloadDescriptor);

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
