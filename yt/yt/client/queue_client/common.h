#pragma once

#include <yt/yt/core/misc/common.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): Replace this with TRichYPath in YT-18038.
struct TCrossClusterReference
{
    TString Cluster;
    NYPath::TYPath Path;

    bool operator==(const TCrossClusterReference& other) const;
    bool operator<(const TCrossClusterReference& other) const;

    operator NYPath::TRichYPath() const;

    static TCrossClusterReference FromString(TStringBuf path);
    static TCrossClusterReference FromRichYPath(const NYPath::TRichYPath& path);
};

void FormatValue(TStringBuilderBase* builder, const TCrossClusterReference& queueRef, TStringBuf spec);

void Serialize(const TCrossClusterReference& queueRef, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TProfilingTags
{
    TString Cluster;
    TString LeadingStatus;
    TString QueueAgentStage;
    TString ObjectType;

    bool operator==(const TProfilingTags& other) const = default;
    bool operator<(const TProfilingTags& other) const = default;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NQueueClient::TCrossClusterReference>
{
    size_t operator()(const NYT::NQueueClient::TCrossClusterReference& crossClusterRef) const;
};

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NQueueClient::TProfilingTags>
{
    size_t operator()(const NYT::NQueueClient::TProfilingTags& tag) const;
};

////////////////////////////////////////////////////////////////////////////////
