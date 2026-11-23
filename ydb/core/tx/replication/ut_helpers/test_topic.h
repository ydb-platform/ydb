#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>
#include <optional>

namespace NKikimrSchemeOp {
    class TPersQueueGroupDescription;
}

namespace NKikimr::NReplication::NTestHelpers {

struct TTestTopicDescription {
    TString Name;

    void SerializeTo(NKikimrSchemeOp::TPersQueueGroupDescription& proto) const;
};

THolder<NKikimrSchemeOp::TPersQueueGroupDescription> MakeTopicDescription(const TTestTopicDescription& desc);

class TFeatureFlags;
template <bool UseDatabase> class TEnv;

TFeatureFlags TopicReplicationFlags();
void WaitForReplication(TEnv<true>& env, const std::function<bool()>& ready,
    const TString& description = "topic replication");

struct TTopicOffsets {
    ui64 Start;
    ui64 End;
    ui64 Committed;
};

std::optional<TTopicOffsets> GetTopicOffsets(TEnv<true>& env, const TString& topic, const TString& consumer);

struct TTopicRecord {
    TString Data;
    ui64 Offset;
    TString Producer;
    ui64 SeqNo;
};

TVector<TTopicRecord> ReadTopic(TEnv<true>& env, const TString& topic, size_t count, ui64 firstOffset = 0);

}
