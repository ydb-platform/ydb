#pragma once

#include "public.h"

#include <yt/yt/client/object_client/public.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <optional>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

struct TTimestampMap
{
    TCompactVector<std::pair<NObjectClient::TCellTag, TTimestamp>, 4> Timestamps;

    std::optional<TTimestamp> FindTimestamp(NObjectClient::TCellTag) const;
    TTimestamp GetTimestamp(NObjectClient::TCellTag) const;

    void Persist(const TStreamPersistenceContext& context);
};

void ToProto(NProto::TTimestampMap* protoMap, const TTimestampMap& map);
void FromProto(TTimestampMap* map, const NProto::TTimestampMap& protoMap);

void FormatValue(TStringBuilderBase* builder, const TTimestampMap& map, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
