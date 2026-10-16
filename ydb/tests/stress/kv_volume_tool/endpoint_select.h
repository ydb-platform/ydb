#pragma once

#include <ydb/public/api/protos/ydb_discovery.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NKvVolumeTool {

constexpr int TryNextAddress = -1;
constexpr int TryNextNode = -2;

bool IsFailover(int code);

struct TSelectedNode {
    TVector<TString> Addresses;
    TString SslTargetNameOverride;
};

struct TFailoverPosition {
    size_t Node = 0;
    size_t Address = 0;
    size_t NodesEntered = 1;
};

TString FormatHostPort(const TString& host, ui32 port);

TSelectedNode SelectNode(const Ydb::Discovery::EndpointInfo& info, bool useTls);
bool AdvanceAfterFailover(TFailoverPosition& position, int code, const TVector<size_t>& addressCounts);

} // namespace NKvVolumeTool
