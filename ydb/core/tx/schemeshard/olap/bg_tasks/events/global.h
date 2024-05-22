#pragma once
#include "common.h"
#include <ydb/core/tx/schemeshard/olap/bg_tasks/protos/data.pb.h>

namespace NKikimr::NSchemeShard::NBackground {

struct TEvListRequest: public TEventPB<TEvListRequest, NKikimrSchemeShardTxBackgroundProto::TEvListRequest, EvListRequest> {
    TEvListRequest() = default;

    explicit TEvListRequest(const TString& dbName, ui64 pageSize, TString pageToken) {
        Record.SetDatabaseName(dbName);
        Record.SetPageSize(pageSize);
        Record.SetPageToken(pageToken);
    }
};

struct TEvListResponse: public TEventPB<TEvListResponse, NKikimrSchemeShardTxBackgroundProto::TEvListResponse, EvListResponse> {
private:
    using TBase = TEventPB<TEvListResponse, NKikimrSchemeShardTxBackgroundProto::TEvListResponse, EvListResponse>;
public:
    using TBase::TBase;
    TEvListResponse(const NKikimrSchemeShardTxBackgroundProto::TEvListResponse& proto)
        : TBase(proto)
    {

    }
};


}