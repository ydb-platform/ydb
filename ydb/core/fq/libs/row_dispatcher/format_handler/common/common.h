#pragma once

#include <ydb/library/conclusion/generic/result.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>

namespace NFq::NRowDispatcher {

using EStatusId = NYql::NDqProto::StatusIds;
using TStatusCode = EStatusId::StatusCode;
using TStatus = NKikimr::TYQLConclusionSpecialStatus<TStatusCode, EStatusId::SUCCESS, EStatusId::INTERNAL_ERROR>;

template <typename TValue>
using TValueStatus = NKikimr::TConclusionImpl<TStatus, TValue>;

struct TSchemaColumn {
    TString Name;
    TString TypeYson;

    bool operator==(const TSchemaColumn& other) const = default;

    TString ToString() const;
};

}  // namespace NFq::NRowDispatcher
