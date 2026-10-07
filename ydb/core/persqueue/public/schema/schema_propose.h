#pragma once

#include "schema.h"

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <utility>

namespace NKikimr::NPQ::NSchema {

using TResultBase = std::pair<Ydb::StatusIds::StatusCode, TString>;

struct TResult : public TResultBase {

    TResult()
        : TResultBase(Ydb::StatusIds::SUCCESS, TString())
    {
    }

    TResult(Ydb::StatusIds::StatusCode YdbCode, TString&& errorMessage)
        : TResultBase(YdbCode, std::move(errorMessage))
    {
    }

    Ydb::StatusIds::StatusCode GetStatus() const {
        return first;
    }

    TString& GetErrorMessage() {
        return second;
    }

    explicit operator bool() const {
        return GetStatus() == Ydb::StatusIds::SUCCESS;
    }
};

TResult ProposeCreateTopic(
    NKikimrSchemeOp::TModifyScheme& modifyScheme,
    Ydb::Topic::CreateTopicRequest request,
    const TString& database,
    const TString& workingDir,
    const TString& name
);

} // namespace NKikimr::NPQ::NSchema
