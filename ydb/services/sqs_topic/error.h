#pragma once

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <ydb/services/datastreams/codes/datastreams_codes.h>

#include <util/generic/string.h>

namespace NKikimr::NSqsTopic::V1 {

    struct TMessageError {
        Ydb::StatusIds::StatusCode StatusCode = Ydb::StatusIds::SUCCESS;
        NYds::EErrorCodes IssueCode = NYds::EErrorCodes::OK;
        TString Text;
    };
} // namespace NKikimr::NSqsTopic::V1
