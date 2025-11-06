#pragma once
#include "error.h"
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <util/generic/string.h>

namespace NKikimr::NSqsTopic::V1 {

    struct TMappedDescriberError {
        NKikimr::NPQ::NDescriber::EStatus DescriberStatus{};
        TMessageError Error; // or ok
    };

    TMappedDescriberError MapDescriberStatus(const TString& topicPath, NKikimr::NPQ::NDescriber::EStatus status);
} // namespace NKikimr::NSqsTopic::V1
