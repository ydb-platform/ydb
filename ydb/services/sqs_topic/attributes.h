#pragma once

#include <util/generic/strbuf.h>

namespace NKikimr::NSqsTopic::V1 {
    namespace NMessageConsts {

        constexpr TStringBuf MessageDeduplicationId = "__MessageDeduplicationId";
        constexpr TStringBuf MessageAttributes = "__MessageAttributes";
        constexpr TStringBuf DelaySeconds = "__DelaySeconds";
        constexpr TStringBuf MessageId = "__MessageId";
    } // namespace NMessageConsts
} // namespace NKikimr::NSqsTopic::V1
