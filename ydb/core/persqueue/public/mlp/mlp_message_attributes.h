#pragma once

#include <ydb/core/persqueue/public/constants.h>

#include <util/generic/strbuf.h>

namespace NKikimr::NPQ::NMLP  {
    namespace NMessageConsts {
        constexpr TStringBuf MessageDeduplicationId = "message_deduplication_id";
        constexpr TStringBuf MessageAttributes = "__message_attributes";
        constexpr TStringBuf DelaySeconds = "__delay_seconds";
    } // namespace NMessageConsts
} // namespace NKikimr::NPQ::NMLP
