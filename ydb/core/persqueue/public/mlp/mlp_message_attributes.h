#pragma once

#include <ydb/core/persqueue/public/constants.h>

#include <util/generic/strbuf.h>

namespace NKikimr::NPQ::NMLP  {
    namespace NMessageConsts {
        constexpr TStringBuf MessageDeduplicationId = "__MessageDeduplicationId";
        constexpr TStringBuf MessageAttributes = "__MessageAttributes";
        constexpr TStringBuf DelaySeconds = "__DelaySeconds";
    } // namespace NMessageConsts
} // namespace NKikimr::NPQ::NMLP
