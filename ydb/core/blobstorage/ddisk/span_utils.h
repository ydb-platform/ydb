#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/wilson/wilson_span.h>

namespace NKikimr::NDDisk::NPrivate {

    inline void AddMessageWaitAttributes(NWilson::TSpan& span) {
        if (!span) {
            return;
        }

        const auto messageWaitUs =
            static_cast<i64>(TActivationContext::GetCurrentEventDeliveryTimeUs());
        const auto mailboxQueueWaitUs =
            static_cast<i64>(TActivationContext::GetCurrentActivationTimeUs());

        span
            .Attribute("message_wait_us", messageWaitUs)
            .Attribute("mailbox_queue_wait_us", mailboxQueueWaitUs);
    }

} // NKikimr::NDDisk::NPrivate
