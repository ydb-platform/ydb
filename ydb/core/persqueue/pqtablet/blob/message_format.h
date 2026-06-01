#pragma once

#include "blob.h"

#include <ydb/core/protos/msgbus_pq.pb.h>

namespace NKikimr::NPQ {

inline EMessageFormat FromProtoMessageFormat(NKikimrClient::EMessageFormat format) {
    switch (format) {
        case NKikimrClient::STANDARD:
            return EMessageFormat::STANDARD;
        case NKikimrClient::KAFKA_BATCH:
            return EMessageFormat::KAFKA_BATCH;
    }
    Y_ABORT("Unknown NKikimrClient::EMessageFormat");
}

inline NKikimrClient::EMessageFormat ToProtoMessageFormat(EMessageFormat format) {
    switch (format) {
        case EMessageFormat::STANDARD:
            return NKikimrClient::STANDARD;
        case EMessageFormat::KAFKA_BATCH:
            return NKikimrClient::KAFKA_BATCH;
        case EMessageFormat::COUNT:
            break;
    }
    Y_ABORT("Unknown EMessageFormat");
}

} // namespace NKikimr::NPQ
