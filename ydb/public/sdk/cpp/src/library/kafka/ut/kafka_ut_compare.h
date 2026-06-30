#pragma once

#include <ydb/public/sdk/cpp/src/library/kafka/kafka.h>

#include <util/generic/strbuf.h>

namespace NKafka {

inline bool KafkaBytesEqual(const TKafkaBytes& bytes, TStringBuf expected) {
    return bytes && TStringBuf(bytes->data(), bytes->size()) == expected;
}

inline bool KafkaBytesEqual(const TKafkaBytes& lhs, const TKafkaBytes& rhs) {
    if (!lhs || !rhs) {
        return !lhs && !rhs;
    }
    return lhs->size() == rhs->size()
        && memcmp(lhs->data(), rhs->data(), lhs->size()) == 0;
}

}
