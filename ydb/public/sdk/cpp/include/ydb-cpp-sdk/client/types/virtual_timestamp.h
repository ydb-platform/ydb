#pragma once

#include <ydb/public/api/protos/ydb_common.pb.h>

#include <util/stream/output.h>

#include <tuple>

namespace NYdb::inline Dev {
namespace NScheme {

struct TVirtualTimestamp {
    uint64_t PlanStep = 0;
    uint64_t TxId = 0;

    TVirtualTimestamp() = default;
    TVirtualTimestamp(uint64_t planStep, uint64_t txId);
    TVirtualTimestamp(const ::Ydb::VirtualTimestamp& proto);

    std::string ToString() const;
    void Out(IOutputStream& out) const;

    bool operator<(const TVirtualTimestamp& rhs) const;
    bool operator<=(const TVirtualTimestamp& rhs) const;
    bool operator>(const TVirtualTimestamp& rhs) const;
    bool operator>=(const TVirtualTimestamp& rhs) const;
    bool operator==(const TVirtualTimestamp& rhs) const;
    bool operator!=(const TVirtualTimestamp& rhs) const;
};

} // namespace NScheme
} // namespace NYdb
