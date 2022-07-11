#pragma once

#include "defs.h"

#include <ydb/core/base/counters.h>

namespace NKikimr {
namespace NDataShard {

class IStatHolder {
public:
    virtual ~IStatHolder() = default;

    virtual ui64 GetRows() const = 0;
    virtual ui64 GetBytes() const = 0;
};

class TUploadMonStats: public IStatHolder {
public:
    explicit TUploadMonStats() = default;
    explicit TUploadMonStats(const TString service,
                             const TString subsystem);

    void Aggr(ui64 rows, ui64 bytes);
    void Aggr(IStatHolder const* other);

    TString ToString() const;

    ui64 GetRows() const override final {
        return RowsSent;
    }

    ui64 GetBytes() const override final {
        return BytesSent;
    }

private:
    ui64 RowsSent = 0;
    ui64 BytesSent = 0;

    ::NMonitoring::TDynamicCounters::TCounterPtr MonRows;
    ::NMonitoring::TDynamicCounters::TCounterPtr MonBytes;
};

}}
