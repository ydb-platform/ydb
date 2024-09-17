// TODO: remove file
#pragma once
#include "object.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NColumnShard::NTiers {

class TSnapshot: public NMetadata::NFetcher::ISnapshot {
private:
    using TBase = NMetadata::NFetcher::ISnapshot;
    using TTiers = std::map<TString, TTierConfig>;
    YDB_READONLY_DEF(TTiers, Tiers);

protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) override;
    virtual TString DoSerializeToString() const override {
        return DebugString();
    }

private:
    TString DebugString() const;

public:
    using TBase::TBase;
};

}
