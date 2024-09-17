// TODO: remove file
#pragma once
#include "object.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NColumnShard::NTiers {

class TTieringRulesSnapshot: public NMetadata::NFetcher::ISnapshot {
private:
    using TBase = NMetadata::NFetcher::ISnapshot;
    using TRules = std::map<TString, TTieringRule>;
    YDB_READONLY_DEF(TRules, Rules);

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
