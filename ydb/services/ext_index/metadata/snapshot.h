#pragma once
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/library/accessor/accessor.h>
#include "object.h"

namespace NKikimr::NMetadata::NCSIndex {

class TSnapshot: public NFetcher::ISnapshot {
private:
    using TBase = NFetcher::ISnapshot;
    using TObjects = std::map<TString, std::vector<TObject>>;
    YDB_READONLY_DEF(TObjects, Objects);
protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) override;
    virtual TString DoSerializeToString() const override;
public:
    using TBase::TBase;

    std::vector<TObject> GetIndexes(const TString& tablePath) const;
    void GetObjectsForActivity(std::vector<TObject>& activation, std::vector<TObject>& remove) const;
};

}
