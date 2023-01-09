#pragma once
#include "object.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NInitializer {

class TSnapshot: public NFetcher::ISnapshot {
private:
    using TBase = NFetcher::ISnapshot;
    using TObjects = std::map<TDBInitializationKey, TDBInitialization>;
    YDB_READONLY_DEF(TObjects, Objects);
protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) override;
    virtual TString DoSerializeToString() const override;
public:
    bool HasComponent(const TString& componentId) const;

    using TBase::TBase;
};

}
