#pragma once
#include "object.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadataInitializer {

class TSnapshot: public NMetadataProvider::ISnapshot {
private:
    using TBase = NMetadataProvider::ISnapshot;
    using TObjects = std::map<TDBInitializationKey, TDBInitialization>;
    YDB_READONLY_DEF(TObjects, Objects);
protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) override;
    virtual TString DoSerializeToString() const override;
public:
    using TBase::TBase;
};

}
