#pragma once
#include "udf_meta.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NUdfStore {

class TSnapshot: public NMetadata::NFetcher::ISnapshot {
private:
    using TBase = NMetadata::NFetcher::ISnapshot;
    using TUdfs = std::map<TString, TUdfMeta>;
    YDB_READONLY_DEF(TUdfs, Udfs);
protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) override;
    virtual TString DoSerializeToString() const override;
public:
    using TBase::TBase;

    const TUdfMeta* GetUdfByMd5(const TString& name) const;
    std::vector<TString> GetUdfMd5s() const;
};

} // namespace NKikimr::NUdfStore
