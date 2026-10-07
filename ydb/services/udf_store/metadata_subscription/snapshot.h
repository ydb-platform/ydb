#pragma once
#include "udf_module.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NUdfStore {

class TSnapshot: public NMetadata::NFetcher::ISnapshot {
private:
    using TBase = NMetadata::NFetcher::ISnapshot;
    using TUdfs = std::map<TString, TUdfModule>;
    using TLibraries = std::map<TString, TUdfModule>;
    YDB_READONLY_DEF(TUdfs, Udfs);
    YDB_READONLY_DEF(TLibraries, Libraries);
protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) override;
    virtual TString DoSerializeToString() const override;
public:
    using TBase::TBase;

    const TUdfModule* GetUdfByName(const TString& name) const;
    std::vector<TString> GetUdfNames() const;
    const TUdfModule* GetLibraryByName(const TString& name) const;
    std::vector<TString> GetLibraryNames() const;
};

} // namespace NKikimr::NUdfStore
