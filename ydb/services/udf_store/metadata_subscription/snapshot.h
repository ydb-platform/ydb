#pragma once
#include "udf_meta.h"
#include "library_source.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NUdfStore {

class TSnapshot: public NMetadata::NFetcher::ISnapshot {
private:
    using TBase = NMetadata::NFetcher::ISnapshot;
    using TUdfs = std::map<TString, TUdfMeta>;
    using TLibraries = std::map<TString, TUdfLibrarySource>;
    YDB_READONLY_DEF(TUdfs, Udfs);
    YDB_READONLY_DEF(TLibraries, Libraries);
protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) override;
    virtual TString DoSerializeToString() const override;
public:
    using TBase::TBase;

    const TUdfMeta* GetUdfByMd5(const TString& name) const;
    std::vector<TString> GetUdfMd5s() const;
    const TUdfLibrarySource* GetLibraryByName(const TString& name) const;
    std::vector<TString> GetLibraryNames() const;
};

} // namespace NKikimr::NUdfStore
