#pragma once
#include <ydb/services/metadata/service.h>

namespace NKikimr::NColumnShard {

class TCSKVSnapshot: public NMetadataProvider::ISnapshot {
private:
    using TBase = NMetadataProvider::ISnapshot;
    using TKVData = TMap<TString, TString>;
    YDB_READONLY_DEF(TKVData, Data);
protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::ResultSet& rawData) override;
    virtual TString DoSerializeToString() const override {
        TStringBuilder sb;
        for (auto&& i : Data) {
            sb << i.first << "=" << i.second << ";";
        }
        return sb;
    }
public:
    std::optional<TString> GetValue(const TString& key) const;

    using TBase::TBase;
};

class TSnapshotConstructor: public NMetadataProvider::TGenericSnapshotParser<TCSKVSnapshot> {
private:
    const TString TablePath;
protected:
    virtual Ydb::Table::CreateTableRequest DoGetTableSchema() const override;
    virtual const TString& DoGetTablePath() const override {
        return TablePath;
    }
public:
    TSnapshotConstructor(const TString& tablePath)
        : TablePath(tablePath)
    {

    }
};

}
