#pragma once

#include "common.h"

#include <ydb/core/base/appdata.h>

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/services/metadata/manager/table_record.h>

namespace NKikimr::NMetadata::NModifications {

class IObjectManager;

class TBaseObject : public IRecordsMerger {
protected:
    virtual IColumnValuesMerger::TPtr BuildMerger(const TString& columnName) const override;

public:
    using TPtr = std::shared_ptr<TBaseObject>;

    static Ydb::Table::CreateTableRequest AddHistoryTableScheme(const Ydb::Table::CreateTableRequest& baseScheme, const TString& tableName);
    virtual TConclusionStatus MergeRecords(NInternal::TTableRecord& value, const NInternal::TTableRecord& patch) const override;

    virtual std::shared_ptr<IObjectManager> GetObjectManager() const = 0;
};

class IObjectManager {
public:
    using TPtr = std::shared_ptr<IObjectManager>;

    virtual TBaseObject::TPtr DeserializeFromRecord(const NInternal::TTableRecord& value) const = 0;
    virtual NInternal::TTableRecord SerializeToRecord(const TBaseObject::TPtr& value) const = 0;

    virtual TBaseObject::TPtr ApplyPatch(const TBaseObject::TPtr object, const NInternal::TTableRecord& patch) const = 0;

    virtual TString GetTypeId() const = 0;

    virtual ~IObjectManager() = default;
};

template <class TObject>
struct TObjectManager: IObjectManager {
    TBaseObject::TPtr DeserializeFromRecord(const NInternal::TTableRecord& value) const override {
        TObject result;
        if (!TObject::TDecoder::DeserializeFromRecord(value.BuildRow(), result)) {
            return nullptr;
        }
        return result;
    }

    NInternal::TTableRecord SerializeToRecord(const TBaseObject::TPtr& object) const override {
        AFL_VERIFY(object);
        NInternal::TTableRecord record;
        TObject::TDecoder::SerializeToRecord(object, record);
        return record;
    }

    TBaseObject::TPtr ApplyPatch(const TBaseObject::TPtr object, const NInternal::TTableRecord& patch) const override {
        AFL_VERIFY(object);
        auto record = SerializeToRecord(object);
        return object->MergeRecords(record, patch);
    }

    TString GetTypeId() const override {
        return TObject::GetTypeId();
    }
};

template <class TDerived>
class TObject: public TBaseObject {
public:
    static Ydb::Table::CreateTableRequest AddHistoryTableScheme(const Ydb::Table::CreateTableRequest& baseScheme) {
        return TBaseObject::AddHistoryTableScheme(baseScheme, TDerived::GetBehaviour()->GetStorageHistoryTablePath());
    }

    IObjectManager::TPtr GetObjectManager() const override {
        return std::make_shared<TObjectManager<TDerived>>();
    }
};
}
