#pragma once

#include "common.h"

#include <ydb/core/base/appdata.h>

#include <ydb/library/conclusion/result.h>
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

    virtual TConclusion<TBaseObject::TPtr> ApplyPatch(const TBaseObject::TPtr& object, const NInternal::TTableRecord& patch) const = 0;

    virtual TString GetTypeId() const = 0;

    virtual ~IObjectManager() = default;
};

template <class TObject>
class TObjectManager: public IObjectManager {
private:
    std::shared_ptr<TObject> CastVerified(const TBaseObject::TPtr& object) const {
        AFL_VERIFY(object)("type_id", TObject::GetTypeId());
        auto specificObject = std::dynamic_pointer_cast<TObject>(object);
        AFL_VERIFY(specificObject)("type_id", TObject::GetTypeId());
        return specificObject;
    }

public:
    TBaseObject::TPtr DeserializeFromRecord(const NInternal::TTableRecord& value) const override {
        auto recordSet = value.BuildRecordSet();
        AFL_VERIFY(recordSet.rowsSize() == 1);
        typename TObject::TDecoder decoder(recordSet);
        auto result = std::make_shared<TObject>();
        if (!result->DeserializeFromRecord(decoder, recordSet.Getrows(0))) {
            return nullptr;
        }
        return result;
    }

    NInternal::TTableRecord SerializeToRecord(const TBaseObject::TPtr& object) const override {
        return CastVerified(object)->SerializeToRecord();
    }

    TConclusion<TBaseObject::TPtr> ApplyPatch(const TBaseObject::TPtr& object, const NInternal::TTableRecord& patch) const override {
        NInternal::TTableRecord record = SerializeToRecord(object);
        if (auto status = object->MergeRecords(record, patch); status.IsFail()) {
            return status;
        }
        return DeserializeFromRecord(record);
    }

    TString GetTypeId() const override {
        return TObject::GetTypeId();
    }
};

template <typename T>
concept RecordSerializableObject = std::derived_from<T, TBaseObject> && requires {
    { T::GetTypeId() } -> std::same_as<TString>;
} && requires(const T t) {
    { t.SerializeToRecord() } -> std::same_as<NMetadata::NInternal::TTableRecord>;
} && requires(T t, const T::TDecoder& decoder, const Ydb::Value& value) {
    { t.DeserializeFromRecord(decoder, value) } -> std::same_as<bool>;
} && requires(const T t, NInternal::TTableRecord& value, const NInternal::TTableRecord& patch) {
    { t.MergeRecords(value, patch) } -> std::same_as<TConclusionStatus>;
};

template <class TDerived>
class TObject: public TBaseObject {
private:
    static inline const auto ObjectManager = std::make_shared<TObjectManager<TDerived>>();

protected:
    TObject() {
        static_assert(RecordSerializableObject<TDerived>, "Object implementations must satisfy RecordSerializableObject");
    }

public:
    static Ydb::Table::CreateTableRequest AddHistoryTableScheme(const Ydb::Table::CreateTableRequest& baseScheme) {
        return TBaseObject::AddHistoryTableScheme(baseScheme, TDerived::GetBehaviour()->GetStorageHistoryTablePath());
    }

    IObjectManager::TPtr GetObjectManager() const override {
        return ObjectManager;
    }
};
}
