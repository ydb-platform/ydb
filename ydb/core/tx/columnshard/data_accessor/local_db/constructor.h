#pragma once
#include <ydb/core/tx/columnshard/data_accessor/abstract/constructor.h>

namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB {

class TManagerConstructor: public IManagerConstructor {
public:
    static TString GetClassNameStatic() {
        return "local_db";
    }

private:
    ui64 MemoryCacheSize = (ui64)128 << 20;
    bool FetchOnStart = false;

    virtual TConclusion<std::shared_ptr<IMetadataMemoryManager>> DoBuild(const TManagerConstructionContext& context) const override;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonValue) override {
        if (jsonValue.Has("memory_cache_size")) {
            if (!jsonValue["memory_cache_size"].IsUInteger()) {
                return TConclusionStatus::Fail("'memory_cache_size' have to been uint64");
            }
            MemoryCacheSize = jsonValue["memory_cache_size"].GetUIntegerRobust();
        }

        if (jsonValue.Has("fetch_on_start")) {
            if (!jsonValue["fetch_on_start"].IsBoolean()) {
                return TConclusionStatus::Fail("'fetch_on_start' have to been boolean");
            }
            FetchOnStart = jsonValue["fetch_on_start"].GetBoolean();
        }

        return TConclusionStatus::Success();
    }
    virtual bool DoDeserializeFromProto(const TProto& proto) override {
        if (!proto.HasLocalDB()) {
            return true;
        }
        if (proto.GetLocalDB().HasMemoryCacheSize()) {
            MemoryCacheSize = proto.GetLocalDB().GetMemoryCacheSize();
        }
        if (proto.GetLocalDB().HasFetchOnStart()) {
            FetchOnStart = proto.GetLocalDB().GetFetchOnStart();
        }
        
        return true;
    }
    virtual void DoSerializeToProto(TProto& proto) const override {
        proto.MutableLocalDB()->SetMemoryCacheSize(MemoryCacheSize);
        proto.MutableLocalDB()->SetFetchOnStart(FetchOnStart);
        return;
    }
    static const inline TFactory::TRegistrator<TManagerConstructor> Registrator =
        TFactory::TRegistrator<TManagerConstructor>(GetClassNameStatic());

public:
    static std::shared_ptr<TManagerConstructor> BuildDefault() {
        auto result = std::make_shared<TManagerConstructor>();
        result->MemoryCacheSize = Max<ui32>();
        result->FetchOnStart = true;
        return result;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB
