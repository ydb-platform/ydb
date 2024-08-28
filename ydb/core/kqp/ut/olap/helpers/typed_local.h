#pragma once
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/formats/arrow/simple_builder/array.h>
#include <ydb/core/formats/arrow/simple_builder/batch.h>
#include <ydb/core/formats/arrow/simple_builder/filler.h>

#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NKqp {

class TTypedLocalHelper: public Tests::NCS::THelper {
private:
    using TBase = Tests::NCS::THelper;
    const TString TypeName;
    TKikimrRunner& KikimrRunner;
    const TString TablePath;
    const TString TableName;
    const TString StoreName;
protected:
    virtual TString GetTestTableSchema() const override;
    virtual std::vector<TString> GetShardingColumns() const override {
        return { "pk_int" };
    }
public:
    TTypedLocalHelper(const TString& typeName, TKikimrRunner& kikimrRunner, const TString& tableName = "olapTable", const TString& storeName = "olapStore")
        : TBase(kikimrRunner.GetTestServer())
        , TypeName(typeName)
        , KikimrRunner(kikimrRunner)
        , TablePath(storeName.empty() ? "/Root/" + tableName : "/Root/" + storeName + "/" + tableName)
        , TableName(tableName)
        , StoreName(storeName) {
        SetShardingMethod("HASH_FUNCTION_CONSISTENCY_64");
    }

    void ExecuteSchemeQuery(const TString& alterQuery, const NYdb::EStatus expectedStatus = NYdb::EStatus::SUCCESS) const;

    TString GetQueryResult(const TString& request) const;

    void PrintCount();

    class TDistribution {
    private:
        YDB_READONLY(ui32, Count, 0);
        YDB_READONLY(ui32, MinCount, 0);
        YDB_READONLY(ui32, MaxCount, 0);
        YDB_READONLY(ui32, GroupsCount, 0);
    public:
        TDistribution(const ui32 count, const ui32 minCount, const ui32 maxCount, const ui32 groupsCount)
            : Count(count)
            , MinCount(minCount)
            , MaxCount(maxCount)
            , GroupsCount(groupsCount) {

        }

        TString DebugString() const {
            return TStringBuilder()
                << "count=" << Count << ";"
                << "min_count=" << MinCount << ";"
                << "max_count=" << MaxCount << ";"
                << "groups_count=" << GroupsCount << ";";
        }
    };

    TDistribution GetDistribution(const bool verbose = false);

    void GetVolumes(ui64& rawBytes, ui64& bytes, const bool verbose = false, const std::vector<TString> columnNames = {});

    void GetStats(std::vector<NJson::TJsonValue>& stats, const bool verbose = false);

    void GetCount(ui64& count);

    template <class TFiller>
    void FillTable(const TFiller& fillPolicy, const double pkKff = 0, const ui32 numRows = 800000) const {
        std::vector<NArrow::NConstruction::IArrayBuilder::TPtr> builders;
        builders.emplace_back(NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::Int64Type>>::BuildNotNullable("pk_int", numRows * pkKff));
        builders.emplace_back(std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<TFiller>>("field", fillPolicy));
        NArrow::NConstruction::TRecordBatchConstructor batchBuilder(builders);
        std::shared_ptr<arrow::RecordBatch> batch = batchBuilder.BuildBatch(numRows);
        TBase::SendDataViaActorSystem(TablePath, batch);
    }

    void FillPKOnly(const double pkKff = 0, const ui32 numRows = 800000) const;

    void CreateTestOlapTable(ui32 storeShardsCount = 4, ui32 tableShardsCount = 3) {
        CreateOlapTableWithStore(TableName, StoreName, storeShardsCount, tableShardsCount);
    }
};

}