#pragma once

#include "tpch.h"
#include <library/cpp/object_factory/object_factory.h>

namespace NYdbWorkload {

class TTpchWorkloadDataInitializerGenerator: public TWorkloadDataInitializerBase {
public:
    TTpchWorkloadDataInitializerGenerator(const TTpchWorkloadParams& params);
    void ConfigureOpts(NLastGetopt::TOpts& opts) override;
    YDB_READONLY(ui64, Scale, 1);
    YDB_READONLY_DEF(TSet<TString>, Tables);
    YDB_READONLY(ui32, ProcessIndex, 0);
    YDB_READONLY(ui32, ProcessCount, 1);
protected:
    TBulkDataGeneratorList DoGetBulkInitialData() override;

public:
    class TBulkDataGenerator : public IBulkDataGenerator {
    public:
        using TFactory = NObjectFactory::TParametrizedObjectFactory<TBulkDataGenerator, TString, const TTpchWorkloadDataInitializerGenerator&>;

        TBulkDataGenerator(const TTpchWorkloadDataInitializerGenerator& owner, int tableNum);
        TDataPortions GenerateDataPortion() override;

        bool operator < (const TBulkDataGenerator& other) const {
            return TableNum < other.TableNum;
        }

    protected:
        class TContext {
        public:
            TContext(const TBulkDataGenerator& owner, int tableNum, TGeneratorStateProcessor* state);
            NYdb::TValueBuilder& GetBuilder();
            TStringBuilder& GetCsv();
            void AppendPortions(TDataPortions& result);
            YDB_ACCESSOR(ui64, Count, 0);
            YDB_ACCESSOR(ui64, Start, 0);

        private:
            THolder<NYdb::TValueBuilder> Builder;
            TStringBuilder Csv;
            const TBulkDataGenerator& Owner;
            int TableNum;
            TGeneratorStateProcessor* State;
        };

        using TContexts = TVector<TContext>;

        virtual void GenerateRows(TContexts& ctxs) = 0;

        int TableNum;
        ui64 Generated = 0;
        TAdaptiveLock NumbersLock;
        TAdaptiveLock DriverLock;

    private:
        TString GetFullTableName(const char* table) const;
        static ui64 CalcCountToGenerate(const TTpchWorkloadDataInitializerGenerator& owner, int tableNum, bool useState);
        const TTpchWorkloadDataInitializerGenerator& Owner;
        ui64 TableSize;
    };

};

}