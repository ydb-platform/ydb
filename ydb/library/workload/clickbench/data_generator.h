#pragma once

#include "clickbench.h"

namespace NYdbWorkload {

class TClickbenchWorkloadDataInitializerGenerator: public TWorkloadDataInitializerBase {
public:
    TClickbenchWorkloadDataInitializerGenerator(const TClickbenchWorkloadParams& params);
    void ConfigureOpts(NLastGetopt::TOpts& opts) override;
    YDB_READONLY_DEF(TFsPath, DataFiles);

protected:
    TBulkDataGeneratorList DoGetBulkInitialData() override;

private:
    class TDataGenerartor final: public IBulkDataGenerator {
    public:
        explicit TDataGenerartor(const TClickbenchWorkloadDataInitializerGenerator& owner);
        virtual TDataPortions GenerateDataPortion() override;

    private:
        class TFile : public TSimpleRefCount<TFile> {
        public:
            using TPtr = TIntrusivePtr<TFile>;
            TFile(TDataGenerartor& owner)
                : Owner(owner)
            {}
            virtual ~TFile() = default;
            virtual TDataPortionPtr GetPortion() = 0;

        protected:
            TDataGenerartor& Owner;
        };
        class TCsvFileBase;
        class TTsvFile;
        class TCsvFile;
        void AddFile(const TFsPath& path);
        static ui64 CalcSize(const TClickbenchWorkloadDataInitializerGenerator& owner);

    private:
        const TClickbenchWorkloadDataInitializerGenerator& Owner;
        TVector<TFile::TPtr> Files;
        TAdaptiveLock Lock;
    };
};

}