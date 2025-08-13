#pragma once

#include "workload.h"

namespace NYdbWorkload {

class TWorkloadDataInitializerBase::TDataGenerator final: public IBulkDataGenerator {
public:
    enum class EPortionSizeUnit {
        Byte,
        Line
    };

    explicit TDataGenerator(
        const TWorkloadDataInitializerBase& owner,
        const TString& name, ui64 size,
        const TString& tablePath,
        const TFsPath& dataPath,
        const TVector<TString>& columnNames = {},
        EPortionSizeUnit sizeUnit = EPortionSizeUnit::Byte
    );
    virtual TDataPortions GenerateDataPortion() override;

private:
    class TFile : public TSimpleRefCount<TFile> {
    public:
        using TPtr = TIntrusivePtr<TFile>;
        TFile(TDataGenerator& owner, const TString& path);
        virtual ~TFile() = default;
        virtual TDataPortionPtr GetPortion() = 0;

    protected:
        TDataGenerator& Owner;
        TString Path;
        TAdaptiveLock Lock;
    };
    class TCsvFileBase;
    class TTsvFile;
    class TCsvFile;
    class TParquetFile;
    void AddFile(const TFsPath& path);

private:
    const TWorkloadDataInitializerBase& Owner;
    TString TablePath;
    TVector<TString> ColumnNames;
    TVector<TFile::TPtr> Files;
    ui32 FilesCount = 0;
    TAdaptiveLock Lock;
    bool FirstPortion = true;
    EPortionSizeUnit SizeUnit;
};

}
