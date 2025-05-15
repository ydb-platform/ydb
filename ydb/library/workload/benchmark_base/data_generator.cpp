#include "data_generator.h"
#include <library/cpp/streams/factory/open_by_signature/factory.h>
#include <util/stream/file.h>

namespace NYdbWorkload {

TWorkloadDataInitializerBase::TDataGenerator::TDataGenerator(const TWorkloadDataInitializerBase& owner, const TString& name, ui64 size,
    const TString& tablePath, const TFsPath& dataPath, const TVector<TString>& columnNames)
    : IBulkDataGenerator(name, size)
    , Owner(owner)
    , TablePath(tablePath)
    , ColumnNames(columnNames)
{
    if (dataPath.IsDirectory()) {
        TVector<TFsPath> children;
        dataPath.List(children);
        for (const auto& ch: children) {
            AddFile(ch);
        }
    } else {
        AddFile(dataPath);
    }
}

IBulkDataGenerator::TDataPortions TWorkloadDataInitializerBase::TDataGenerator::GenerateDataPortion() {
    while (true) {
        TFile::TPtr file;
        with_lock(Lock) {
            if (!FilesCount) {
                return {};
            }
            if (FirstPortion) {
                FirstPortion = false;
                ui64 toSkip = 0;
                if (Owner.StateProcessor) {
                    for (const auto& [file, state]: Owner.StateProcessor->GetState()) {
                        toSkip += state.Position;
                    }
                }
                if (toSkip) {
                    return { MakeIntrusive<TDataPortion>(
                        Owner.Params.GetFullTableName(nullptr),
                        TDataPortion::TSkip(),
                        toSkip
                    )};
                }
            }
            if (!Files.empty()) {
                file = Files.back();
                Files.pop_back();
            }
        }
        if (!file) {
            Sleep(TDuration::MilliSeconds(100));
            continue;
        }
        if (auto result = file->GetPortion()) {
            auto g = Guard(Lock);
            Files.push_back(file);
            file.Drop();
            return {result};
        } else {
            auto g = Guard(Lock);
            --FilesCount;
        }
    }
}

class TWorkloadDataInitializerBase::TDataGenerator::TCsvFileBase: public TWorkloadDataInitializerBase::TDataGenerator::TFile {
public:
    TCsvFileBase(TDataGenerator& owner, const TString& path, const TString& delimiter, const TString& foramt)
        : TFile(owner)
        , Path(TFsPath(path).RealPath())
        , Decompressor(OpenOwnedMaybeCompressedInput(MakeHolder<TFileInput>(path)))
        , Header(JoinSeq(delimiter, owner.ColumnNames))
        , Foramt(foramt)
    {
        if (!Header) {
            Decompressor->ReadLine(Header);
        }
    }

    virtual TDataPortionPtr GetPortion() override final {
        TVector<TString> lines;
        lines.reserve(Owner.Owner.Params.BulkSize);
        with_lock(Lock) {
            TString line;
            if (Owner.Owner.StateProcessor && Owner.Owner.StateProcessor->GetState().contains(Path)) {
                auto position = Owner.Owner.StateProcessor->GetState().at(Path).Position;
                while(position > Readed && Decompressor->ReadLine(line)) {
                    ++Readed;
                }
            }
            while (Decompressor->ReadLine(line)) {
                lines.emplace_back(line);
                if (lines.size() >= Owner.Owner.Params.BulkSize) {
                    break;
                }
            }
        }
        if (lines.empty()) {
            return {};
        }


        TStringBuilder data;
        data.reserve(lines.size() * 10000);
        if (Header) {
            data << Header << Endl;
        }
        data << JoinSeq("\n", lines) << Endl;
        const ui64 position = Readed;
        Readed += lines.size();
        return MakeIntrusive<TDataPortionWithState>(
            Owner.Owner.StateProcessor.Get(),
            Owner.Owner.Params.GetFullTableName(Owner.TablePath.c_str()),
            Path,
            TDataPortion::TCsv(std::move(data), Foramt),
            position,
            lines.size()
        );
    }

private:
    TString Path;
    THolder<IInputStream> Decompressor;
    TString Header;
    const TString& Foramt;
    TAdaptiveLock Lock;
    ui64 Readed = 0;
};

class TWorkloadDataInitializerBase::TDataGenerator::TTsvFile final: public TWorkloadDataInitializerBase::TDataGenerator::TCsvFileBase {
public:
    TTsvFile(TDataGenerator& owner, const TString& path)
        : TCsvFileBase(owner, path, TWorkloadGeneratorBase::TsvDelimiter, TWorkloadGeneratorBase::TsvFormatString)
    {}
};

class TWorkloadDataInitializerBase::TDataGenerator::TCsvFile final: public TWorkloadDataInitializerBase::TDataGenerator::TCsvFileBase {
public:
    TCsvFile(TDataGenerator& owner, const TString& path)
        : TCsvFileBase(owner, path, TWorkloadGeneratorBase::CsvDelimiter, TWorkloadGeneratorBase::CsvFormatString)
    {}
};

void TWorkloadDataInitializerBase::TDataGenerator::AddFile(const TFsPath& path) {
    const auto name = path.GetName();
    if (name.EndsWith(".tsv") || name.EndsWith(".tsv.gz")) {
        Files.push_back(MakeIntrusive<TTsvFile>(*this, path));
    } else if (name.EndsWith(".csv") || name.EndsWith(".csv.gz")) {
        Files.push_back(MakeIntrusive<TCsvFile>(*this, path));
    }
    ++FilesCount;
}

}