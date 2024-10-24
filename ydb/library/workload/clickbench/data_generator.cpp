#include "data_generator.h"
#include <library/cpp/streams/factory/open_by_signature/factory.h>
#include <util/stream/file.h>
#include <thread>

namespace NYdbWorkload {

TClickbenchWorkloadDataInitializerGenerator::TClickbenchWorkloadDataInitializerGenerator(const TClickbenchWorkloadParams& params)
    : TWorkloadDataInitializerBase("files", "Upload Clickbench dataset from files.", params)
{}

void TClickbenchWorkloadDataInitializerGenerator::ConfigureOpts(NLastGetopt::TOpts& opts) {
    TWorkloadDataInitializerBase::ConfigureOpts(opts);
    opts.AddLongOption('i', "input",
        "File or Directory with clickbench dataset. If directory is set, all its available files will be used."
        "Now supported zipped and unzipped csv and tsv files, that may be downloaded here:  https://datasets.clickhouse.com/hits_compatible/hits.csv.gz, "
        "https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz. "
        "For better perfomanse you may split it to some parts for parrallel upload."
        ).StoreResult(&DataFiles);
}

TBulkDataGeneratorList TClickbenchWorkloadDataInitializerGenerator::DoGetBulkInitialData() {
    if (!DataFiles.IsDefined()) {
        throw yexception() << "'input' parameter must be set.";
    }
    if (!DataFiles.Exists()) {
        throw yexception() << "Invalid 'input' parameter, path does not exist: " << DataFiles;
    }
    return {std::make_shared<TDataGenerartor>(*this)};
}

TClickbenchWorkloadDataInitializerGenerator::TDataGenerartor::TDataGenerartor(const TClickbenchWorkloadDataInitializerGenerator& owner)
    : IBulkDataGenerator("hits", CalcSize(owner))
    , Owner(owner)
{
    if (Owner.GetDataFiles().IsDirectory()) {
        TVector<TFsPath> children;
        Owner.GetDataFiles().List(children);
        for (const auto& ch: children) {
            AddFile(ch);
        }
    } else {
        AddFile(Owner.GetDataFiles());
    }
}

IBulkDataGenerator::TDataPortions TClickbenchWorkloadDataInitializerGenerator::TDataGenerartor::GenerateDataPortion() {
    while (true) {
        size_t index;
        TFile::TPtr file;
        with_lock(Lock) {
            if (Files.empty()) {
                return {};
            }
            index = std::hash<std::thread::id>{}(std::this_thread::get_id()) % Files.size();
            file = Files[index];
        }
        if (auto result = file->GetPortion()) {
            return {result};
        }
        with_lock(Lock) {
            if (index < Files.size() && file == Files[index]) {
                if (index + 1 != Files.size()) {
                    Files[index].Swap(Files.back());
                }
                Files.pop_back();
            }
        }
    }
}

ui64 TClickbenchWorkloadDataInitializerGenerator::TDataGenerartor::CalcSize(const TClickbenchWorkloadDataInitializerGenerator& owner) {
    ui64 result = 99997497;
    if (owner.StateProcessor) {
        for (const auto& [file, state]: owner.StateProcessor->GetState()) {
            result -= state.Position;
        }
    }
    return result;
}

class TClickbenchWorkloadDataInitializerGenerator::TDataGenerartor::TCsvFileBase: public TClickbenchWorkloadDataInitializerGenerator::TDataGenerartor::TFile {
public:
    TCsvFileBase(TDataGenerartor& owner, const TString& path, const TString& delimiter, const TString& foramt)
        : TFile(owner)
        , Path(TFsPath(path).RealPath())
        , Decompressor(OpenOwnedMaybeCompressedInput(MakeHolder<TFileInput>(path)))
        , Delimiter(delimiter)
        , Foramt(foramt)
    {
        const auto schema = NResource::Find("click_bench_schema.sql");
        TStringInput si (schema);
        TVector<TString> header;
        header.reserve(105);
        TString line;
        ui32 field = 0;
        while (si.ReadLine(line)) {
            if (line.find("{notnull}") != TString::npos) {
                const auto parts = StringSplitter(line).Split(' ').SkipEmpty().ToList<TString>();
                header.push_back(parts[0]);
                if (parts[1].StartsWith("Date")) {
                    DateFileds.insert(field);
                }
                if (parts[1].StartsWith("Timestamp")) {
                    TsFileds.insert(field);
                }
                ++field;
            }
        }
        Header = JoinSeq(Delimiter, header);
    }

    virtual TDataPortionPtr GetPortion() override final {
        TVector<TString> lines;
        lines.reserve(Owner.Owner.Params.BulkSize);
        with_lock(Lock) {
            TString line;
            if (Owner.Owner.StateProcessor && Owner.Owner.StateProcessor->GetState().contains(Path)) {
                while(Owner.Owner.StateProcessor->GetState().at(Path).Position > Readed && Decompressor->ReadLine(line)) {
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
        data << Header << Endl;
        data << JoinSeq("\n", lines) << Endl;
        const ui64 position = Readed;
        Readed += lines.size();
        return MakeIntrusive<TDataPortionWithState>(
            Owner.Owner.StateProcessor.Get(),
            Owner.Owner.Params.GetFullTableName(nullptr),
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
    TSet<ui32> DateFileds;
    TSet<ui32> TsFileds;
    const TString& Delimiter;
    const TString& Foramt;
    TAdaptiveLock Lock;
    ui64 Readed = 0;
};

class TClickbenchWorkloadDataInitializerGenerator::TDataGenerartor::TTsvFile final: public TClickbenchWorkloadDataInitializerGenerator::TDataGenerartor::TCsvFileBase {
public:
    TTsvFile(TDataGenerartor& owner, const TString& path)
        : TCsvFileBase(owner, path, TWorkloadGeneratorBase::TsvDelimiter, TWorkloadGeneratorBase::TsvFormatString)
    {}
};

class TClickbenchWorkloadDataInitializerGenerator::TDataGenerartor::TCsvFile final: public TClickbenchWorkloadDataInitializerGenerator::TDataGenerartor::TCsvFileBase {
public:
    TCsvFile(TDataGenerartor& owner, const TString& path)
        : TCsvFileBase(owner, path, TWorkloadGeneratorBase::CsvDelimiter, TWorkloadGeneratorBase::CsvFormatString)
    {}
};

void TClickbenchWorkloadDataInitializerGenerator::TDataGenerartor::AddFile(const TFsPath& path) {
    const auto name = path.GetName();
    if (name.EndsWith(".tsv") || name.EndsWith(".tsv.gz")) {
        Files.push_back(MakeIntrusive<TTsvFile>(*this, path));
    } else if (name.EndsWith(".csv") || name.EndsWith(".csv.gz")) {
        Files.push_back(MakeIntrusive<TCsvFile>(*this, path));
    }
}

}