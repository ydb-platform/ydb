#include "data_generator.h"
#include <ydb/library/yaml_json/yaml_to_json.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/streams/factory/open_by_signature/factory.h>
#include <contrib/libs/yaml-cpp/include/yaml-cpp/node/parse.h>
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
    : IBulkDataGenerator("hits", DataSetSize)
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
            return {result};
        } else {
            auto g = Guard(Lock);
            --FilesCount;
        }
    }
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
        const auto yaml = YAML::Load(NResource::Find("click_bench_schema.yaml").c_str());
        const auto json = NKikimr::NYaml::Yaml2Json(yaml, true);
        const auto& columns = json["table"]["columns"].GetArray();
        TVector<TString> header;
        header.reserve(columns.size());
        for (const auto& c: columns) {
            header.emplace_back(c["name"].GetString());
        }
        Header = JoinSeq(Delimiter, header);
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
    ++FilesCount;
}

}