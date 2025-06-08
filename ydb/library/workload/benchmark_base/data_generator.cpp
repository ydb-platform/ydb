#include "data_generator.h"
#include <ydb/library/formats/arrow/validation/validation.h>
#include <library/cpp/streams/factory/open_by_signature/factory.h>
#include <ydb/public/lib/ydb_cli/import/cli_arrow_helpers.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/file_reader.h>

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

TWorkloadDataInitializerBase::TDataGenerator::TFile::TFile(TDataGenerator& owner, const TString& path)
    : Owner(owner)
    , Path(TFsPath(path).RealPath())
{}

class TWorkloadDataInitializerBase::TDataGenerator::TCsvFileBase: public TWorkloadDataInitializerBase::TDataGenerator::TFile {
public:
    TCsvFileBase(TDataGenerator& owner, const TString& path, const TString& delimiter, const TString& foramt)
        : TFile(owner, path)
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
        ui64 readBytes = 0;
        with_lock(Lock) {
            TString line;
            if (Owner.Owner.StateProcessor && Owner.Owner.StateProcessor->GetState().contains(Path)) {
                auto position = Owner.Owner.StateProcessor->GetState().at(Path).Position;
                while(position > ReadBytes) {
                    ReadBytes += Decompressor->Skip(position - ReadBytes);
                }
            }
            while (const auto read = Decompressor->ReadLine(line)) {
                readBytes += read;
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
        const auto position = ReadBytes;
        ReadBytes += readBytes;
        return MakeIntrusive<TDataPortionWithState>(
            Owner.Owner.StateProcessor.Get(),
            Owner.Owner.Params.GetFullTableName(Owner.TablePath.c_str()),
            Path,
            TDataPortion::TCsv(std::move(data), Foramt),
            position,
            readBytes
        );
    }

private:
    THolder<IInputStream> Decompressor;
    TString Header;
    const TString& Foramt;
    ui64 ReadBytes = 0;
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

using NKikimr::NArrow::TStatusValidator;
class TWorkloadDataInitializerBase::TDataGenerator::TParquetFile final: public TWorkloadDataInitializerBase::TDataGenerator::TFile {
public:
    TParquetFile(TDataGenerator& owner, const TString& path)
        : TFile(owner, path)
        , WriteOptions(arrow::ipc::IpcWriteOptions::Defaults())
    {
        constexpr auto codecType = arrow::Compression::type::ZSTD;
        WriteOptions.codec = TStatusValidator::GetValid(arrow::util::Codec::Create(codecType));

        ReadableFile = TStatusValidator::GetValid(arrow::io::ReadableFile::Open(Path));
        TStatusValidator::Validate(parquet::arrow::OpenFile(ReadableFile, arrow::default_memory_pool(), &FileReader));
        auto metadata = parquet::ReadMetaData(ReadableFile);
        const i64 numRowGroups = metadata->num_row_groups();
        Size = metadata->num_rows();

        std::vector<int> row_group_indices(numRowGroups);
        for (i64 i = 0; i < numRowGroups; i++) {
            row_group_indices[i] = i;
        }

        TStatusValidator::Validate(FileReader->GetRecordBatchReader(row_group_indices, &RecordBatchReader));
    }

    TDataPortionPtr GetPortion() override {
        std::shared_ptr<arrow::RecordBatch> batchToSend;
        ui64 position = 0;
        with_lock(Lock) {
            if (Owner.Owner.StateProcessor && Owner.Owner.StateProcessor->GetState().contains(Path)) {
                auto positionInState = Owner.Owner.StateProcessor->GetState().at(Path).Position;
                if (positionInState >= Size) {
                    return {};
                }
                while(positionInState > PositionInFile && SetCurrentBatch()) {
                    if (PositionInFile + CurrentBatch->num_rows() <= positionInState) {
                        PositionInFile += CurrentBatch->num_rows();
                        CurrentBatch.reset();
                    } else {
                        PositionInBatch = positionInState - PositionInFile;
                        PositionInFile = positionInState;
                    }
                }
            }
            if (!SetCurrentBatch()) {
                return {};
            }
            batchToSend = CurrentBatch->Slice(PositionInBatch, std::min<ui64>(Owner.Owner.Params.BulkSize, CurrentBatch->num_rows() - PositionInBatch));
            PositionInBatch += batchToSend->num_rows();
            position = PositionInFile;
            PositionInFile += batchToSend->num_rows();
            if (CurrentBatch->num_rows() <= (i64)PositionInBatch) {
                CurrentBatch.reset();
            }
        }
        return MakeIntrusive<TDataPortionWithState>(
            Owner.Owner.StateProcessor.Get(),
            Owner.Owner.Params.GetFullTableName(Owner.TablePath.c_str()),
            Path,
            TDataPortion::TArrow(
                NYdb_cli::NArrow::SerializeBatch(batchToSend, WriteOptions),
                NYdb_cli::NArrow::SerializeSchema(*batchToSend->schema())
            ),
            position,
            (ui64)batchToSend->num_rows()
        );
    }

private:
    bool SetCurrentBatch() {
        if (!CurrentBatch) {
            TStatusValidator::Validate(RecordBatchReader->ReadNext(&CurrentBatch));
            PositionInBatch = 0;
        }
        return !!CurrentBatch;
    }
    arrow::ipc::IpcWriteOptions WriteOptions;
    std::shared_ptr<arrow::io::ReadableFile> ReadableFile;
    std::unique_ptr<parquet::arrow::FileReader> FileReader;
    std::unique_ptr<arrow::RecordBatchReader> RecordBatchReader;
    std::shared_ptr<arrow::RecordBatch> CurrentBatch;
    ui64 PositionInFile = 0;
    ui64 PositionInBatch = 0;
    ui64 Size = 0;
};

void TWorkloadDataInitializerBase::TDataGenerator::AddFile(const TFsPath& path) {
    const auto name = path.GetName();
    if (name.EndsWith(".tsv") || name.EndsWith(".tsv.gz")) {
        Files.push_back(MakeIntrusive<TTsvFile>(*this, path));
    } else if (name.EndsWith(".csv") || name.EndsWith(".csv.gz")) {
        Files.push_back(MakeIntrusive<TCsvFile>(*this, path));
#if not defined(_win32_)
    } else if (name.EndsWith(".parquet")) {
        Files.push_back(MakeIntrusive<TParquetFile>(*this, path));
#endif
    }
    ++FilesCount;
}

}