#include "print_operation.h"
#include "pretty_table.h"

#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb {
namespace NConsoleClient {

namespace {

    using namespace NKikimr::NOperationId;

    /// Common
    TPrettyTable MakeTable(const TOperation&) {
        return TPrettyTable({"id", "ready", "status"});
    }

    void PrettyPrint(const TOperation& operation, TPrettyTable& table) {
        const auto& status = operation.Status();

        auto& row = table.AddRow();
        row
            .Column(0, ProtoToString(operation.Id()))
            .Column(1, operation.Ready() ? "true" : "false")
            .Column(2, status.GetStatus() == NYdb::EStatus::STATUS_UNDEFINED ? "" : ToString(status.GetStatus()));

        TStringBuilder freeText;

        if (!status.GetIssues().Empty()) {
            freeText << "Issues: " << Endl;
            for (const auto& issue : status.GetIssues()) {
                freeText << "  - " << issue << Endl;
            }
        }
        
        if (!operation.CreatedBy().Empty()) {
            freeText << "Created by: " << operation.CreatedBy() << Endl;
        }

        if (operation.CreateTime() != TInstant::Zero()) {
            freeText << "Create time: " << operation.CreateTime().ToStringUpToSeconds() << Endl;
        }

        if (operation.EndTime() != TInstant::Zero()) {
            freeText << "End time: " << operation.EndTime().ToStringUpToSeconds() << Endl;
        }

        row.FreeText(freeText);
    }

    template <typename EProgress, typename TMetadata>
    TString PrintProgress(const TMetadata& metadata) {
        TStringBuilder result;

        result << metadata.Progress;
        if (metadata.Progress != EProgress::TransferData) {
            return result;
        }

        if (!metadata.ItemsProgress) {
            return result;
        }

        ui32 partsTotal = 0;
        ui32 partsCompleted = 0;
        for (const auto& item : metadata.ItemsProgress) {
            if (!item.PartsTotal) {
                return result;
            }

            partsTotal += item.PartsTotal;
            partsCompleted += item.PartsCompleted;
        }

        float percentage = float(partsCompleted) / partsTotal * 100;
        result << " (" << FloatToString(percentage, PREC_POINT_DIGITS, 2) + "%)";

        return result;
    }

    /// YT
    TPrettyTable MakeTable(const NExport::TExportToYtResponse&) {
        return TPrettyTable({"id", "ready", "status", "progress", "yt proxy"});
    }

    void PrettyPrint(const NExport::TExportToYtResponse& operation, TPrettyTable& table) {
        const auto& status = operation.Status();
        const auto& metadata = operation.Metadata();
        const auto& settings = metadata.Settings;

        auto& row = table.AddRow();
        row
            .Column(0, ProtoToString(operation.Id()))
            .Column(1, operation.Ready() ? "true" : "false")
            .Column(2, status.GetStatus())
            .Column(3, PrintProgress<decltype(metadata.Progress)>(metadata))
            .Column(4, TStringBuilder() << settings.Host_ << ":" << settings.Port_.GetOrElse(80));

        TStringBuilder freeText;

        if (!status.GetIssues().Empty()) {
            freeText << "Issues: " << Endl;
            for (const auto& issue : status.GetIssues()) {
                freeText << "  - " << issue << Endl;
            }
        }

        freeText << "Items: " << Endl;
        for (const auto& item : settings.Item_) {
            freeText
                << "  - source: " << item.Src << Endl
                << "    destination: " << item.Dst << Endl;
        }

        if (settings.Description_) {
            freeText << "Description: " << settings.Description_.GetRef() << Endl;
        }

        if (settings.NumberOfRetries_) {
            freeText << "Number of retries: " << settings.NumberOfRetries_.GetRef() << Endl;
        }

        freeText << "TypeV3: " << (settings.UseTypeV3_ ? "true" : "false") << Endl;

        if (!operation.CreatedBy().Empty()) {
            freeText << "Created by: " << operation.CreatedBy() << Endl;
        }

        if (operation.CreateTime() != TInstant::Zero()) {
            freeText << "Create time: " << operation.CreateTime().ToStringUpToSeconds() << Endl;
        }

        if (operation.EndTime() != TInstant::Zero()) {
            freeText << "End time: " << operation.EndTime().ToStringUpToSeconds() << Endl;
        }

        row.FreeText(freeText);
    }

    /// S3
    TPrettyTable MakeTableS3() {
        return TPrettyTable({"id", "ready", "status", "progress", "endpoint", "bucket"});
    }

    template <typename T>
    void PrettyPrintS3(const T& operation, TPrettyTable& table) {
        const auto& status = operation.Status();
        const auto& metadata = operation.Metadata();
        const auto& settings = metadata.Settings;

        auto& row = table.AddRow();
        row
            .Column(0, ProtoToString(operation.Id()))
            .Column(1, operation.Ready() ? "true" : "false")
            .Column(2, status.GetStatus())
            .Column(3, PrintProgress<decltype(metadata.Progress)>(metadata))
            .Column(4, settings.Endpoint_)
            .Column(5, settings.Bucket_);

        TStringBuilder freeText;

        if constexpr (std::is_same_v<NExport::TExportToS3Response, T>) {
            freeText << "StorageClass: " << settings.StorageClass_ << Endl;
            if (settings.Compression_) {
                freeText << "Compression: " << *settings.Compression_ << Endl;
            }
        }

        if (!status.GetIssues().Empty()) {
            freeText << "Issues: " << Endl;
            for (const auto& issue : status.GetIssues()) {
                freeText << "  - " << issue << Endl;
            }
        }

        freeText << "Items: " << Endl;
        for (const auto& item : settings.Item_) {
            freeText
                << "  - source: " << item.Src << Endl
                << "    destination: " << item.Dst << Endl;
        }

        if (settings.Description_) {
            freeText << "Description: " << settings.Description_.GetRef() << Endl;
        }

        if (settings.NumberOfRetries_) {
            freeText << "Number of retries: " << settings.NumberOfRetries_.GetRef() << Endl;
        }

        if (!operation.CreatedBy().Empty()) {
            freeText << "Created by: " << operation.CreatedBy() << Endl;
        }

        if (operation.CreateTime() != TInstant::Zero()) {
            freeText << "Create time: " << operation.CreateTime().ToStringUpToSeconds() << Endl;
        }

        if (operation.EndTime() != TInstant::Zero()) {
            freeText << "End time: " << operation.EndTime().ToStringUpToSeconds() << Endl;
        }

        row.FreeText(freeText);
    }

    // export
    TPrettyTable MakeTable(const NExport::TExportToS3Response&) {
        return MakeTableS3();
    }

    void PrettyPrint(const NExport::TExportToS3Response& operation, TPrettyTable& table) {
        PrettyPrintS3(operation, table);
    }

    // import
    TPrettyTable MakeTable(const NImport::TImportFromS3Response&) {
        return MakeTableS3();
    }

    void PrettyPrint(const NImport::TImportFromS3Response& operation, TPrettyTable& table) {
        PrettyPrintS3(operation, table);
    }

    /// Index build
    TPrettyTable MakeTable(const NYdb::NTable::TBuildIndexOperation&) {
        return TPrettyTable({"id", "ready", "status", "state", "progress", "table", "index"});
    }

    void PrettyPrint(const NYdb::NTable::TBuildIndexOperation& operation, TPrettyTable& table) {
        const auto& status = operation.Status();
        const auto& metadata = operation.Metadata();

        auto& row = table.AddRow();
        row
            .Column(0, ProtoToString(operation.Id()))
            .Column(1, operation.Ready() ? "true" : "false")
            .Column(2, status.GetStatus() == NYdb::EStatus::STATUS_UNDEFINED ? "" : ToString(status.GetStatus()))
            .Column(3, metadata.State)
            .Column(4, FloatToString(metadata.Progress, PREC_POINT_DIGITS, 2) + "%")
            .Column(5, metadata.Path)
            .Column(6, metadata.Desctiption ? metadata.Desctiption->GetIndexName() : "");

        TStringBuilder freeText;

        if (!status.GetIssues().Empty()) {
            freeText << "Issues: " << Endl;
            for (const auto& issue : status.GetIssues()) {
                freeText << "  - " << issue << Endl;
            }
        }

        row.FreeText(freeText);
    }

    /// QueryService
    TPrettyTable MakeTable(const NYdb::NQuery::TScriptExecutionOperation&) {
        return TPrettyTable({"id", "ready", "status", "execution_id", "exec_status", "exec_mode"});
    }

    void PrettyPrint(const NYdb::NQuery::TScriptExecutionOperation& operation, TPrettyTable& table) {
        const auto& status = operation.Status();
        const auto& metadata = operation.Metadata();

        auto& row = table.AddRow();
        row
            .Column(0, ProtoToString(operation.Id()))
            .Column(1, operation.Ready() ? "true" : "false")
            .Column(2, status.GetStatus() == NYdb::EStatus::STATUS_UNDEFINED ? "" : ToString(status.GetStatus()))
            .Column(3, metadata.ExecutionId)
            .Column(4, metadata.ExecStatus)
            .Column(5, metadata.ExecMode);

        TStringBuilder freeText;

        if (!status.GetIssues().Empty()) {
            freeText << "Issues: " << Endl;
            for (const auto& issue : status.GetIssues()) {
                freeText << "  - " << issue << Endl;
            }
        }

        row.FreeText(freeText);
    }

    // Common
    template <typename T>
    void PrintOperationImpl(const T& operation, EOutputFormat format) {
        switch (format) {
        case EOutputFormat::Default:
        case EOutputFormat::Pretty:
        {
            auto table = MakeTable(operation);
            PrettyPrint(operation, table);
            Cout << table << Endl;
            break;
        }

        case EOutputFormat::Json:
            Cerr << "Warning! Option --json is deprecated and will be removed soon. "
                << "Use \"--format proto-json-base64\" option instead." << Endl;
            [[fallthrough]];
        case EOutputFormat::ProtoJsonBase64:
            Cout << operation.ToJsonString() << Endl;
            break;

        default:
            Y_ABORT("Unknown format");
        }
    }

    template <typename T>
    void PrintOperationsListImpl(const T& operations, EOutputFormat format) {
        switch (format) {
        case EOutputFormat::Default:
        case EOutputFormat::Pretty:
            if (operations.GetList()) {
                auto table = MakeTable(operations.GetList().front());
                for (const auto& operation : operations.GetList()) {
                    PrettyPrint(operation, table);
                }
                Cout << table;
            }
            if (operations.NextPageToken()) {
                Cout << Endl << "Next page token: " << operations.NextPageToken() << Endl;
            }
            break;

        case EOutputFormat::Json:
            Cerr << "Warning! Option --json is deprecated and will be removed soon. "
                << "Use \"--format proto-json-base64\" option instead." << Endl;
            [[fallthrough]];
        case EOutputFormat::ProtoJsonBase64:
            Cout << operations.ToJsonString() << Endl;
            break;

        default:
            Y_ABORT("Unknown format");
        }
    }

}

/// Common
void PrintOperation(const TOperation& operation, EOutputFormat format) {
    PrintOperationImpl(operation, format);
}

/// YT
void PrintOperation(const NExport::TExportToYtResponse& operation, EOutputFormat format) {
    PrintOperationImpl(operation, format);
}

void PrintOperationsList(const NOperation::TOperationsList<NExport::TExportToYtResponse>& operations, EOutputFormat format) {
    PrintOperationsListImpl(operations, format);
}

/// S3
// export
void PrintOperation(const NExport::TExportToS3Response& operation, EOutputFormat format) {
    PrintOperationImpl(operation, format);
}

void PrintOperationsList(const NOperation::TOperationsList<NExport::TExportToS3Response>& operations, EOutputFormat format) {
    PrintOperationsListImpl(operations, format);
}

// import
void PrintOperation(const NImport::TImportFromS3Response& operation, EOutputFormat format) {
    PrintOperationImpl(operation, format);
}

void PrintOperationsList(const NOperation::TOperationsList<NImport::TImportFromS3Response>& operations, EOutputFormat format) {
    PrintOperationsListImpl(operations, format);
}

/// Index build
void PrintOperation(const NYdb::NTable::TBuildIndexOperation& operation, EOutputFormat format) {
    PrintOperationImpl(operation, format);
}

void PrintOperationsList(const NOperation::TOperationsList<NYdb::NTable::TBuildIndexOperation>& operations, EOutputFormat format) {
    PrintOperationsListImpl(operations, format);
}

/// QueryService
void PrintOperation(const NYdb::NQuery::TScriptExecutionOperation& operation, EOutputFormat format) {
    PrintOperationImpl(operation, format);
}

void PrintOperationsList(const NOperation::TOperationsList<NYdb::NQuery::TScriptExecutionOperation>& operations, EOutputFormat format) {
    PrintOperationsListImpl(operations, format);
}

}
}

template <>
void Out<NYdb::NQuery::EExecStatus>(IOutputStream& o, NYdb::NQuery::EExecStatus status) {
    using NYdb::NQuery::EExecStatus;
    switch (status) {
        case EExecStatus::Starting:
            o << TStringBuf("starting");
            return;
        case EExecStatus::Aborted:
            o << TStringBuf("aborted");
            return;
        case EExecStatus::Canceled:
            o << TStringBuf("canceled");
            return;
        case EExecStatus::Completed:
            o << TStringBuf("completed");
            return;
        case EExecStatus::Unspecified:
            o << TStringBuf("unspecified");
            return;
        default:
            o << TStringBuf("unknown");
            return;
    }

    Y_ABORT(); // for GCC
}

template <>
void Out<NYdb::NQuery::EExecMode>(IOutputStream& o, NYdb::NQuery::EExecMode status) {
    using NYdb::NQuery::EExecMode;
    switch (status) {
        case EExecMode::Parse:
            o << TStringBuf("parse");
            return;
        case EExecMode::Validate:
            o << TStringBuf("validate");
            return;
        case EExecMode::Explain:
            o << TStringBuf("explain");
            return;
        case EExecMode::Execute:
            o << TStringBuf("execute");
            return;
        case EExecMode::Unspecified:
            o << TStringBuf("unspecified");
            return;
        default:
            o << TStringBuf("unknown");
            return;
    }

    Y_ABORT(); // for GCC
}
