#include "print_utils.h"

#include <google/protobuf/util/json_util.h>
#include <google/protobuf/port_def.inc>

#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/stream/format.h>

namespace NYdb::NConsoleClient {

void PrintSchemeEntry(IOutputStream& o, const NScheme::TSchemeEntry& entry, NColorizer::TColors colors) {
    switch (entry.Type) {
    case NScheme::ESchemeEntryType::ColumnStore:
        o << colors.LightGrayColor();
        break;
    case NScheme::ESchemeEntryType::Directory:
        o << colors.LightBlueColor();
        break;
    case NScheme::ESchemeEntryType::Table:
        o << colors.WhiteColor();
        break;
    case NScheme::ESchemeEntryType::ColumnTable:
        o << colors.WhiteColor();
        break;
    case NScheme::ESchemeEntryType::PqGroup:
    case NScheme::ESchemeEntryType::Topic:
        o << colors.BrownColor();
        break;
    case NScheme::ESchemeEntryType::SubDomain:
        o << colors.CyanColor();
        break;
    case NScheme::ESchemeEntryType::RtmrVolume:
        o << colors.LightGreenColor();
        break;
    case NScheme::ESchemeEntryType::BlockStoreVolume:
        o << colors.LightPurpleColor();
        break;
    case NScheme::ESchemeEntryType::CoordinationNode:
        o << colors.YellowColor();
        break;
    case NScheme::ESchemeEntryType::ExternalTable:
    case NScheme::ESchemeEntryType::ExternalDataSource:
        o << colors.LightWhite();
        break;
    case NScheme::ESchemeEntryType::ResourcePool:
        o << colors.LightWhite();
        break;
    case NScheme::ESchemeEntryType::SysView:
        o << colors.LightYellow();
        break;
    case NScheme::ESchemeEntryType::StreamingQuery:
        o << colors.LightWhite();
        break;
    case NScheme::ESchemeEntryType::BackupCollection:
        o << colors.LightBlueColor();
        break;
    default:
        o << colors.RedColor();
    }
    o << entry.Name << colors.OldColor();
}

TString PrettySize(ui64 size) {
    double sizeFormat = size;
    return ToString(HumanReadableSize(sizeFormat, ESizeFormat::SF_QUANTITY)) + "B";
}

TString PrettyNumber(ui64 number) {
    double numberFormat = number;
    return ToString(HumanReadableSize(numberFormat, ESizeFormat::SF_QUANTITY));
}

TString FormatTime(TInstant time) {
    if (time.GetValue()) {
        return time.ToRfc822StringLocal();
    } else {
        return "Unknown";
    }
};

TString FormatDuration(TDuration duration) {
    if (duration == TDuration::Zero()) {
        return "0s";
    }

    ui64 totalMs = duration.MilliSeconds();

    // For sub-second durations, show milliseconds
    if (totalMs < 1000) {
        return TStringBuilder() << totalMs << "ms";
    }

    ui64 totalSeconds = duration.Seconds();
    ui64 hours = totalSeconds / 3600;
    ui64 minutes = (totalSeconds % 3600) / 60;
    ui64 seconds = totalSeconds % 60;

    TStringBuilder result;

    if (hours > 0) {
        result << hours << "h";
        if (minutes > 0) {
            result << " " << minutes << "m";
        }
    } else if (minutes > 0) {
        result << minutes << "m";
        if (seconds > 0) {
            result << " " << seconds << "s";
        }
    } else {
        // Show seconds with one decimal for precision when < 1 minute
        double secs = duration.SecondsFloat();
        if (secs < 10) {
            result << Sprintf("%.1fs", secs);
        } else {
            result << seconds << "s";
        }
    }

    return result;
}

namespace {

TString InsertSpaceBeforeUnit(TString result) {
    // Insert space before unit suffix (KiB, MiB, GiB, TiB, or B)
    for (size_t i = 0; i < result.size(); ++i) {
        char c = result[i];
        if (c == 'K' || c == 'M' || c == 'G' || c == 'T' || c == 'B') {
            result.insert(i, " ");
            break;
        }
    }
    return result;
}

} // namespace

TString FormatBytes(ui64 bytes) {
    return InsertSpaceBeforeUnit(ToString(HumanReadableSize(bytes, SF_BYTES)));
}

TString FormatSpeed(double bytesPerSecond) {
    return InsertSpaceBeforeUnit(ToString(HumanReadableSize(bytesPerSecond, SF_BYTES))) + "/s";
}

TString FormatEta(TDuration duration) {
    ui64 totalSeconds = duration.Seconds();
    if (totalSeconds == 0) {
        return "<1s";
    }

    ui64 hours = totalSeconds / 3600;
    ui64 minutes = (totalSeconds % 3600) / 60;
    ui64 seconds = totalSeconds % 60;

    TStringBuilder result;
    if (hours > 0) {
        result << hours << "h";
        if (minutes > 0) {
            result << " " << minutes << "m";
        }
    } else if (minutes > 0) {
        result << minutes << "m";
        if (seconds > 0) {
            result << " " << seconds << "s";
        }
    } else {
        result << seconds << "s";
    }

    return result;
}

TString EntryTypeToString(NScheme::ESchemeEntryType entry) {
    switch (entry) {
    case NScheme::ESchemeEntryType::Directory:
        return "dir";
    case NScheme::ESchemeEntryType::Table:
        return "table";
    case NScheme::ESchemeEntryType::ColumnTable:
        return "column-table";
    case NScheme::ESchemeEntryType::ColumnStore:
        return "column-store";
    case NScheme::ESchemeEntryType::PqGroup:
    case NScheme::ESchemeEntryType::Topic:
        return "topic";
    case NScheme::ESchemeEntryType::SubDomain:
        return "sub-domain";
    case NScheme::ESchemeEntryType::RtmrVolume:
        return "rtmr-volume";
    case NScheme::ESchemeEntryType::BlockStoreVolume:
        return "block-store-volume";
    case NScheme::ESchemeEntryType::CoordinationNode:
        return "coordination-node";
    case NScheme::ESchemeEntryType::ExternalDataSource:
        return "external-data-source";
    case NScheme::ESchemeEntryType::ExternalTable:
        return "external-table";
    case NScheme::ESchemeEntryType::View:
        return "view";
    case NScheme::ESchemeEntryType::Replication:
        return "replication";
    case NScheme::ESchemeEntryType::Transfer:
        return "transfer";
    case NScheme::ESchemeEntryType::ResourcePool:
        return "resource-pool";
    case NScheme::ESchemeEntryType::SysView:
        return "sys-view";
    case NScheme::ESchemeEntryType::StreamingQuery:
        return "streaming-query";
    case NScheme::ESchemeEntryType::BackupCollection:
        return "backup-collection";
    case NScheme::ESchemeEntryType::Unknown:
    case NScheme::ESchemeEntryType::Sequence:
        return "unknown";
    }
}

int PrintProtoJsonBase64(const google::protobuf::Message& msg, IOutputStream& out) {
    using namespace google::protobuf::util;

    TString json;
    JsonPrintOptions opts;
    opts.preserve_proto_field_names = true;
    const auto status = MessageToJsonString(msg, &json, opts);

    if (!status.ok()) {
        #if PROTOBUF_VERSION >= 4022005
        Cerr << "Error occurred while converting proto to json: " << status.message() << Endl;
        #else
        Cerr << "Error occurred while converting proto to json: " << status.message().ToString() << Endl;
        #endif
        return EXIT_FAILURE;
    }

    out << json << Endl;
    return EXIT_SUCCESS;
}

FHANDLE GetStdinFileno() {
#if defined(_win32_)
    return GetStdHandle(STD_INPUT_HANDLE);
#elif defined(_unix_)
    return STDIN_FILENO;
#endif
}

TString BlurSecret(const TString& in) {
    TString out(in);
    size_t clearSymbolsCount = Min(size_t(10), out.length() / 4);
    for (size_t i = clearSymbolsCount; i < out.length() - clearSymbolsCount; ++i) {
        out[i] = '*';
    }
    return out;
}

void PrintPermissions(const std::vector<NScheme::TPermissions>& permissions, IOutputStream& out) {
    if (permissions.size()) {
        for (const NScheme::TPermissions& permission : permissions) {
            out << permission.Subject << ":";
            for (const std::string& name : permission.PermissionNames) {
                if (name != *permission.PermissionNames.begin()) {
                    out << ",";
                }
                out << name;
            }
            out << Endl;
        }
    } else {
        out << "none" << Endl;
    }
}

void PrintAllPermissions(
    const std::string& owner,
    const std::vector<NScheme::TPermissions>& permissions,
    const std::vector<NScheme::TPermissions>& effectivePermissions,
    IOutputStream& out
) {
    out << "Owner: " << owner << Endl << Endl << "Permissions: " << Endl;
    PrintPermissions(permissions, out);
    out << Endl << "Effective permissions: " << Endl;
    PrintPermissions(effectivePermissions, out);
}

} // namespace NYdb::NConsoleClient
