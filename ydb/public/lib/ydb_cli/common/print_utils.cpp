#include "print_utils.h"

#include <google/protobuf/util/json_util.h>
#include <google/protobuf/port_def.inc>

#include <util/string/printf.h>

namespace NYdb {
namespace NConsoleClient {

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
    default:
        o << colors.RedColor();
    }
    o << entry.Name << colors.OldColor();
}

TString PrettySize(size_t size) {
    double sizeFormat = size;
    TString mod = "b";
    const char* mods[] = { "Kb", "Mb", "Gb", "Tb", "Pb", "Eb" };
    TString numFormat = "%.0f";

    for (const char* nextMod : mods) {
        if (sizeFormat > 1024) {
            sizeFormat /= 1024;
            mod = nextMod;
            numFormat = "%.02f";
        } else {
            break;
        }
    }

    return Sprintf((numFormat + " %s").data(), sizeFormat, mod.data());
}

TString FormatTime(TInstant time) {
    if (time.GetValue()) {
        return time.ToRfc822StringLocal();
    } else {
        return "Unknown";
    }
};

TString FormatDuration(TDuration duration) {
    return Sprintf("%.02f seconds",(duration.MilliSeconds() * 0.001));
};


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
    case NScheme::ESchemeEntryType::ResourcePool:
        return "resource-pool";
    case NScheme::ESchemeEntryType::Unknown:
    case NScheme::ESchemeEntryType::Sequence:
        return "unknown";
    }
}

int PrintProtoJsonBase64(const google::protobuf::Message& msg) {
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

    Cout << json << Endl;
    return EXIT_SUCCESS;
}

}
}
