#include "print_utils.h"

#include <util/string/printf.h>

namespace NYdb {
namespace NConsoleClient {

void PrintSchemeEntry(IOutputStream& o, const NScheme::TSchemeEntry& entry, NColorizer::TColors colors) {
    switch (entry.Type) {
    case NScheme::ESchemeEntryType::Directory:
        o << colors.LightBlueColor();
        break;
    case NScheme::ESchemeEntryType::Table:
        o << colors.WhiteColor();
        break;
    case NScheme::ESchemeEntryType::PqGroup:
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

}
}
