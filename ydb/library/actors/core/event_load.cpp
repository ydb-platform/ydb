#include "event_load.h"

#include <util/string/builder.h>

namespace NActors {

TString TEventSectionInfo::ToString() const {
    return TStringBuilder()
        << "{headroom# " << Headroom
        << " size# " << Size
        << " tailroom# " << Tailroom
        << " alignment# " << Alignment
        << " inline# " << IsInline
        << " rdma# " << IsRdmaCapable
        << "}";
}

TString SerializeEventSections(const TEventSerializationInfo& info) {
    TStringBuilder sections;
    sections << "[";
    for (size_t i = 0; i < info.Sections.size(); ++i) {
        if (i) {
            sections << ", ";
        }
        sections << "{idx# " << i << " section# " << info.Sections[i].ToString() << "}";
    }
    sections << "]";
    return sections;
}

}
