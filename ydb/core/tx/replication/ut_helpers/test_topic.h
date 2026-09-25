#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimrSchemeOp {
    class TPersQueueGroupDescription;
}

namespace NKikimr::NReplication::NTestHelpers {

struct TTestTopicDescription {
    TString Name;

    void SerializeTo(NKikimrSchemeOp::TPersQueueGroupDescription& proto) const;
};

std::unique_ptr<NKikimrSchemeOp::TPersQueueGroupDescription> MakeTopicDescription(const TTestTopicDescription& desc);

}
