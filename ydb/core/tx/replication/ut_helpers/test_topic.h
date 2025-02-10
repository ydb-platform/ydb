#pragma once

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimrSchemeOp {
    class TPersQueueGroupDescription;
}

namespace NKikimr::NReplication::NTestHelpers {

struct TTestTopicDescription {
    TString Name;

    void SerializeTo(NKikimrSchemeOp::TPersQueueGroupDescription& proto) const;
};

THolder<NKikimrSchemeOp::TPersQueueGroupDescription> MakeTopicDescription(const TTestTopicDescription& desc);

}
