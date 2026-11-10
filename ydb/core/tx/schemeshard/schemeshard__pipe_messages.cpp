#include "schemeshard__pipe_messages.h"

#include "schemeshard__operation.h"

namespace NKikimr::NSchemeShard {

TIntrusivePtr<TEventSerializedData> TOperation::TPreSerializedMessage::Materialize() {
    if (!Data) {
        Y_ABORT_UNLESS(Deferred.has_value(), "Materialize on an empty message");

        auto& fragments = Deferred->Items;
        Y_ABORT_UNLESS(!fragments.empty());

        // Fragments are the message body in emit order: chained back to back
        // they are exactly the bytes sent (rope glueing of adjacent chunks
        // changes the layout, not the content).
        TRope rope;
        for (const auto& fragment : fragments) {
            if (fragment.GetSize()) {
                rope.Insert(rope.End(), TRope(fragment));
            }
        }

        // Matches what CreateSerializationInfo(false) produces for a
        // payload-free pre-serialized event: no payload, no extended format.
        NActors::TEventSerializationInfo info;

        Data = MakeIntrusive<NActors::TEventSerializedData>(std::move(rope), std::move(info));
        Deferred.reset();
    }
    return Data;
}

}
