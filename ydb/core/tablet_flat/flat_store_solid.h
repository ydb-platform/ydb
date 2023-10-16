#pragma once

#include "flat_sausage_solid.h"
#include <ydb/core/tablet_flat/flat_executor.pb.h>
#include <functional>

namespace NKikimr {
namespace NTabletFlatExecutor {

    struct TLargeGlobIdProto {
        using TLargeGlobId = NPageCollection::TLargeGlobId;
        using TProto = NKikimrExecutorFlat::TLargeGlobId;
        using TArr = TArrayRef<NKikimrProto::TLogoBlobID*>;
        using TRep = NProtoBuf::RepeatedPtrField<NKikimrProto::TLogoBlobID>;

        /* In the near future all blobs tracked by executor binary logs
            will use TLargeGlobId for keeping of serialized entities with full
            blobs identifiers (i.e. with BS group). But this requires some
            migration timespan within of TLargeGlobId will be faked from legacy
            series of TLogoBlobIDs with external group lookup functor.
         */

        using TLookup = std::function<ui32(const TLogoBlobID&)>;

        static void Put(TProto &proto, const TLargeGlobId &largeGlobId)
        {
            Y_ABORT_UNLESS(largeGlobId.Group != TLargeGlobId::InvalidGroup, "Please, set BS group");

            proto.SetGroup(largeGlobId.Group);
            proto.SetBytes(largeGlobId.Bytes);

            LogoBlobIDFromLogoBlobID(largeGlobId.Lead, proto.MutableLead());
        }

        static TLargeGlobId Get(const TProto &from)
        {
            auto lead = LogoBlobIDFromLogoBlobID(from.GetLead());

            return { from.GetGroup(), lead, from.GetBytes() };
        }

        static TLargeGlobId Get(const TRep &rep, const TLookup &lookup)
        {
            Y_ABORT_UNLESS(rep.size(), "TLargeGlobId accepts only non-empty sequence");

            const auto lead = LogoBlobIDFromLogoBlobID(rep.Get(0));
            ui32 bytes = lead.BlobSize();

            for (int it = 1; it < rep.size(); it++) {
                auto logo = LogoBlobIDFromLogoBlobID(rep.Get(it));

                if (bytes > Max<ui32>() - logo.BlobSize())
                    Y_ABORT("Got too large TLargeGlobId in ids sequence");
                if (lead.Cookie() + it != logo.Cookie())
                    Y_ABORT("Got an invalid sequence of logo ids");

                bytes += logo.BlobSize();
            }

            return { lookup(lead), lead, bytes };
        }
    };
}
}
