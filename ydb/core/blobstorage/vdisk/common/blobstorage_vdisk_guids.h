#pragma once

#include "defs.h"

#include <util/stream/output.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TVDiskIncarnationGuid
    // For every incarnation of VDisk there is an instance of TVDiskIncarnationGuid,
    // that is used for metadata sync of the Hull database.
    // Other words, every type VDisk is formatted (i.e. PDisk under VDisk),
    // a new TVDiskIncarnationGuid is generated.
    ////////////////////////////////////////////////////////////////////////////
    struct TVDiskIncarnationGuid {
        ui64 Guid = 0;

        TVDiskIncarnationGuid() = default;
        TVDiskIncarnationGuid(const TVDiskIncarnationGuid &) = default;
        TVDiskIncarnationGuid &operator=(const TVDiskIncarnationGuid &) = default;
        TVDiskIncarnationGuid(ui64 guid)
            : Guid(guid)
        {}

        bool operator ==(const TVDiskIncarnationGuid &g) const {
            return Guid == g.Guid;
        }

        void Output(IOutputStream &str) const {
            str << Guid;
        }

        TString ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        void Serialize(IOutputStream &s) const {
            s.Write(&Guid, sizeof(Guid));
        }

        operator ui64() const {
            return Guid;
        }

        static TVDiskIncarnationGuid Generate();
    };

    ////////////////////////////////////////////////////////////////////////////
    // TVDiskEternalGuid
    // TVDiskEternalGuid is unchanged for the VDisk (slot in BlobStorage group)
    // while BlobStorage group exists. It is generated during the Sync Guid phase
    // on the first group run.
    // TVDiskEternalGuid is used to understand if VDisk has lost its data.
    ////////////////////////////////////////////////////////////////////////////
    struct TVDiskEternalGuid {
        ui64 Guid = 0;

        TVDiskEternalGuid() = default;
        TVDiskEternalGuid(const TVDiskEternalGuid &) = default;
        TVDiskEternalGuid &operator=(const TVDiskEternalGuid &other) = default;
        TVDiskEternalGuid(ui64 guid)
            : Guid(guid)
        {}

        bool operator ==(const TVDiskEternalGuid &g) const {
            return Guid == g.Guid;
        }

        void Output(IOutputStream &str) const {
            str << Guid;
        }

        TString ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        operator ui64() const {
            return Guid;
        }

        static TVDiskEternalGuid Generate();

        explicit operator bool() const {
            return (Guid != 0);
        }
    };

} // NKikimr

