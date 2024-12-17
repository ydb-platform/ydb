#pragma once

#include "defs.h"

#include <util/stream/output.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimrTx {
class TMessageSeqNo;
}

namespace NKikimr {
    // A helper for check the order of messages sent by a tablet
    struct TMessageSeqNo {
        ui64 Generation;
        ui64 Round;

        TMessageSeqNo()
            : Generation(0)
            , Round(0)
        {}

        TMessageSeqNo(ui64 gen, ui64 round)
            : Generation(gen)
            , Round(round)
        {}

        operator bool () const {
            return Generation != 0;
        }

        bool operator == (const TMessageSeqNo& other) const {
            return Generation == other.Generation &&
                    Round == other.Round;
        }

        bool operator != (const TMessageSeqNo& other) const {
            return !(*this == other);
        }

        bool operator < (const TMessageSeqNo& other) const {
            return Generation < other.Generation ||
                    (Generation == other.Generation && Round < other.Round);
        }

        bool operator > (const TMessageSeqNo& other) const {
            return (other < *this);
        }

        bool operator <= (const TMessageSeqNo& other) const {
            return Generation < other.Generation ||
                    (Generation == other.Generation && Round <= other.Round);
        }

        bool operator >= (const TMessageSeqNo& other) const {
            return (other <= *this);
        }

        TMessageSeqNo& operator ++ () {
            if (0 == ++Round) {
                ++Generation;
            }
            return *this;
        }

        void Out(IOutputStream& o) const {
            o << Generation << ":" << Round;
        }

        TString SerializeToString() const;
        TConclusionStatus DeserializeFromString(const TString& data);
        NKikimrTx::TMessageSeqNo SerializeToProto() const;
        TConclusionStatus DeserializeFromProto(const NKikimrTx::TMessageSeqNo& proto);
    };

}


template<>
inline void Out<NKikimr::TMessageSeqNo>(IOutputStream& o, const NKikimr::TMessageSeqNo& x) {
    return x.Out(o);
}
