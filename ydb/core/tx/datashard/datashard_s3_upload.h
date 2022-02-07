#pragma once

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/join.h>

namespace NKikimr {
namespace NDataShard {

struct TS3Upload {
    enum class EStatus: ui8 {
        UploadParts,
        Complete,
        Abort,
    };

    explicit TS3Upload(const TString& id)
        : Id(id)
        , Status(EStatus::UploadParts)
    {
    }

    TString Id;
    EStatus Status;
    TMaybe<TString> Error;
    TVector<TString> Parts;

    void Out(IOutputStream& out) const {
        out << "{"
            << " Id: " << Id
            << " Status: " << Status
            << " Error: " << Error
            << " Parts: [" << JoinSeq(",", Parts) << "]"
        << " }";
    }

}; // TS3Upload

} // namespace NDataShard
} // namespace NKikimr

Y_DECLARE_OUT_SPEC(inline, NKikimr::NDataShard::TS3Upload, out, value) {
    value.Out(out);
}
