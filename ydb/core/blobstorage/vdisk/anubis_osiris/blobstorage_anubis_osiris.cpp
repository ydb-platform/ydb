#include "blobstorage_anubis_osiris.h"
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TEvAnubisOsirisPut
    ////////////////////////////////////////////////////////////////////////////
    TEvAnubisOsirisPut::TEvAnubisOsirisPut(const TAnubisOsirisPutRecoveryLogRec &rec)
        : LogoBlobId(rec.Id)
    {}

    ////////////////////////////////////////////////////////////////////////////
    // TAnubisOsirisPutRecoveryLogRec
    ////////////////////////////////////////////////////////////////////////////
    TAnubisOsirisPutRecoveryLogRec::TAnubisOsirisPutRecoveryLogRec(const TEvAnubisOsirisPut &msg)
        : Id(msg.LogoBlobId)
    {}

    TString TAnubisOsirisPutRecoveryLogRec::Serialize() const {
        NKikimrVDiskData::TAnubisOsirisPutRecoveryLogRec proto;
        auto *protoId = proto.MutableLogoBlobId();
        LogoBlobIDFromLogoBlobID(Id, protoId);
        TString lbSerialized;
        bool res = proto.SerializeToString(&lbSerialized);
        Y_ABORT_UNLESS(res);
        return lbSerialized;
    }

    bool TAnubisOsirisPutRecoveryLogRec::ParseFromString(const TString &data) {
        return ParseFromArray(data.data(), data.size());
    }

    bool TAnubisOsirisPutRecoveryLogRec::ParseFromArray(const char* data, size_t size) {
        NKikimrVDiskData::TAnubisOsirisPutRecoveryLogRec proto;
        bool res = proto.ParseFromArray(data, size);
        if (!res)
            return res;
        Id = LogoBlobIDFromLogoBlobID(proto.GetLogoBlobId());
        return true;
    }

    TString TAnubisOsirisPutRecoveryLogRec::ToString() const {
        TStringStream str;
        Output(str);
        return str.Str();
    }

    void TAnubisOsirisPutRecoveryLogRec::Output(IOutputStream &str) const {
        str << "{Id# " << Id << "}";
    }

} // NKikimr

