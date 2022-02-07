#include "blobstorage_anubis_osiris.h"

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
        Y_VERIFY(res);
        return lbSerialized;
    }

    bool TAnubisOsirisPutRecoveryLogRec::ParseFromString(const TString &data) {
        NKikimrVDiskData::TAnubisOsirisPutRecoveryLogRec proto;
        bool res = proto.ParseFromString(data);
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

