#include "hullop_entryserialize.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // THullDbSignatureRoutines
    ////////////////////////////////////////////////////////////////////////////
    const ui32 THullDbSignatureRoutines::CurSignature = 0x93F7ADD5;

    TString THullDbSignatureRoutines::Serialize(const NKikimrVDiskData::THullDbEntryPoint &pb) {
        // signature
        TStringStream str;
        str.Write(&CurSignature, sizeof(ui32));
        // pb payload
        bool success = pb.SerializeToArcadiaStream(&str);
        Y_ABORT_UNLESS(success);
        return str.Str();
    }

    bool THullDbSignatureRoutines::Parse(NKikimrVDiskData::THullDbEntryPoint &pb,
                                         const TString &source,
                                         TString &explanation) {
        return ParseArray(pb, source.data(), source.size(), explanation);
    }

    bool THullDbSignatureRoutines::ParseArray(NKikimrVDiskData::THullDbEntryPoint &pb,
                                              const char *data,
                                              size_t size,
                                              TString &explanation) {
        TStringStream str;
        if (size < sizeof(ui32)) {
            str << "Can't check signature because serialized data size is less than sizeof(ui32)";
            explanation = str.Str();
            return false;
        }

        const ui32 s = *(const ui32*)data;
        if (s == CurSignature) {
            // new format -- protobuf
            bool success = pb.ParseFromArray(data + sizeof(ui32), size - sizeof(ui32));
            if (!success) {
                str << "Failed to parse protobuf";
                explanation = str.Str();
                return false;
            }
        } else {
            str << "Unknown signature: " << s;
            explanation = str.Str();
            return false;
        }

        return true;
    }

} // NKikimr

