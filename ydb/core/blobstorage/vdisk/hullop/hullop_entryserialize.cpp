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
        Y_VERIFY(success);
        return str.Str();
    }

    bool THullDbSignatureRoutines::Parse(NKikimrVDiskData::THullDbEntryPoint &pb,
                                         const TString &source, 
                                         TString &explanation) { 
        TStringStream str;
        if (source.size() < sizeof(ui32)) {
            str << "Can't check signature because serialized data size is less than sizeof(ui32)";
            explanation = str.Str();
            return false;
        }

        const ui32 s = *(const ui32*)source.data();
        if (s == CurSignature) {
            // new format -- protobuf
            bool success = pb.ParseFromArray(source.data() + sizeof(ui32), source.size() - sizeof(ui32));
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

