#include "blobstorage_hulldefs.h" 
#include <ydb/core/base/blobstorage_grouptype.h> 
 
namespace NKikimr { 
 
    //////////////////////////////////////////////////////////////////////////// 
    // THullCtx 
    //////////////////////////////////////////////////////////////////////////// 
    void THullCtx::UpdateSpaceCounters(const NHullComp::TSstRatio& prev, const NHullComp::TSstRatio& current) { 
         LsmHullSpaceGroup.DskSpaceCurIndex()         += current.IndexBytesTotal   - prev.IndexBytesTotal; 
         LsmHullSpaceGroup.DskSpaceCurInplacedData()  += current.InplacedDataTotal - prev.InplacedDataTotal; 
         LsmHullSpaceGroup.DskSpaceCurHugeData()      += current.HugeDataTotal     - prev.HugeDataTotal; 
         LsmHullSpaceGroup.DskSpaceCompIndex()        += current.IndexBytesKeep    - prev.IndexBytesKeep; 
         LsmHullSpaceGroup.DskSpaceCompInplacedData() += current.InplacedDataKeep  - prev.InplacedDataKeep; 
         LsmHullSpaceGroup.DskSpaceCompHugeData()     += current.HugeDataKeep      - prev.HugeDataKeep; 
    } 
 
    //////////////////////////////////////////////////////////////////////////// 
    // TPutRecoveryLogRecOpt 
    //////////////////////////////////////////////////////////////////////////// 
    static_assert(sizeof(TLogoBlobID) == 24, "TLogoBlobID size has changed"); 
 
    TString TPutRecoveryLogRecOpt::Serialize(const TBlobStorageGroupType &gtype, const TLogoBlobID &id, 
            const TRope &rope) { 
        Y_VERIFY(id.PartId() && rope.GetSize() == gtype.PartSize(id), 
            "id# %s rope.GetSize()# %zu", id.ToString().data(), rope.GetSize()); 
 
        TString res = TString::Uninitialized(sizeof(id) + rope.GetSize()); 
        char *buf = &*res.begin(); 
        memcpy(buf, id.GetRaw(), sizeof(id)); 
        rope.Begin().ExtractPlainDataAndAdvance(buf + sizeof(id), rope.GetSize()); 
        return res; 
    } 
 
    bool TPutRecoveryLogRecOpt::ParseFromString(const TBlobStorageGroupType &gtype, 
                                                const TString &data) {
        const char *pos = data.data();
        const char *end = data.data() + data.size();
        if (size_t(end - pos) < 24) 
            return false; 
 
        const ui64 *raw = (const ui64 *)pos; 
        Id = TLogoBlobID(raw[0], raw[1], raw[2]); 
        pos += 24; 
 
        ui64 partSize = gtype.PartSize(Id);
 
        if (size_t(end - pos) != partSize) 
            return false; 
 
        Data = TString(pos, partSize);
        return true; 
    } 
 
    TString TPutRecoveryLogRecOpt::ToString() const {
        TStringStream str; 
        Output(str); 
        return str.Str(); 
    } 
 
    void TPutRecoveryLogRecOpt::Output(IOutputStream &str) const {
        str << "{Id# " << Id << "}"; 
    } 
 
} // NKikimr 
