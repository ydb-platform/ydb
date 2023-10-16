#include "resolved_value.h"

namespace NKikimr::NBlobDepot {

    TResolvedValue::TLink::TLink(const NKikimrBlobDepot::TResolvedValueChain& link)
        : BlobId(LogoBlobIDFromLogoBlobID(link.GetBlobId()))
        , GroupId(link.GetGroupId())
        , SubrangeBegin(link.GetSubrangeBegin())
        , SubrangeEnd(link.HasSubrangeEnd() ? link.GetSubrangeEnd() : BlobId.BlobSize())
    {
        Y_DEBUG_ABORT_UNLESS(link.HasBlobId() && link.HasGroupId());
    }

    void TResolvedValue::TLink::Output(IOutputStream& s) const {
        s << BlobId << '@' << GroupId << '{' << SubrangeBegin << '-' << SubrangeEnd - 1 << '}';
    }

    TString TResolvedValue::TLink::ToString() const {
        TStringStream s;
        Output(s);
        return s.Str();
    }

    TResolvedValue::TResolvedValue(const NKikimrBlobDepot::TEvResolveResult::TResolvedKey& item)
        : Defined(true)
        , ReliablyWritten(item.GetReliablyWritten())
        , Version(item.GetValueVersion())
        , Chain([](auto& x) { return decltype(Chain)(x.begin(), x.end()); }(item.GetValueChain()))
    {}

    bool TResolvedValue::Supersedes(const TResolvedValue& old) const {
        Y_ABORT_UNLESS(Defined);
        if (!old.Defined) {
            return true;
        } else if (Version < old.Version) {
            return false;
        } else if (Version == old.Version) {
            Y_ABORT_UNLESS(Chain == old.Chain);
            Y_ABORT_UNLESS(old.ReliablyWritten <= ReliablyWritten); // value may not become 'unreliably written'
            return old.ReliablyWritten < ReliablyWritten;
        } else {
            Y_ABORT_UNLESS(old.ReliablyWritten <= ReliablyWritten); // item can't suddenly become unreliably written
            return true;
        }
    }

    void TResolvedValue::Output(IOutputStream& s) const {
        if (Defined) {
            s << '{' << FormatList(Chain) << " Version# " << Version << " ReliablyWritten# " << ReliablyWritten << '}';
        } else {
            s << "{}";
        }
    }

    TString TResolvedValue::ToString() const {
        TStringStream s;
        Output(s);
        return s.Str();
    }

} // NKikimr::NBlobDepot

template<>
void Out<NKikimr::NBlobDepot::TResolvedValue::TLink>(IOutputStream& s, const NKikimr::NBlobDepot::TResolvedValue::TLink& x) {
    x.Output(s);
}
