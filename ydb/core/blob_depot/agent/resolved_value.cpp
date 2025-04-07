#include "resolved_value.h"

namespace NKikimr::NBlobDepot {

    TResolvedValue::TLink::TLink(const NKikimrBlobDepot::TResolvedValueChain& link)
        : SubrangeBegin(link.GetSubrangeBegin())
        , SubrangeEnd(link.GetSubrangeEnd())
    {
        std::optional<ui32> length;
        if (link.HasBlobId() && link.HasGroupId()) {
            const TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(link.GetBlobId());
            Blob.emplace(blobId, link.GetGroupId());
            Y_VERIFY_S(!length || *length == blobId.BlobSize(), SingleLineProto(link));
            length.emplace(blobId.BlobSize());
        }
        if (link.HasS3Locator()) {
            S3Locator.emplace(TS3Locator::FromProto(link.GetS3Locator()));
            Y_VERIFY_S(!length || *length == S3Locator->Len, SingleLineProto(link));
            length.emplace(S3Locator->Len);
        }
        if (!link.HasSubrangeEnd()) {
            Y_VERIFY_S(length, SingleLineProto(link));
            SubrangeEnd = *length;
        }
    }

    void TResolvedValue::TLink::Output(IOutputStream& s) const {
        if (Blob) {
            const auto& [blobId, groupId] = *Blob;
            s << blobId << '@' << groupId;
        }
        if (S3Locator) {
            s << S3Locator->ToString();
        }
        s << '{' << SubrangeBegin << '-' << SubrangeEnd - 1 << '}';
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
