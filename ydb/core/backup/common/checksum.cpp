#include "checksum.h"

#include <openssl/sha.h>

#include <util/string/hex.h>

namespace {

template <typename T, size_t N>
void FillArrayFromProto(T (&array)[N], const NProtoBuf::RepeatedField<T>& proto) {
    for (int i = 0; i < proto.size(); ++i) {
        if (static_cast<size_t>(i) < std::size(array)) {
            array[i] = proto.Get(i);
        }
    }
}

} // anonymous

namespace NKikimr::NBackup {

class TSHA256 : public IChecksum {
public:
    TSHA256() {
        SHA256_Init(&Context);
    }

    void AddData(TStringBuf data) override {
        SHA256_Update(&Context, data.data(), data.size());
    }

    TString Finalize() override {
        unsigned char hash[SHA256_DIGEST_LENGTH];
        SHA256_Final(hash, &Context);
        return to_lower(HexEncode(hash, SHA256_DIGEST_LENGTH));
    }

    TChecksumState GetState() const override {
        TChecksumState state;
        auto& sha256State = *state.MutableSha256State();

        for (ui32 h : Context.h) {
            sha256State.AddH(h);
        }
        sha256State.SetNh(Context.Nh);
        sha256State.SetNl(Context.Nl);
        for (ui32 data : Context.data) {
            sha256State.AddData(data);
        }
        sha256State.SetNum(Context.num);
        sha256State.SetMdLen(Context.md_len);

        return state;
    }

    void Continue(const TChecksumState& state) override {
        const auto& sha256State = state.GetSha256State();
        SHA256_Init(&Context);
        FillArrayFromProto(Context.h, sha256State.GetH());
        Context.Nh = sha256State.GetNh();
        Context.Nl = sha256State.GetNl();
        FillArrayFromProto(Context.data, sha256State.GetData());
        Context.num = sha256State.GetNum();
        Context.md_len = sha256State.GetMdLen();
    }

private:
    SHA256_CTX Context;
};

TString ComputeChecksum(TStringBuf data) {
    IChecksum::TPtr checksum(CreateChecksum());
    checksum->AddData(data);
    return checksum->Finalize();
}

IChecksum* CreateChecksum() {
    return new TSHA256();
}

TString ChecksumKey(const TString& objKey) {
    TString ret;
    TStringBuf key = objKey;
    key.ChopSuffix(".enc"); // We calculate checksum for nonencrypted data, so cut .enc suffix
    ret = key;
    ret += ".sha256";
    return ret;
}

} // NKikimr::NBackup
