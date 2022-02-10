#include "static.h"
#include "common.h"

#include <library/cpp/codecs/static/static_codec_info.pb.h>
#include <library/cpp/archive/yarchive.h>

#include <util/draft/datetime.h>

#include <util/string/builder.h>
#include <util/stream/buffer.h>
#include <util/stream/mem.h>
#include <util/string/hex.h>
#include <util/ysaveload.h>

namespace NCodecs {
    static constexpr TStringBuf STATIC_CODEC_INFO_MAGIC = "CodecInf";

    static TStringBuf GetStaticCodecInfoMagic() {
        return STATIC_CODEC_INFO_MAGIC;
    }

    void SaveCodecInfoToStream(IOutputStream& out, const TStaticCodecInfo& info) { 
        TBufferOutput bout;
        info.SerializeToArcadiaStream(&bout);
        ui64 hash = DataSignature(bout.Buffer());
        out.Write(GetStaticCodecInfoMagic());
        ::Save(&out, hash);
        ::Save(&out, bout.Buffer());
    }

    TStaticCodecInfo LoadCodecInfoFromStream(IInputStream& in) { 
        {
            TBuffer magic;
            magic.Resize(GetStaticCodecInfoMagic().size());
            Y_ENSURE_EX(in.Read(magic.Data(), GetStaticCodecInfoMagic().size()) == GetStaticCodecInfoMagic().size(),
                        TCodecException() << "bad codec info");
            Y_ENSURE_EX(TStringBuf(magic.data(), magic.size()) == GetStaticCodecInfoMagic(),
                        TCodecException() << "bad codec info");
        }

        ui64 hash;
        ::Load(&in, hash);
        TBuffer info;
        ::Load(&in, info);
        Y_ENSURE_EX(hash == DataSignature(info), TCodecException() << "bad codec info");

        TStaticCodecInfo result;
        Y_ENSURE_EX(result.ParseFromArray(info.data(), info.size()), TCodecException() << "bad codec info");

        return result;
    }

    TString SaveCodecInfoToString(const TStaticCodecInfo& info) {
        TStringStream s;
        SaveCodecInfoToStream(s, info);
        return s.Str();
    }

    TStaticCodecInfo LoadCodecInfoFromString(TStringBuf data) {
        TMemoryInput m{data.data(), data.size()};
        return LoadCodecInfoFromStream(m);
    }

    TString FormatCodecInfo(const TStaticCodecInfo& ci) {
        TStringBuilder s;
        s << "codec name:      " << ci.GetDebugInfo().GetCodecName() << Endl;
        s << "codec hash:      " << HexWriteScalar(ci.GetDebugInfo().GetStoredCodecHash()) << Endl;
        s << "dict size:       " << ci.GetStoredCodec().Size() << Endl;
        s << "sample mult:     " << ci.GetDebugInfo().GetSampleSizeMultiplier() << Endl;
        s << "orig.compress:   " << ci.GetDebugInfo().GetCompression() * 100 << " %" << Endl;
        s << "timestamp:       " << ci.GetDebugInfo().GetTimestamp() << " ("
          << NDatetime::TSimpleTM::NewLocal(ci.GetDebugInfo().GetTimestamp()).ToString()
          << ")" << Endl;
        s << "revision:        " << ci.GetDebugInfo().GetRevisionInfo() << Endl;
        s << "training set comment: " << ci.GetDebugInfo().GetTrainingSetComment() << Endl;
        s << "training set resId:   " << ci.GetDebugInfo().GetTrainingSetResId() << Endl;
        return s;
    }

    TString LoadStringFromArchive(const ui8* begin, size_t size) {
        TArchiveReader ar(TBlob::NoCopy(begin, size));
        Y_VERIFY(ar.Count() == 1, "invalid number of entries");
        auto blob = ar.ObjectBlobByKey(ar.KeyByIndex(0));
        return TString{blob.AsCharPtr(), blob.Size()};
    }

    TCodecConstPtr RestoreCodecFromCodecInfo(const TStaticCodecInfo& info) {
        return NCodecs::ICodec::RestoreFromString(info.GetStoredCodec());
    }

    TCodecConstPtr RestoreCodecFromArchive(const ui8* begin, size_t size) {
        const auto& data = LoadStringFromArchive(begin, size);
        const auto& info = LoadCodecInfoFromString(data);
        const auto& codec = RestoreCodecFromCodecInfo(info);
        Y_ENSURE_EX(codec, TCodecException() << "null codec");
        return codec;
    }
}
