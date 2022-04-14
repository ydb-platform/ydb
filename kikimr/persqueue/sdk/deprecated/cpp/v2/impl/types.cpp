#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>

#include <library/cpp/streams/lzop/lzop.h>
#include <library/cpp/streams/zstd/zstd.h>

#include <util/generic/store_policy.h>
#include <util/generic/utility.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>

namespace NPersQueue {

const TString& TServerSetting::GetRootDatabase() {
    static const TString RootDatabase = "/Root";
    return RootDatabase;
}

class TZLibToStringCompressor: private TEmbedPolicy<TStringOutput>, public TZLibCompress {
public:
    TZLibToStringCompressor(TString& dst, ZLib::StreamType type, size_t quality)
        : TEmbedPolicy<TStringOutput>(dst)
        , TZLibCompress(TEmbedPolicy::Ptr(), type, quality)
    {
    }
};

class TLzopToStringCompressor: private TEmbedPolicy<TStringOutput>, public TLzopCompress {
public:
    TLzopToStringCompressor(TString& dst)
        : TEmbedPolicy<TStringOutput>(dst)
        , TLzopCompress(TEmbedPolicy::Ptr())
    {
    }
};

class TZstdToStringCompressor: private TEmbedPolicy<TStringOutput>, public TZstdCompress {
public:
    TZstdToStringCompressor(TString& dst, int quality)
        : TEmbedPolicy<TStringOutput>(dst)
        , TZstdCompress(TEmbedPolicy::Ptr(), quality)
    {
    }
};


TData TData::Encode(TData source, ECodec defaultCodec, int quality) {
    Y_VERIFY(!source.Empty());
    TData data = std::move(source);
    if (data.IsEncoded()) {
        return data;
    }
    Y_VERIFY(defaultCodec != ECodec::RAW && defaultCodec != ECodec::DEFAULT);
    if (data.Codec == ECodec::DEFAULT) {
        data.Codec = defaultCodec;
    }
    THolder<IOutputStream> coder = CreateCoder(data.Codec, data, quality);
    coder->Write(data.SourceData);
    coder->Finish();  // &data.EncodedData may be already invalid on coder destruction
    return data;
}

TData TData::MakeRawIfNotEncoded(TData source) {
    Y_VERIFY(!source.Empty());
    TData data = std::move(source);
    if (!data.IsEncoded()) {
        data.EncodedData = data.SourceData;
        data.Codec = ECodec::RAW;
    }
    return data;
}

THolder<IOutputStream> TData::CreateCoder(ECodec codec, TData& result, int quality) {
    switch (codec) {
    case ECodec::GZIP:
        return MakeHolder<TZLibToStringCompressor>(result.EncodedData, ZLib::GZip, quality >= 0 ? quality : 6);
    case ECodec::LZOP:
        return MakeHolder<TLzopToStringCompressor>(result.EncodedData);
    case ECodec::ZSTD:
        return MakeHolder<TZstdToStringCompressor>(result.EncodedData, quality);
    default:
        Y_FAIL("NOT IMPLEMENTED CODEC TYPE");
    }
}


} // namespace NPersQueue
