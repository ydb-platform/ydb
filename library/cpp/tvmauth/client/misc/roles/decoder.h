#pragma once 
 
#include <util/generic/string.h> 
 
namespace NTvmAuth::NRoles { 
    class TDecoder { 
    public: 
        static TString Decode(const TStringBuf codec, TString&& blob); 
 
    public: 
        struct TCodecInfo { 
            TStringBuf Type; 
            size_t Size = 0; 
            TStringBuf Sha256; 
 
            bool operator==(const TCodecInfo& o) const { 
                return Type == o.Type && 
                       Size == o.Size && 
                       Sha256 == o.Sha256; 
            } 
        }; 
 
        static TCodecInfo ParseCodec(TStringBuf codec); 
        static TString DecodeImpl(TStringBuf codec, const TString& blob); 
        static TString DecodeBrolti(const TString& blob); 
        static TString DecodeGzip(const TString& blob); 
        static TString DecodeZstd(const TString& blob); 
 
        static void VerifySize(const TStringBuf decoded, size_t expected); 
        static void VerifyChecksum(const TStringBuf decoded, const TStringBuf expected); 
    }; 
} 
