#pragma once

#include <util/generic/string.h> 
#include <util/generic/vector.h>
#include <util/memory/blob.h>
#include <cmath>

namespace NCodecs {
    class TStaticCodecInfo;
    class ICodec;

    struct TComprStats {
        double EncSeconds = 0;
        double DecSeconds = 0;
        size_t Records = 0;
        size_t RawSize = 0;
        size_t EncSize = 0;

        static double Round(double n, size_t decPlaces = 2) {
            double p = pow(10, decPlaces);
            return round(n * p) / p;
        }

        static double AsPercent(double n) {
            return Round(n * 100);
        }

        static double AsMicroSecond(double s) {
            return s * 1000000;
        }

        double PerRecord(double n) const {
            return Round((double)(Records ? n / Records : 0));
        }

        double Compression() const {
            return ((double)RawSize - (double)EncSize) / RawSize;
        }

        double EncTimePerRecordUS() const {
            return PerRecord(AsMicroSecond(EncSeconds));
        }

        double DecTimePerRecordUS() const {
            return PerRecord(AsMicroSecond(DecSeconds));
        }

        double RawSizePerRecord() const {
            return PerRecord(RawSize);
        }

        double EncSizePerRecord() const {
            return PerRecord(EncSize);
        }

        double OldEncSizePerRecord(double compr) const {
            return PerRecord((1 - compr) * RawSize);
        }

        TString Format(const TStaticCodecInfo&, bool checkMode) const; 
    };

    TComprStats TestCodec(const ICodec&, const TVector<TString>& data); 

    enum EDataStreamFormat {
        DSF_NONE,
        DSF_PLAIN_LF /* "plain" */,
        DSF_BASE64_LF /* "base64" */,
    };

    void ParseBlob(TVector<TString>&, EDataStreamFormat, const TBlob&); 

    TBlob GetInputBlob(const TString& dataFile); 

}
