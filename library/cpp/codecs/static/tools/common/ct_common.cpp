#include "ct_common.h"

#include <library/cpp/codecs/codecs.h>
#include <library/cpp/codecs/static/static_codec_info.pb.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/stream/output.h> 
#include <util/string/builder.h>
#include <util/system/hp_timer.h>

namespace NCodecs {
    TString TComprStats::Format(const TStaticCodecInfo& info, bool checkMode) const {
        TStringBuilder s;
        s << "raw size/item:      " << RawSizePerRecord() << Endl;
        s << "enc.size/item:      " << EncSizePerRecord() << Endl;
        if (checkMode) {
            s << "orig.enc.size/item: " << OldEncSizePerRecord(info.GetDebugInfo().GetCompression()) << Endl;
        }
        s << "enc time us/item:   " << EncTimePerRecordUS() << Endl;
        s << "dec time us/item:   " << DecTimePerRecordUS() << Endl;
        s << "dict size:          " << info.GetStoredCodec().Size() << Endl;
        s << "compression:        " << AsPercent(Compression()) << " %" << Endl;
        if (checkMode) {
            s << "orig.compression:   " << AsPercent(info.GetDebugInfo().GetCompression()) << " %" << Endl;
        }
        return s;
    }

    TComprStats TestCodec(const ICodec& c, const TVector<TString>& input) {
        TComprStats stats;

        TBuffer encodeBuffer;
        TBuffer decodeBuffer;
        for (const auto& data : input) {
            encodeBuffer.Clear();
            decodeBuffer.Clear();

            stats.Records += 1;
            stats.RawSize += data.size();

            THPTimer timer;
            c.Encode(data, encodeBuffer);
            stats.EncSize += encodeBuffer.size();
            stats.EncSeconds += timer.PassedReset();

            c.Decode(TStringBuf{encodeBuffer.data(), encodeBuffer.size()}, decodeBuffer);
            stats.DecSeconds += timer.PassedReset();
            Y_ENSURE(data == TStringBuf(decodeBuffer.data(), decodeBuffer.size()), "invalid encoding at record " << stats.Records);
        }

        return stats;
    }

    void ParseBlob(TVector<TString>& result, EDataStreamFormat fmt, const TBlob& blob) {
        TStringBuf bin(blob.AsCharPtr(), blob.Size());
        TStringBuf line;
        TString buffer;
        while (bin.ReadLine(line)) {
            if (DSF_BASE64_LF == fmt) {
                Base64Decode(line, buffer);
                line = buffer;
            }
            if (!line) {
                continue;
            }
            result.emplace_back(line.data(), line.size());
        }
    }

    TBlob GetInputBlob(const TString& dataFile) {
        return dataFile && dataFile != "-" ? TBlob::FromFile(dataFile) : TBlob::FromStream(Cin);
    }

}
