#include <library/cpp/codecs/static/tools/common/ct_common.h>
#include <library/cpp/codecs/static/static.h>
#include <library/cpp/codecs/static/static_codec_info.pb.h>
#include <library/cpp/codecs/codecs.h>
#include <library/cpp/getopt/small/last_getopt.h>

#include <util/digest/city.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/stream/buffer.h>
#include <util/stream/format.h>
#include <util/string/builder.h>

int main(int argc, char** argv) {
    NCodecs::TCodecPtr codecPtr;
    NCodecs::EDataStreamFormat fmt = NCodecs::DSF_NONE;
    TString codecFile; 
    bool testCompression = false;

    auto opts = NLastGetopt::TOpts::Default();
    opts.SetTitle("Prints a .codec_info file and optionally checks its performance on new data. See also static_codec_generator.");
    opts.SetCmdLineDescr("-c 9089f3e9b7a0f0d4.codec_info -t -f base64 qtrees.sample.txt");
    NCodecs::TStaticCodecInfo codec;

    opts.AddLongOption('c', "codec-info").RequiredArgument("codec_info").Handler1T<TString>([&codecFile, &codec, &codecPtr](TString name) {
                                                                            codecFile = name;
                                                                            codec.CopyFrom(NCodecs::LoadCodecInfoFromString(TUnbufferedFileInput(name).ReadAll()));
                                                                            codecPtr = NCodecs::ICodec::RestoreFromString(codec.GetStoredCodec());
                                                                        })
        .Required()
        .Help(".codec_info file with serialized static data for codec");

    opts.AddLongOption('t', "test").NoArgument().StoreValue(&testCompression, true).Optional().Help("test current performance");

    opts.AddLongOption('f', "format").RequiredArgument(TStringBuilder() << "(" << NCodecs::DSF_PLAIN_LF << "|" << NCodecs::DSF_BASE64_LF << ")").StoreResult(&fmt).Optional().Help("test set input file format");

    opts.SetFreeArgsMin(0);
    opts.SetFreeArgTitle(0, "testing_set_input_file", "testing set input files");

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    Cout << codecFile << Endl;
    Cout << NCodecs::FormatCodecInfo(codec) << Endl;

    if (testCompression) {
        if (NCodecs::DSF_NONE == fmt) {
            Cerr << "Specify format (-f|--format) for testing set input" << Endl;
            exit(1);
        }

        Cout << "Reading testing set data ... " << Flush;

        TVector<TString> allData; 
        for (const auto& freeArg : res.GetFreeArgs()) {
            NCodecs::ParseBlob(allData, fmt, NCodecs::GetInputBlob(freeArg));
        }

        if (!res.GetFreeArgs()) {
            NCodecs::ParseBlob(allData, fmt, NCodecs::GetInputBlob("-"));
        }

        Cout << "Done" << Endl << Endl;

        Cout << "records:  " << allData.size() << Endl;
        Cout << "raw size: " << NCodecs::GetInputSize(allData.begin(), allData.end()) << " bytes" << Endl << Endl;

        Cout << "Testing compression ... " << Flush;
        auto stats = NCodecs::TestCodec(*codecPtr, allData);
        Cout << "Done" << Endl << Endl;

        Cout << stats.Format(codec, true) << Endl;
    }
}
