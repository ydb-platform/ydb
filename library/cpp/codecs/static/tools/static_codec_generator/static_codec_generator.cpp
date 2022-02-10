#include <library/cpp/codecs/static/tools/common/ct_common.h>
#include <library/cpp/codecs/static/static_codec_info.pb.h>
#include <library/cpp/codecs/static/builder.h>
#include <library/cpp/codecs/codecs.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

int main(int argc, char** argv) {
    NCodecs::TCodecBuildInfo info;
    NCodecs::EDataStreamFormat fmt = NCodecs::DSF_NONE;

    auto opts = NLastGetopt::TOpts::Default();
    opts.SetCmdLineDescr("-m 'Training set: 100000 qtrees taken from web mmeta logs' -f base64 qtrees.sample.txt");
    opts.SetTitle("Teaches the codec and serializes it as a file named CODECNAME.hash(CODECDATA).bin");

    opts.AddLongOption('m', "message").RequiredArgument("training_set_comment").StoreResult(&info.TrainingSetComment).Required().Help("a human description for the training set");

    opts.AddLongOption('r', "resource").RequiredArgument("training_set_res_id").StoreResult(&info.TrainingSetResId).Optional().Help("sandbox resource id for the training set");

    opts.AddLongOption('c', "codec").RequiredArgument("codec_name").StoreResult(&info.CodecName).Optional().DefaultValue(info.CodecName);

    opts.AddLongOption('s', "sample-multiplier").RequiredArgument("multiplier").StoreResult(&info.SampleSizeMultiplier).Optional().DefaultValue(ToString(info.SampleSizeMultiplier)).Help("multiplier for default sample size");

    opts.AddLongOption('f', "format").RequiredArgument(TStringBuilder() << "(" << NCodecs::DSF_PLAIN_LF << "|" << NCodecs::DSF_BASE64_LF << ")").StoreResult(&fmt).Required().Help("training set input file format");

    opts.AddLongOption("list-codecs").NoArgument().Handler0([]() {
                                                      Cout << JoinStrings(NCodecs::ICodec::GetCodecsList(), "\n") << Endl;
                                                      exit(0);
                                                  })
        .Optional()
        .Help("list available codecs");

    opts.AddLongOption("fake-revision").RequiredArgument("revision").StoreResult(&info.RevisionInfo).Optional().Hidden(); // replace static_codec_generator revision in debug info

    opts.AddLongOption("fake-timestamp").RequiredArgument("timestamp").StoreResult(&info.Timestamp).Optional().Hidden(); // replace generating timestamp in debug info

    opts.SetFreeArgsMin(0);
    opts.SetFreeArgTitle(0, "training_set_input_file", "training set input files");

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    Cout << "Reading training set data ... " << Flush;
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

    Cout << "Training " << info.CodecName << " , sample size multiplier is " << info.SampleSizeMultiplier << " ... " << Flush;
    auto codec = NCodecs::BuildStaticCodec(allData, info);
    Cout << "Done" << Endl;

    TString codecName = NCodecs::GetStandardFileName(codec); 
    NCodecs::TCodecPtr codecPtr = NCodecs::ICodec::RestoreFromString(codec.GetStoredCodec());

    Cout << "Testing compression ... " << Flush;
    auto stats = NCodecs::TestCodec(*codecPtr, allData);
    Cout << "Done" << Endl << Endl;

    codec.MutableDebugInfo()->SetCompression(stats.Compression());

    Cout << stats.Format(codec, false) << Endl;

    Cout << "Saving as " << codecName << " ... " << Flush;
    {
        TUnbufferedFileOutput fout{codecName}; 
        NCodecs::SaveCodecInfoToStream(fout, codec);
        fout.Finish();
    }
    Cout << "Done" << Endl << Endl;
}
