#include "builder.h" 
#include "common.h" 
 
#include <library/cpp/codecs/static/static_codec_info.pb.h>
 
#include <library/cpp/codecs/codecs.h>
 
#include <util/generic/yexception.h> 
#include <util/string/subst.h> 
 
namespace NCodecs { 
    TStaticCodecInfo BuildStaticCodec(const TVector<TString>& trainingData, const TCodecBuildInfo& info) {
        TStaticCodecInfo result; 
        TCodecPtr codec = ICodec::GetInstance(info.CodecName); 
        Y_ENSURE_EX(codec, TCodecException() << "empty codec is not allowed"); 
 
        codec->LearnX(trainingData.begin(), trainingData.end(), info.SampleSizeMultiplier); 
        { 
            TStringOutput sout{*result.MutableStoredCodec()}; 
            ICodec::Store(&sout, codec); 
        } 
 
        auto& debugInfo = *result.MutableDebugInfo(); 
        debugInfo.SetStoredCodecHash(DataSignature(result.GetStoredCodec())); 
        debugInfo.SetCodecName(info.CodecName); 
        debugInfo.SetSampleSizeMultiplier(info.SampleSizeMultiplier); 
        debugInfo.SetTimestamp(info.Timestamp); 
        debugInfo.SetRevisionInfo(info.RevisionInfo); 
        debugInfo.SetTrainingSetComment(info.TrainingSetComment); 
        debugInfo.SetTrainingSetResId(info.TrainingSetResId); 
        return result; 
    } 
 
    TString GetStandardFileName(const TStaticCodecInfo& info) {
        TString cName = info.GetDebugInfo().GetCodecName();
        SubstGlobal(cName, ':', '.'); 
        return TStringBuilder() << cName << "." << info.GetDebugInfo().GetTimestamp() << ".codec_info"; 
    } 
} 
