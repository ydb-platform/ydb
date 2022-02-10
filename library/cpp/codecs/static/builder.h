#pragma once

#include "static.h"

#include <library/cpp/svnversion/svnversion.h>

#include <util/datetime/base.h>
#include <util/generic/string.h> 
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCodecs {
    struct TCodecBuildInfo {
        // optimal values from SEARCH-1655
        TString CodecName = "solar-8k-a:zstd08d-1"; 
        float SampleSizeMultiplier = 1;

        // debug info:
        time_t Timestamp = TInstant::Now().TimeT();
        TString RevisionInfo = (TStringBuilder() << "r" << ToString(GetProgramSvnRevision())); 
        TString TrainingSetComment; // a human comment on the training data 
        TString TrainingSetResId;   // sandbox resid of the training set
    };

    TStaticCodecInfo BuildStaticCodec(const TVector<TString>& trainingData, const TCodecBuildInfo&); 

    TString GetStandardFileName(const TStaticCodecInfo&); 

}
