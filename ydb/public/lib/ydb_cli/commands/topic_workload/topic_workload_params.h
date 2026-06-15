#pragma once

#include <util/system/types.h>
#include <util/string/type.h>

namespace NYdb {
    namespace NConsoleClient {
        class TCommandWorkloadTopicParams {
        public:
            static ui32 StrToCodec(const TString& str);
            static ui64 StrToBytes(const TString& str);
        };
    }
}