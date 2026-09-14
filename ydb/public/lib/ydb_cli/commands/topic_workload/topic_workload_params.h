#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/vector.h>
#include <util/system/types.h>
#include <util/string/type.h>

namespace NYdb {
    namespace NConsoleClient {
        class TCommandWorkloadTopicParams {
        public:
            static TVector<NYdb::NTopic::ECodec> GetWriteAllowedCodecs();
            static TVector<NYdb::NTopic::ECodec> GetBatchInnerAllowedCodecs();
            static ui32 StrToCodec(const TString& str);
            static ui32 StrToBatchInnerCodec(const TString& str);
            static ui64 StrToBytes(const TString& str);
        };
    }
}