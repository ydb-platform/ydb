#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb {
    namespace NConsoleClient {

        class TCommandWorkloadTopic: public TClientCommandTree {
        public:
            TCommandWorkloadTopic();
        };
    }
}
