#include "utils.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/http_config.pb.h>

#include <util/system/hostname.h>

namespace NKikimr::NSqsTopic {

    TQueueNameWithConsumer SplitExtendedQueueName(TStringBuf queueNameExt) {
        TQueueNameWithConsumer result;
        if (!queueNameExt.TryRSplit('@', result.QueueName, result.Consumer)) {
            result.QueueName = queueNameExt;
        }
        return result;
    }

    TString GetEndpoint(const NKikimrConfig::TSqsConfig& config) {
        const TString& endpoint = config.GetEndpoint();
        if (endpoint) {
            return endpoint;
        } else {
            return TStringBuilder() << "http://" << FQDNHostName() << ":" << config.GetHttpServerConfig().GetPort();
        }
    }

    const NKikimrConfig::TSqsConfig& Cfg() {
        return AppData()->SqsConfig;
    }
}
