#include "utils.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/public/mlp/mlp.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/http_config.pb.h>

#include <library/cpp/digest/md5/md5.h>

#include <util/system/hostname.h>

#include <format>

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

    TString GenerateMessageId(const TString& database, const TString& topicPath, const NPQ::NMLP::TMessageId& pos) {
        MD5 md5;
        md5.Init();
        md5.Update(database);
        md5.Update("/");
        md5.Update(topicPath);
        md5.Update("/");
        md5.Update(ToString(pos.PartitionId));
        md5.Update("/");
        md5.Update(ToString(pos.Offset));
        ui8 digest[16];
        md5.Final(digest);
        // make guid v3 like
        digest[8] &= 0b10111111;
        digest[8] |= 0b10000000;
        digest[6] &= 0b01011111;
        digest[6] |= 0b01010000;

        TStringBuilder res;
        for (int i = 0; i < std::ssize(digest); ++i) {
            res << (EqualToOneOf(i, 4, 6, 8, 10) ? "-" : "") << Hex(digest[i], HF_FULL);
        }
        return res;
    }
} // namespace NKikimr::NSqsTopic
