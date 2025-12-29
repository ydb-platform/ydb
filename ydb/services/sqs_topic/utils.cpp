#include "utils.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/public/mlp/mlp.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/http_config.pb.h>

#include <library/cpp/digest/md5/md5.h>

#include <util/system/hostname.h>
#include <util/system/unaligned_mem.h>

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

    TVector<std::pair<TString, TString>> GetMetricsLabels(
        const TString& databasePath,
        const TString& topicPath,
        const TString& consumerName,
        const TString& method,
        TVector<std::pair<TString, TString>>&& labels
    ) {
        TVector<std::pair<TString, TString>> common{
            {"database", databasePath},
            {"method", method},
            {"topic", topicPath},
            {"consumer", consumerName},
        };
        std::move(labels.begin(), labels.end(), std::back_inserter(common));
        return common;
    }

    TVector<std::pair<TString, TString>> GetRequestMessageCountMetricsLabels(
        const TString& databasePath,
        const TString& topicPath,
        const TString& consumer,
        const TString& method
    ) {
        return GetMetricsLabels(
            databasePath,
            topicPath,
            consumer,
            method,
            {
                {"name", "api.sqs.request.message_count"}
            }
        );
    }

    TVector<std::pair<TString, TString>> GetResponseMessageCountMetricsLabels(
        const TString& databasePath,
        const TString& topicPath,
        const TString& consumer,
        const TString& method,
        const TString& status
    ) {
        return GetMetricsLabels(
            databasePath,
            topicPath,
            consumer,
            method,
            {
                {"name", "api.sqs.response.message_count"},
                {"status", status}
            }
        );
    }

    TVector<std::pair<TString, TString>> GetResponseEmptyCountMetricsLabels(
        const TString& databasePath,
        const TString& topicPath,
        const TString& consumer,
        const TString& method
    ) {
        return GetMetricsLabels(
            databasePath,
            topicPath,
            consumer,
            method,
            {
                {"name", "api.sqs.response.empty_count"}
            }
        );
    }


    ui64 SampleIdFromRequestId(const TStringBuf requestId) {
        if (sizeof(ui64) <= requestId.size()) [[likely]] {
            return ReadUnaligned<ui64>(requestId.data());
        }
        return 0;
    }
} // namespace NKikimr::NSqsTopic
