#include "consumer_attributes.h"

#include <ydb/core/ymq/base/limits.h>
#include <ydb/services/sqs_topic/queue_url/arn.h>

#include <library/cpp/json/json_reader.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/type.h>

#include <format>

namespace NKikimr::NSqsTopic::V1 {

    std::expected<TQueueAttributes, std::string> ParseQueueAttributes(
        const google::protobuf::Map<TString, TString>& attributes,
        const TString& queueName,
        const TString& /*consumerName*/,
        const TString& database,
        EConsumerAttributeUsageTarget usageTarget
    ) {
        TQueueAttributes result;

        bool fifoQueueByName = queueName.EndsWith(".fifo");
        TMaybe<bool> fifoQueueByAttr;

        for (const auto& [keyStr, value] : attributes) {
            const std::string_view key = keyStr;

            if (key == "VisibilityTimeout") {
                TMaybe<ui32> timeout = TryFromString<ui32>(value);
                if (!timeout) {
                    return std::unexpected(std::format("Invalid VisibilityTimeout"));
                }
                result.DefaultProcessingTimeout = TDuration::Seconds(*timeout);
            } else if (key == "DelaySeconds") {
                TMaybe<ui32> delay = TryFromString<ui32>(value);
                if (!delay) {
                    return std::unexpected(std::format("Invalid DelaySeconds"));
                }
                result.ReceiveMessageDelay = TDuration::Seconds(*delay);
            } else if (key == "ReceiveMessageWaitTimeSeconds") {
                TMaybe<ui32> waitTime = TryFromString<ui32>(value);
                if (!waitTime) {
                    return std::unexpected(std::format("Invalid ReceiveMessageWaitTimeSeconds"));
                }
                result.ReceiveMessageWaitTime = TDuration::Seconds(*waitTime);
            } else if (key == "FifoQueue") {
                fifoQueueByAttr = IsTrue(value);
            } else if (key == "ContentBasedDeduplication") {
                bool dedup = IsTrue(value);
                result.ContentBasedDeduplication = dedup;
            } else if (key == "RedrivePolicy") {
                try {
                    NJson::TJsonValue json;
                    if (!NJson::ReadJsonTree(value, &json)) {
                        return std::unexpected("Invalid RedrivePolicy JSON");
                    }
                    if (!json.Has("deadLetterTargetArn")) {
                        return std::unexpected("Missing deadLetterTargetArn in RedrivePolicy");
                    }
                    if (!json.Has("maxReceiveCount")) {
                        return std::unexpected("Missing maxReceiveCount in RedrivePolicy");
                    }
                    {
                        ui32 maxAttempts = json["maxReceiveCount"].GetUIntegerRobust();
                        result.MaxReceiveCount = maxAttempts;
                    }
                    {
                        TString arn = json["deadLetterTargetArn"].GetStringRobust();

                        auto dlqUrl = ParseQueueArn(arn);
                        if (!dlqUrl.has_value()) {
                            return std::unexpected(TString::Join("Invalid deadLetterTargetArn: ", dlqUrl.error()));
                        }
                        if (dlqUrl->Database != database) {
                            return std::unexpected(TString::Join("DLQ database '", dlqUrl->Database, "' does not match queue database '", database, "'"));
                        }
                        result.DeadLetterQueue = dlqUrl->TopicPath;
                    }
                } catch (...) {
                    return std::unexpected("Failed to parse RedrivePolicy");
                }
            } else if (key == "MessageRetentionPeriod") {
                TMaybe<ui64> period = TryFromString<ui64>(value);
                if (!period) {
                    return std::unexpected(std::format("Invalid MessageRetentionPeriod"));
                }
                result.MessageRetentionPeriod = TDuration::Seconds(*period);
            } else if (key == "MaximumMessageSize") {
                TMaybe<ui32> size = TryFromString<ui32>(value);
                if (!size) {
                    return std::unexpected(std::format("Invalid MaximumMessageSize"));
                }
                result.MaximumMessageSize = *size;
            } else if (key == "KmsMasterKeyId") {
                result.KmsMasterKeyId = value;
            } else if (key == "KmsDataKeyReusePeriodSeconds") {
                TMaybe<ui32> period = TryFromString<ui32>(value);
                if (!period) {
                    return std::unexpected(std::format("Invalid KmsDataKeyReusePeriodSeconds"));
                }
                result.KmsDataKeyReusePeriodSeconds = TDuration::Seconds(*period);
            } else if (key == "SqsManagedSseEnabled") {
                result.SqsManagedSseEnabled = (value == "true");
            } else {
                return std::unexpected(std::format("Unknown attribute: {}", key));
            }
        }

        if (fifoQueueByName) {
            bool fifoQueueByAttrIfNotSet = (usageTarget == EConsumerAttributeUsageTarget::Alter) ? fifoQueueByName : false;
            if (!fifoQueueByAttr.GetOrElse(fifoQueueByAttrIfNotSet)) {
                return std::unexpected("Fifo queue with an '.fifo' suffix should have the FifoQueue attribute");
            }
        }

        result.FifoQueue = fifoQueueByName || fifoQueueByAttr.GetOrElse(false);

        return result;
    }

    std::expected<void, std::string> CompareWithExistingQueueAttributes(
        const NKikimrPQ::TPQTabletConfig& existingConfig,
        const NKikimrPQ::TPQTabletConfig::TConsumer& existingConsumer,
        const TQueueAttributes& newConfig
    ) {
        if (newConfig.DefaultProcessingTimeout.Defined() &&
            existingConsumer.GetDefaultProcessingTimeoutSeconds() != newConfig.DefaultProcessingTimeout->Seconds()) {
            return std::unexpected(std::format(
                "VisibilityTimeout mismatch: new value is {} seconds, existing value is {} seconds",
                newConfig.DefaultProcessingTimeout->Seconds(),
                existingConsumer.GetDefaultProcessingTimeoutSeconds()
            ));
        }
        if (newConfig.ReceiveMessageDelay.Defined()) {
            ui64 newSeconds = newConfig.ReceiveMessageDelay->Seconds();
            ui64 existingSeconds = existingConsumer.GetDefaultDelayMessageTimeMs() / 1000;
            if (existingSeconds != newSeconds) {
                return std::unexpected(std::format(
                    "ReceiveMessageDelay mismatch: new value is {} seconds, existing value is {} seconds",
                    newSeconds,
                    existingSeconds
                ));
            }
        }

        if (newConfig.ReceiveMessageWaitTime.Defined() &&
            existingConsumer.GetDefaultReceiveMessageWaitTimeMs() != newConfig.ReceiveMessageWaitTime->Seconds() * 1000) {
            return std::unexpected(std::format(
                "ReceiveMessageWaitTimeSeconds mismatch: new value is {} ms, existing value is {} ms",
                newConfig.ReceiveMessageWaitTime->Seconds() * 1000,
                existingConsumer.GetDefaultReceiveMessageWaitTimeMs()
            ));
        }

        if (existingConsumer.GetKeepMessageOrder() != newConfig.FifoQueue) {
            return std::unexpected(std::format(
                "FifoQueue mismatch: new value is {}, existing value is {}",
                newConfig.FifoQueue,
                existingConsumer.GetKeepMessageOrder()
            ));
        }

        if (newConfig.ContentBasedDeduplication.Defined()) {
            bool dedup = newConfig.ContentBasedDeduplication.GetOrElse(false);
            if (dedup != existingConfig.GetContentBasedDeduplication()) {
                return std::unexpected(std::format(
                    "ContentBasedDeduplication mismatch: new value is {}, existing value is {}",
                    dedup,
                    existingConfig.GetContentBasedDeduplication()
                ));
            }
        }

        if (newConfig.MaxReceiveCount.Defined() &&
            existingConsumer.GetMaxProcessingAttempts() != *newConfig.MaxReceiveCount) {
            return std::unexpected(std::format(
                "MaxReceiveCount mismatch: new value is {}, existing value is {}",
                *newConfig.MaxReceiveCount,
                existingConsumer.GetMaxProcessingAttempts()
            ));
        }

        if (newConfig.DeadLetterQueue.Defined() &&
            existingConsumer.GetDeadLetterQueue() != *newConfig.DeadLetterQueue) {
            return std::unexpected(TStringBuilder()
                << "DeadLetterQueue mismatch: new value is '" << *newConfig.DeadLetterQueue
                << "', existing value is '" << existingConsumer.GetDeadLetterQueue() << "'");
        }

        if (newConfig.MessageRetentionPeriod.Defined()) {
            ui64 newSeconds = newConfig.MessageRetentionPeriod->Seconds();
            ui64 existingSeconds = static_cast<ui64>(existingConfig.GetPartitionConfig().GetLifetimeSeconds());
            if (existingSeconds != newSeconds) {
                return std::unexpected(std::format(
                    "MessageRetentionPeriod mismatch: new value is {} seconds, existing value is {} seconds",
                    newSeconds,
                    existingSeconds
                ));
            }
        }

        return {};
    }

    std::expected<void, std::string> ValidateLimits(const TQueueAttributes& config) {
        if (config.DefaultProcessingTimeout.Defined()) {
            if (config.DefaultProcessingTimeout->Seconds() > NSQS::TLimits::MaxVisibilityTimeout.Seconds()) {
                return std::unexpected(std::format(
                    "VisibilityTimeout exceeds maximum of {} seconds",
                    NSQS::TLimits::MaxVisibilityTimeout.Seconds()
                ));
            }
        }

        if (config.ReceiveMessageDelay.Defined()) {
            if (config.ReceiveMessageDelay->Seconds() > NSQS::TLimits::MaxDelaySeconds) {
                return std::unexpected(std::format(
                    "ReceiveMessageDelay exceeds maximum of {} seconds",
                    NSQS::TLimits::MaxDelaySeconds
                ));
            }
        }

        if (config.MaxReceiveCount.Defined()) {
            ui32 attempts = *config.MaxReceiveCount;
            if (attempts < NSQS::TLimits::MinMaxReceiveCount ||
                attempts > NSQS::TLimits::MaxMaxReceiveCount) {
                return std::unexpected(std::format(
                    "MaxReceiveCount must be between {} and {}",
                    NSQS::TLimits::MinMaxReceiveCount,
                    NSQS::TLimits::MaxMaxReceiveCount
                ));
            }
        }

        if (config.MessageRetentionPeriod.Defined()) {
            if (*config.MessageRetentionPeriod > NSQS::TLimits::MaxMessageRetentionPeriod) {
                return std::unexpected(std::format(
                    "MessageRetentionPeriod exceeds maximum of {} days",
                    NSQS::TLimits::MaxMessageRetentionPeriod.Days()
                ));
            }
        }

        if (config.MaximumMessageSize.Defined()) {
            if (*config.MaximumMessageSize > NSQS::TLimits::MaxMessageSize) {
                return std::unexpected(std::format(
                    "MaximumMessageSize exceeds maximum of {} bytes",
                    NSQS::TLimits::MaxMessageSize
                ));
            }
        }

        return {};
    }

} // namespace NKikimr::NSqsTopic::V1
