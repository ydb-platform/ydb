#include "consumer_attributes.h"

#include <ydb/core/ymq/base/limits.h>
#include <ydb/services/sqs_topic/queue_url/arn.h>

#include <library/cpp/json/json_reader.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/type.h>

#include <format>

namespace NKikimr::NSqsTopic::V1 {

    std::expected<TConsumerAttributes, std::string> ParseConsumerAttributes(
        const google::protobuf::Map<TString, TString>& attributes,
        const TString& queueName,
        const TString& consumerName,
        const TString& database,
        EConsumerAtributeUsageTarget usageTarget
    ) {
        TConsumerAttributes result;

        auto& config = result.Consumer;
        bool fifoQueueByName = queueName.EndsWith(".fifo");
        TMaybe<bool> fifoQueueByAttr;

        for (const auto& [keyStr, value] : attributes) {
            const std::string_view key = keyStr;

            if (key == "VisibilityTimeout") {
                TMaybe<ui32> timeout = TryFromString<ui32>(value);
                if (!timeout) {
                    return std::unexpected(std::format("Invalid VisibilityTimeout"));
                }
                config.SetDefaultProcessingTimeoutSeconds(*timeout);
            } else if (key == "DelaySeconds") {
                TMaybe<ui32> delay = TryFromString<ui32>(value);
                if (!delay) {
                    return std::unexpected(std::format("Invalid DelaySeconds"));
                }
                config.SetDefaultDelayMessageTimeMs(*delay * 1000);
            } else if (key == "ReceiveMessageWaitTimeSeconds") {
                TMaybe<ui32> waitTime = TryFromString<ui32>(value);
                if (!waitTime) {
                    return std::unexpected(std::format("Invalid ReceiveMessageWaitTimeSeconds"));
                }
                config.SetDefaultReceiveMessageWaitTimeMs(*waitTime * 1000);
            } else if (key == "FifoQueue") {
                fifoQueueByAttr = IsTrue(value);
            } else if (key == "ContentBasedDeduplication") {
                bool dedup = IsTrue(value);
                config.SetContentBasedDeduplication(dedup);
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
                        config.SetMaxProcessingAttempts(maxAttempts);
                        config.SetDeadLetterPolicyEnabled(true);
                        config.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
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
                        config.SetDeadLetterQueue(dlqUrl->TopicPath);
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
                config.SetAvailabilityPeriodMs(TDuration::Seconds(*period).MilliSeconds());
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
            bool fifoQueueByAttrIfNotSet = (usageTarget == EConsumerAtributeUsageTarget::Alter) ? fifoQueueByName : false;
            if (!fifoQueueByAttr.GetOrElse(fifoQueueByAttrIfNotSet)) {
                return std::unexpected("Fifo queue with an '.fifo' suffix should have the FifoQueue attribute");
            }
        }
        const bool fifoQueue = fifoQueueByName || fifoQueueByAttr.GetOrElse(false);

        config.SetName(consumerName);
        config.SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
        config.SetKeepMessageOrder(fifoQueue);

        return result;
    }

    std::expected<void, std::string> CompareWithExistingQueueAttributes(
        const NKikimrPQ::TPQTabletConfig& existingConfig,
        const NKikimrPQ::TPQTabletConfig::TConsumer& existingConsumer,
        const TConsumerAttributes& newConfig
    ) {
        const auto& newConsumer = newConfig.Consumer;

        if (newConsumer.HasDefaultProcessingTimeoutSeconds() &&
            existingConsumer.GetDefaultProcessingTimeoutSeconds() != newConsumer.GetDefaultProcessingTimeoutSeconds()) {
            return std::unexpected(std::format(
                "VisibilityTimeout mismatch: new value is {} seconds, existing value is {} seconds",
                newConsumer.GetDefaultProcessingTimeoutSeconds(),
                existingConsumer.GetDefaultProcessingTimeoutSeconds()
            ));
        }

        if (newConsumer.HasDefaultDelayMessageTimeMs() &&
            existingConsumer.GetDefaultDelayMessageTimeMs() != newConsumer.GetDefaultDelayMessageTimeMs()) {
            return std::unexpected(std::format(
                "DelaySeconds mismatch: new value is {} ms, existing value is {} ms",
                newConsumer.GetDefaultDelayMessageTimeMs(),
                existingConsumer.GetDefaultDelayMessageTimeMs()
            ));
        }

        if (newConsumer.HasDefaultReceiveMessageWaitTimeMs() &&
            existingConsumer.GetDefaultReceiveMessageWaitTimeMs() != newConsumer.GetDefaultReceiveMessageWaitTimeMs()) {
            return std::unexpected(std::format(
                "ReceiveMessageWaitTimeSeconds mismatch: new value is {} ms, existing value is {} ms",
                newConsumer.GetDefaultReceiveMessageWaitTimeMs(),
                existingConsumer.GetDefaultReceiveMessageWaitTimeMs()
            ));
        }

        if (newConsumer.HasKeepMessageOrder() &&
            existingConsumer.GetKeepMessageOrder() != newConsumer.GetKeepMessageOrder()) {
            return std::unexpected(std::format(
                "FifoQueue mismatch: new value is {}, existing value is {}",
                newConsumer.GetKeepMessageOrder(),
                existingConsumer.GetKeepMessageOrder()
            ));
        }

        if (newConsumer.HasContentBasedDeduplication() &&
            existingConsumer.GetContentBasedDeduplication() != newConsumer.GetContentBasedDeduplication()) {
            return std::unexpected(std::format(
                "ContentBasedDeduplication mismatch: new value is {}, existing value is {}",
                newConsumer.GetContentBasedDeduplication(),
                existingConsumer.GetContentBasedDeduplication()
            ));
        }

        if (newConsumer.HasMaxProcessingAttempts() &&
            existingConsumer.GetMaxProcessingAttempts() != newConsumer.GetMaxProcessingAttempts()) {
            return std::unexpected(std::format(
                "MaxReceiveCount mismatch: new value is {}, existing value is {}",
                newConsumer.GetMaxProcessingAttempts(),
                existingConsumer.GetMaxProcessingAttempts()
            ));
        }

        if (newConsumer.HasDeadLetterPolicyEnabled() &&
            existingConsumer.GetDeadLetterPolicyEnabled() != newConsumer.GetDeadLetterPolicyEnabled()) {
            return std::unexpected(std::format(
                "DeadLetterPolicyEnabled mismatch: new value is {}, existing value is {}",
                newConsumer.GetDeadLetterPolicyEnabled(),
                existingConsumer.GetDeadLetterPolicyEnabled()
            ));
        }

        if (newConsumer.HasDeadLetterPolicy() &&
            existingConsumer.GetDeadLetterPolicy() != newConsumer.GetDeadLetterPolicy()) {
            return std::unexpected(std::format(
                "DeadLetterPolicy mismatch: new value is {}, existing value is {}",
                static_cast<int>(newConsumer.GetDeadLetterPolicy()),
                static_cast<int>(existingConsumer.GetDeadLetterPolicy())
            ));
        }

        if (newConsumer.HasDeadLetterQueue() &&
            existingConsumer.GetDeadLetterQueue() != newConsumer.GetDeadLetterQueue()) {
            return std::unexpected(TStringBuilder()
                << "DeadLetterQueue mismatch: new value is '" << newConsumer.GetDeadLetterQueue()
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

    std::expected<void, std::string> ValidateLimits(const TConsumerAttributes& config) {
        const auto& consumer = config.Consumer;

        if (consumer.HasDefaultProcessingTimeoutSeconds()) {
            if (consumer.GetDefaultProcessingTimeoutSeconds() > NSQS::TLimits::MaxVisibilityTimeout.Seconds()) {
                return std::unexpected(std::format(
                    "VisibilityTimeout exceeds maximum of {} seconds",
                    NSQS::TLimits::MaxVisibilityTimeout.Seconds()
                ));
            }
        }

        if (consumer.HasDefaultDelayMessageTimeMs()) {
            if (consumer.GetDefaultDelayMessageTimeMs() > NSQS::TLimits::MaxDelaySeconds * 1000) {
                return std::unexpected(std::format(
                    "DelaySeconds exceeds maximum of {} seconds",
                    NSQS::TLimits::MaxDelaySeconds
                ));
            }
        }

        if (consumer.HasMaxProcessingAttempts()) {
            ui32 attempts = consumer.GetMaxProcessingAttempts();
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
