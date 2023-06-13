#pragma once

#include <ydb/core/fq/libs/config/protos/compute.pb.h>
#include <ydb/core/fq/libs/protos/fq_private.pb.h>

namespace NFq {

class TComputeConfig {
public:
    explicit TComputeConfig(const NFq::NConfig::TComputeConfig& computeConfig)
        : ComputeConfig(computeConfig)
    {}

    NFq::NConfig::EComputeType GetComputeType(const Fq::Private::GetTaskResult::Task& task) const {
        for (const auto& rule: ComputeConfig.GetComputeMapping().GetRule()) {
            const bool isMatched = AllOf(rule.GetKey(),
                [&task](const auto& key) {
                    switch (key.key_case()) {
                        case NConfig::TComputeMappingRuleKey::kQueryType:
                            return key.GetQueryType() == task.query_type();
                        case NConfig::TComputeMappingRuleKey::KEY_NOT_SET:
                            return false;
                    }
                });
            if (isMatched) {
                return rule.GetCompute();
            }
        }
        return NFq::NConfig::EComputeType::IN_PLACE;
    }
private:
    NFq::NConfig::TComputeConfig ComputeConfig;
};

} /* NFq */
