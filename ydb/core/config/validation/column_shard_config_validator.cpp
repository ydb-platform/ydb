#include "validators.h"

#include <ydb/core/formats/arrow/serializer/parsing.h>
#include <ydb/core/formats/arrow/serializer/utils.h>
#include <ydb/core/protos/config.pb.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <vector>

namespace NKikimr::NConfig {
namespace {

EValidationResult ValidateDefaultCompression(const NKikimrConfig::TColumnShardConfig& columnShardConfig, std::vector<TString>& msg) {
    if (!columnShardConfig.HasDefaultCompression() && !columnShardConfig.HasDefaultCompressionLevel()) {
        return EValidationResult::Ok;
    }
    if (!columnShardConfig.HasDefaultCompression() && columnShardConfig.HasDefaultCompressionLevel()) {
        msg.push_back("ColumnShardConfig: compression level is set without compression type");
        return EValidationResult::Error;
    }
    std::optional<arrow::Compression::type> codec = NArrow::CompressionFromProto(columnShardConfig.GetDefaultCompression());
    if (!codec.has_value()) {
        msg.push_back("ColumnShardConfig: Unknown compression");
        return EValidationResult::Error;
    }
    if (columnShardConfig.HasDefaultCompressionLevel()) {
        if (!NArrow::SupportsCompressionLevel(codec.value())) {
            TString messageErr = TStringBuilder() << "ColumnShardConfig: compression `" << NArrow::CompressionToString(codec.value())
                                                  << "` does not support compression level";
            msg.push_back(messageErr);
            return EValidationResult::Error;
        } else if (!NArrow::SupportsCompressionLevel(codec.value(), columnShardConfig.GetDefaultCompressionLevel())) {
            TString messageErr = TStringBuilder()
                                 << "ColumnShardConfig: compression `" << NArrow::CompressionToString(codec.value())
                                 << "` does not support compression level = " << std::to_string(columnShardConfig.GetDefaultCompressionLevel());
            msg.push_back(messageErr);
            return EValidationResult::Error;
        }
    }
    return EValidationResult::Ok;
}

}  // namespace

EValidationResult ValidateColumnShardConfig(const NKikimrConfig::TColumnShardConfig& columnShardConfig, std::vector<TString>& msg) {
    EValidationResult validateDefaultCompressionResult = ValidateDefaultCompression(columnShardConfig, msg);
    if (validateDefaultCompressionResult == EValidationResult::Error) {
        return EValidationResult::Error;
    }
    return EValidationResult::Ok;
}

}  // namespace NKikimr::NConfig
