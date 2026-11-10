#pragma once

#include <ydb/core/protos/cdc_aggregator.pb.h>
#include <ydb/core/tx/cdc_aggregator/stream/activate_usecase.h>
#include <ydb/core/tx/cdc_aggregator/stream/configure_usecase.h>
#include <ydb/core/tx/cdc_aggregator/stream/replace_source_usecase.h>
#include <ydb/core/tx/cdc_aggregator/stream/report_delivered_usecase.h>

// Mapping between the wire protocol and the typed commands of the use cases.
// Parsing never fails: missing fields become invalid values that the use cases
// reject with BAD_REQUEST.

namespace NKikimr::NCdcAggregator {

TStreamConfig ParseStreamConfig(const NKikimrCdcAggregator::TStreamConfig& proto);
void FillStreamConfig(const TStreamConfig& config, NKikimrCdcAggregator::TStreamConfig& proto);

TConfigureCommand ParseConfigureCommand(const NKikimrCdcAggregator::TEvConfigure& proto);
TActivateCommand ParseActivateCommand(const NKikimrCdcAggregator::TEvActivate& proto);
TReportDeliveredCommand ParseReportDeliveredCommand(const NKikimrCdcAggregator::TEvReportDeliveredBatch& proto);
TReplaceSourceCommand ParseReplaceSourceCommand(const NKikimrCdcAggregator::TEvReplaceSource& proto);

void FillConfigureResult(const TConfigureResult& result, NKikimrCdcAggregator::TEvConfigureResult& proto);
void FillActivateResult(const TActivateResult& result, NKikimrCdcAggregator::TEvActivateResult& proto);
void FillReportDeliveredResult(const TReportDeliveredResult& result, NKikimrCdcAggregator::TEvReportDeliveredBatchResult& proto);
void FillReplaceSourceResult(const TReplaceSourceResult& result, NKikimrCdcAggregator::TEvReplaceSourceResult& proto);
void FillStreamDescription(const TStream& stream, NKikimrCdcAggregator::TEvDescribeStreamResult& proto);

} // namespace NKikimr::NCdcAggregator
