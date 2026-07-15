#include "columnshard_impl.h"
#include "columnshard_view.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <library/cpp/json/writer/json.h>
#include <util/datetime/base.h>

#include <limits>

namespace NKikimr::NColumnShard {

namespace {

bool HasPkSchema(const std::shared_ptr<arrow::Schema>& schema) {
    return schema && schema->num_fields() > 0;
}

bool HasPkRowSchema(const NArrow::TSimpleRow& row) {
    return HasPkSchema(row.GetSchema());
}

TString BinaryToHexKey(const char* data, size_t size) {
    static const char hexDigits[] = "0123456789abcdef";
    TString result;
    result.reserve(size * 2);
    for (size_t i = 0; i < size; ++i) {
        const unsigned char byte = static_cast<unsigned char>(data[i]);
        result.push_back(hexDigits[byte >> 4]);
        result.push_back(hexDigits[byte & 0x0f]);
    }
    return result;
}

std::optional<TString> BinaryScalarToHexKey(const std::shared_ptr<arrow::Buffer>& buffer) {
    if (!buffer) {
        return std::nullopt;
    }
    return BinaryToHexKey(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

void WriteJsonForHtmlScript(IOutputStream& out, const NJson::TJsonValue& value) {
    NJsonWriter::TBuf buf(NJsonWriter::HEM_DONT_ESCAPE_HTML, &out);
    buf.WriteJsonValue(&value, false);
}

bool UsesCompressedPkView(const arrow::Type::type typeId) {
    switch (typeId) {
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::UINT8:
        case arrow::Type::UINT16:
        case arrow::Type::UINT32:
        case arrow::Type::TIMESTAMP:
        case arrow::Type::DATE32:
        case arrow::Type::DATE64:
        case arrow::Type::TIME32:
        case arrow::Type::TIME64:
        case arrow::Type::DURATION:
        case arrow::Type::INTERVAL_MONTHS:
        case arrow::Type::INTERVAL_DAY_TIME:
            return false;
        default:
            return true;
    }
}

bool IsTemporalPkType(const arrow::Type::type typeId) {
    switch (typeId) {
        case arrow::Type::TIMESTAMP:
        case arrow::Type::DATE32:
        case arrow::Type::DATE64:
        case arrow::Type::TIME32:
        case arrow::Type::TIME64:
        case arrow::Type::DURATION:
        case arrow::Type::INTERVAL_MONTHS:
        case arrow::Type::INTERVAL_DAY_TIME:
            return true;
        default:
            return false;
    }
}

std::optional<double> GetPk0NumericValue(const NArrow::TSimpleRow& row) {
    if (!HasPkRowSchema(row)) {
        return std::nullopt;
    }
    const auto scalar = row.GetScalar(0);
    if (!scalar || !scalar->is_valid) {
        return std::nullopt;
    }
    switch (scalar->type->id()) {
        case arrow::Type::INT8:
            return static_cast<double>(static_cast<const arrow::Int8Scalar&>(*scalar).value);
        case arrow::Type::INT16:
            return static_cast<double>(static_cast<const arrow::Int16Scalar&>(*scalar).value);
        case arrow::Type::INT32:
            return static_cast<double>(static_cast<const arrow::Int32Scalar&>(*scalar).value);
        case arrow::Type::INT64:
            return static_cast<double>(static_cast<const arrow::Int64Scalar&>(*scalar).value);
        case arrow::Type::UINT8:
            return static_cast<double>(static_cast<const arrow::UInt8Scalar&>(*scalar).value);
        case arrow::Type::UINT16:
            return static_cast<double>(static_cast<const arrow::UInt16Scalar&>(*scalar).value);
        case arrow::Type::UINT32:
            return static_cast<double>(static_cast<const arrow::UInt32Scalar&>(*scalar).value);
        case arrow::Type::UINT64:
            return static_cast<double>(static_cast<const arrow::UInt64Scalar&>(*scalar).value);
        case arrow::Type::TIMESTAMP:
            return static_cast<double>(static_cast<const arrow::TimestampScalar&>(*scalar).value);
        case arrow::Type::DATE32:
            return static_cast<double>(static_cast<const arrow::Date32Scalar&>(*scalar).value);
        case arrow::Type::DATE64:
            return static_cast<double>(static_cast<const arrow::Date64Scalar&>(*scalar).value);
        case arrow::Type::TIME32:
            return static_cast<double>(static_cast<const arrow::Time32Scalar&>(*scalar).value);
        case arrow::Type::TIME64:
            return static_cast<double>(static_cast<const arrow::Time64Scalar&>(*scalar).value);
        case arrow::Type::DURATION:
            return static_cast<double>(static_cast<const arrow::DurationScalar&>(*scalar).value);
        case arrow::Type::INTERVAL_MONTHS:
            return static_cast<double>(static_cast<const arrow::MonthIntervalScalar&>(*scalar).value);
        case arrow::Type::INTERVAL_DAY_TIME: {
            const auto& interval = static_cast<const arrow::DayTimeIntervalScalar&>(*scalar).value;
            return static_cast<double>(interval.days) * 86400000.0 + interval.milliseconds;
        }
        default:
            return std::nullopt;
    }
}

TString GetFirstFieldFromDebugString(const TString& debug) {
    if (!debug) {
        return {};
    }
    TString first = debug;
    if (first.EndsWith(',')) {
        first.pop_back();
    }
    if (const auto comma = first.find(','); comma != TString::npos) {
        first = first.substr(0, comma);
    }
    if (first == "NULL") {
        return {};
    }
    return first;
}

TString GetPk0DisplayLabel(const NArrow::TSimpleRow& row) {
    if (!HasPkRowSchema(row)) {
        return {};
    }
    return GetFirstFieldFromDebugString(row.DebugString());
}

std::optional<TString> ScalarToCanonicalKey(const std::shared_ptr<arrow::Scalar>& scalar) {
    if (!scalar || !scalar->is_valid) {
        return std::nullopt;
    }
    switch (scalar->type->id()) {
        case arrow::Type::STRING: {
            const auto& value = static_cast<const arrow::StringScalar&>(*scalar).value;
            return value ? TString(value->ToString()) : TString{};
        }
        case arrow::Type::LARGE_STRING: {
            const auto& value = static_cast<const arrow::LargeStringScalar&>(*scalar).value;
            return value ? TString(value->ToString()) : TString{};
        }
        case arrow::Type::BINARY: {
            const auto& value = static_cast<const arrow::BinaryScalar&>(*scalar).value;
            return BinaryScalarToHexKey(value);
        }
        case arrow::Type::LARGE_BINARY: {
            const auto& value = static_cast<const arrow::LargeBinaryScalar&>(*scalar).value;
            return BinaryScalarToHexKey(value);
        }
        case arrow::Type::FIXED_SIZE_BINARY: {
            const auto& value = static_cast<const arrow::FixedSizeBinaryScalar&>(*scalar).value;
            return BinaryScalarToHexKey(value);
        }
        case arrow::Type::BOOL:
            return static_cast<const arrow::BooleanScalar&>(*scalar).value ? TString("true") : TString("false");
        case arrow::Type::INT8:
            return ToString(static_cast<const arrow::Int8Scalar&>(*scalar).value);
        case arrow::Type::INT16:
            return ToString(static_cast<const arrow::Int16Scalar&>(*scalar).value);
        case arrow::Type::INT32:
            return ToString(static_cast<const arrow::Int32Scalar&>(*scalar).value);
        case arrow::Type::INT64:
            return ToString(static_cast<const arrow::Int64Scalar&>(*scalar).value);
        case arrow::Type::UINT8:
            return ToString(static_cast<const arrow::UInt8Scalar&>(*scalar).value);
        case arrow::Type::UINT16:
            return ToString(static_cast<const arrow::UInt16Scalar&>(*scalar).value);
        case arrow::Type::UINT32:
            return ToString(static_cast<const arrow::UInt32Scalar&>(*scalar).value);
        case arrow::Type::UINT64:
            return ToString(static_cast<const arrow::UInt64Scalar&>(*scalar).value);
        case arrow::Type::FLOAT:
            return ToString(static_cast<const arrow::FloatScalar&>(*scalar).value);
        case arrow::Type::DOUBLE:
            return ToString(static_cast<const arrow::DoubleScalar&>(*scalar).value);
        case arrow::Type::TIMESTAMP:
            return ToString(static_cast<const arrow::TimestampScalar&>(*scalar).value);
        case arrow::Type::DATE32:
            return ToString(static_cast<const arrow::Date32Scalar&>(*scalar).value);
        case arrow::Type::DATE64:
            return ToString(static_cast<const arrow::Date64Scalar&>(*scalar).value);
        case arrow::Type::TIME32:
            return ToString(static_cast<const arrow::Time32Scalar&>(*scalar).value);
        case arrow::Type::TIME64:
            return ToString(static_cast<const arrow::Time64Scalar&>(*scalar).value);
        case arrow::Type::DURATION:
            return ToString(static_cast<const arrow::DurationScalar&>(*scalar).value);
        case arrow::Type::INTERVAL_MONTHS:
            return ToString(static_cast<const arrow::MonthIntervalScalar&>(*scalar).value);
        case arrow::Type::INTERVAL_DAY_TIME: {
            const auto& interval = static_cast<const arrow::DayTimeIntervalScalar&>(*scalar).value;
            return ToString(interval.days) + "d" + ToString(interval.milliseconds) + "ms";
        }
        default:
            return std::nullopt;
    }
}

std::optional<TString> GetPk0CanonicalKey(const NArrow::TSimpleRow& row) {
    if (HasPkRowSchema(row)) {
        if (auto key = ScalarToCanonicalKey(row.GetScalar(0))) {
            return key;
        }
    }

    const TString display = GetPk0DisplayLabel(row);
    if (!display) {
        return std::nullopt;
    }
    return display;
}

i64 LookupBoundaryIndex(const THashMap<TString, i64>& boundaryIndex, const std::optional<TString>& key) {
    if (!key) {
        return 0;
    }
    const auto it = boundaryIndex.find(*key);
    return it != boundaryIndex.end() ? it->second : 0;
}

void SerializePortionToJson(
    const NOlap::TPortionInfo& portion, const bool compressed, const THashMap<TString, i64>& boundaryIndex, NJson::TJsonValue& json)
{
    json.InsertValue("portionId", portion.GetPortionId());
    json.InsertValue("planStepMin", portion.RecordSnapshotMin().GetPlanStep());
    json.InsertValue("planStepMax", portion.RecordSnapshotMax().GetPlanStep());
    json.InsertValue("compactionLevel", portion.GetMeta().GetCompactionLevel());
    json.InsertValue("pkMinStr", GetPk0DisplayLabel(portion.IndexKeyStart()));
    json.InsertValue("pkMaxStr", GetPk0DisplayLabel(portion.IndexKeyEnd()));

    if (compressed) {
        const auto pkMin = GetPk0CanonicalKey(portion.IndexKeyStart());
        const auto pkMax = GetPk0CanonicalKey(portion.IndexKeyEnd());
        json.InsertValue("pkMin", LookupBoundaryIndex(boundaryIndex, pkMin));
        json.InsertValue("pkMax", LookupBoundaryIndex(boundaryIndex, pkMax));
    } else {
        json.InsertValue("pkMin", GetPk0NumericValue(portion.IndexKeyStart()).value_or(0.0));
        json.InsertValue("pkMax", GetPk0NumericValue(portion.IndexKeyEnd()).value_or(0.0));
    }
}

struct TPlanStepFilter {
    bool All = false;
    std::optional<ui64> FromMs;
    std::optional<ui64> ToMs;
};

constexpr ui64 DEFAULT_PLAN_STEP_WINDOW_MS = 3600 * 1000;
constexpr ui64 PLAN_STEP_TO_INF = std::numeric_limits<ui64>::max();

struct TPlanStepRange {
    ui64 From = 0;
    ui64 To = 0;
};

std::optional<ui64> ParsePlanStepMsParam(const TString& value) {
    if (!value) {
        return std::nullopt;
    }
    try {
        return std::stoull(value);
    } catch (...) {
        return std::nullopt;
    }
}

TPlanStepRange ComputeEffectivePlanStepRange(const std::vector<std::shared_ptr<NOlap::TPortionInfo>>& portions, const TPlanStepFilter& filter)
{
    if (portions.empty()) {
        return {};
    }

    ui64 minPlanStep = std::numeric_limits<ui64>::max();
    ui64 maxPlanStep = 0;
    for (const auto& portion : portions) {
        minPlanStep = std::min(minPlanStep, portion->RecordSnapshotMin().GetPlanStep());
        maxPlanStep = std::max(maxPlanStep, portion->RecordSnapshotMax().GetPlanStep());
    }

    if (filter.All) {
        return { minPlanStep, maxPlanStep };
    }

    TPlanStepRange range;
    if (filter.FromMs && filter.ToMs) {
        range.From = *filter.FromMs;
        range.To = *filter.ToMs;
        if (range.From > range.To) {
            std::swap(range.From, range.To);
        }
    } else if (filter.FromMs) {
        range.From = *filter.FromMs;
        range.To = PLAN_STEP_TO_INF;
    } else if (filter.ToMs) {
        range.From = minPlanStep;
        range.To = *filter.ToMs;
    } else {
        const ui64 nowMs = TInstant::Now().MilliSeconds();
        range.From = nowMs > DEFAULT_PLAN_STEP_WINDOW_MS ? nowMs - DEFAULT_PLAN_STEP_WINDOW_MS : 0;
        range.To = PLAN_STEP_TO_INF;
    }
    return range;
}

bool PortionOverlapsPlanStepWindow(const NOlap::TPortionInfo& portion, const ui64 windowStart, const ui64 windowEnd) {
    const ui64 planStepMin = portion.RecordSnapshotMin().GetPlanStep();
    const ui64 planStepMax = portion.RecordSnapshotMax().GetPlanStep();
    return planStepMax >= windowStart && planStepMin <= windowEnd;
}

void FilterPortionsByPlanStep(
    std::vector<std::shared_ptr<NOlap::TPortionInfo>>& portions, const TPlanStepFilter& filter, const TPlanStepRange& range)
{
    if (filter.All || portions.empty()) {
        return;
    }

    std::vector<std::shared_ptr<NOlap::TPortionInfo>> filtered;
    filtered.reserve(portions.size());
    for (const auto& portion : portions) {
        if (PortionOverlapsPlanStepWindow(*portion, range.From, range.To)) {
            filtered.emplace_back(portion);
        }
    }
    portions = std::move(filtered);
}

struct TPortionsLimitFilter {
    bool All = false;
    size_t Limit = 1000;
};

constexpr size_t DEFAULT_PORTIONS_LIMIT = 1000;

size_t ParsePortionsLimitParam(const TString& value) {
    if (!value) {
        return DEFAULT_PORTIONS_LIMIT;
    }
    try {
        const size_t limit = std::stoull(value);
        return limit > 0 ? limit : DEFAULT_PORTIONS_LIMIT;
    } catch (...) {
        return DEFAULT_PORTIONS_LIMIT;
    }
}

void LimitPortionsCount(std::vector<std::shared_ptr<NOlap::TPortionInfo>>& portions, const TPortionsLimitFilter& filter) {
    if (filter.All || portions.size() <= filter.Limit) {
        return;
    }

    Sort(portions, [](const auto& left, const auto& right) {
        const ui64 leftPlanStep = left->RecordSnapshotMax().GetPlanStep();
        const ui64 rightPlanStep = right->RecordSnapshotMax().GetPlanStep();
        if (leftPlanStep != rightPlanStep) {
            return leftPlanStep > rightPlanStep;
        }
        return left->GetPortionId() > right->GetPortionId();
    });
    portions.resize(filter.Limit);
}

NJson::TJsonValue CollectPortionsData(const NOlap::TColumnEngineForLogs& engine, const TInternalPathId pathId,
    const TPlanStepFilter& planStepFilter, const TPortionsLimitFilter& portionsLimitFilter)
{
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue("pathId", pathId.GetRawValue());

    const auto& granule = engine.GetGranuleVerified(pathId);

    const auto lastSchema = engine.GetVersionedIndex().GetLastSchema();
    if (!lastSchema) {
        result.InsertValue("error", "table schema is not available");
        return result;
    }
    const auto& pkSchema = lastSchema->GetIndexInfo().GetPrimaryKey();
    if (!HasPkSchema(pkSchema)) {
        result.InsertValue("error", "primary key schema is empty");
        return result;
    }

    std::vector<std::shared_ptr<NOlap::TPortionInfo>> activePortions;
    activePortions.reserve(granule.GetPortions().size());
    for (const auto& [_, portion] : granule.GetPortions()) {
        if (!portion->HasRemoveSnapshot()) {
            activePortions.emplace_back(portion);
        }
    }

    result.InsertValue("planStepAll", planStepFilter.All);
    result.InsertValue("portionsTotal", activePortions.size());

    const TPlanStepRange planStepRange = ComputeEffectivePlanStepRange(activePortions, planStepFilter);
    if (!activePortions.empty()) {
        result.InsertValue("planStepFrom", planStepRange.From);
        if (planStepRange.To == PLAN_STEP_TO_INF) {
            result.InsertValue("planStepToInf", true);
        } else {
            result.InsertValue("planStepTo", planStepRange.To);
        }
    }

    FilterPortionsByPlanStep(activePortions, planStepFilter, planStepRange);

    result.InsertValue("portionsLimitAll", portionsLimitFilter.All);
    result.InsertValue("portionsLimit", portionsLimitFilter.Limit);
    result.InsertValue("portionsAfterPlanStep", activePortions.size());

    LimitPortionsCount(activePortions, portionsLimitFilter);

    const auto pk0Field = pkSchema->field(0);
    const bool compressed = UsesCompressedPkView(pk0Field->type()->id());

    result.InsertValue("pk0ArrowType", pk0Field->type()->ToString());
    result.InsertValue("pk0Name", pk0Field->name());
    result.InsertValue("compressed", compressed);
    result.InsertValue("pk0Temporal", IsTemporalPkType(pk0Field->type()->id()));

    THashMap<TString, i64> boundaryIndex;
    if (compressed) {
        THashMap<TString, NArrow::TSimpleRow> boundaryRepresentatives;
        THashMap<TString, TString> boundaryDisplayLabels;
        for (const auto& portion : activePortions) {
            for (const auto& boundaryRow : { portion->IndexKeyStart(), portion->IndexKeyEnd() }) {
                if (const auto key = GetPk0CanonicalKey(boundaryRow)) {
                    boundaryRepresentatives.try_emplace(*key, boundaryRow);
                    const TString displayLabel = GetPk0DisplayLabel(boundaryRow);
                    boundaryDisplayLabels.try_emplace(*key, displayLabel ? displayLabel : *key);
                }
            }
        }
        TVector<TString> labels;
        labels.reserve(boundaryRepresentatives.size());
        for (const auto& [key, _] : boundaryRepresentatives) {
            labels.emplace_back(key);
        }
        Sort(labels, [&](const TString& left, const TString& right) {
            const auto& leftRow = boundaryRepresentatives.at(left);
            const auto& rightRow = boundaryRepresentatives.at(right);
            if (HasPkRowSchema(leftRow) && HasPkRowSchema(rightRow)) {
                return leftRow < rightRow;
            }
            return left < right;
        });
        NJson::TJsonValue labelsJson = NJson::JSON_ARRAY;
        for (size_t i = 0; i < labels.size(); ++i) {
            boundaryIndex.emplace(labels[i], (i64)i);
            labelsJson.AppendValue(boundaryDisplayLabels.at(labels[i]));
        }
        result.InsertValue("pkLabels", labelsJson);
    }

    NJson::TJsonValue portionsJson = NJson::JSON_ARRAY;
    ui64 maxCompactionLevel = 0;
    for (const auto& portion : activePortions) {
        NJson::TJsonValue portionJson = NJson::JSON_MAP;
        SerializePortionToJson(*portion, compressed, boundaryIndex, portionJson);
        portionsJson.AppendValue(portionJson);
        maxCompactionLevel = std::max(maxCompactionLevel, (ui64)portion->GetMeta().GetCompactionLevel());
    }
    result.InsertValue("portions", portionsJson);
    result.InsertValue("maxCompactionLevel", maxCompactionLevel);
    result.InsertValue("portionsCount", portionsJson.GetArray().size());
    return result;
}

}   // namespace

class TTxMonitoring: public TTransactionBase<TColumnShard> {
public:
    TTxMonitoring(TColumnShard* self, const NMon::TEvRemoteHttpInfo::TPtr& ev)
        : TBase(self)
        , HttpInfoEvent(ev)
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    //TTxType GetTxType() const override { return TXTYPE_INIT; }

private:
    NMon::TEvRemoteHttpInfo::TPtr HttpInfoEvent;
    NJson::TJsonValue JsonReport = NJson::JSON_MAP;
    TString RenderCompactionPage();
    TString RenderMainPage();
    TString RenderPortionsPage();
};

inline TString TEscapeHtml(const TString& in) {
    TString out;
    out.reserve(in.size());
    for (char c : in) {
        switch (c) {
            case '<':
                out += "&lt;";
                break;
            case '>':
                out += "&gt;";
                break;
            case '&':
                out += "&amp;";
                break;
            case '\'':
                out += "&#39;";
                break;
            case '\"':
                out += "&#34;";
                break;
            default:
                out += c;
                break;
        }
    }

    return out;
}

TString EscapeJsString(const TString& in) {
    TString out;
    out.reserve(in.size());
    for (char c : in) {
        switch (c) {
            case '\\':
                out += "\\\\";
                break;
            case '\'':
                out += "\\'";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            default:
                out += c;
                break;
        }
    }
    return out;
}

TString RenderLwTraceShardLogUrl(const TString& provider, const TString& probe, ui64 tabletId) {
    const TString traceId = TStringBuilder() << "." << provider << "." << probe << ".alsrt100000";
    return TStringBuilder() << "../trace?mode=log&id=" << traceId << "&f=tabletId=" << tabletId;
}

TString RenderLwTraceShardLinks(const TString& vizPage, const TString& provider, const TString& probe, ui64 tabletId, const TString& title)
{
    const TString tabletIdStr = ToString(tabletId);
    const TString traceUrl = RenderLwTraceShardLogUrl(provider, probe, tabletId);
    const TString safeTitle = TEscapeHtml(title);
    const TString safeVizPage = TEscapeHtml(vizPage);
    const TString safeTraceUrl = TEscapeHtml(traceUrl);
    TStringStream html;
    html << "<a href=\"app?page=" << safeVizPage << "&amp;TabletID=" << tabletIdStr << "\">" << safeTitle << "</a>";
    html << " | ";
    html << "<a href=\"" << safeTraceUrl << "\">" << safeTitle << " (text)</a>";
    return html.Str();
}

TString RenderScanTracesPage(ui64 tabletId) {
    const TString tabletIdStr = ToString(tabletId);
    const TString traceUrl = RenderLwTraceShardLogUrl("YDB_CS_SCAN", "StartScan", tabletId);
    TStringStream html;
    html << "<h3>Traces for all scans on shard</h3>";
    html << "<p>Trace source: <a href=\"" << TEscapeHtml(traceUrl) << "\">" << TEscapeHtml(traceUrl) << "</a></p>";
    html << "<p><a href=\"app?TabletID=" << tabletIdStr << "\">Back</a></p>";
    html << R"(<script language="javascript" type="text/javascript" src="../columnshard/plotly-2.35.2.min.js"></script>)";
    html << R"(<script language="javascript" type="text/javascript" src="../columnshard/scan-trace-viz.js"></script>)";
    html << "<div id=\"scan-trace-viz\" style=\"width:100%;\"></div>";
    html << "<script>renderScanTraceVisualization('" << EscapeJsString(traceUrl) << "', 'scan-trace-viz');</script>";
    return html.Str();
}

bool TTxMonitoring::Execute(TTransactionContext& txc, const TActorContext&) {
    return Self->TablesManager.FillMonitoringReport(txc, JsonReport["tables_manager"]);
}

template <typename T>
void TPrintErrorTable(TStringStream& html, THashMap<TString, std::queue<T>> errors, const std::string& errorType) {
    html << R"(
        <style>
            .error-table {
                border-collapse: collapse;
                width: 100%;
                margin-bottom: 1em;
                font-family: Arial, sans-serif;
            }
            .error-table th,
            .error-table td {
                border: 1px solid #ddd;
                padding: 8px;
                word-break: break-all;
                text-align: left;
                vertical-align: top;
            }
            .error-table th {
                background-color: #f2f2f2;
                font-weight: bold;
            }
            .error-table tr:nth-child(even) {
                background-color: #fafafa;
            }
            .error-table tr:hover {
                background-color: #f4f8ff;
            }
        </style>
    )";

    if (errors.empty()) {
        html << "No " << errorType << " errors<br />";
    } else {
        html << "<b>" << errorType << " errors:</b><br />";
        html << "<table class='error-table'>"
                "<tr><th>Tier</th><th>Time</th><th>Error</th></tr>";

        for (auto [tier, queue] : errors) {
            while (!queue.empty()) {
                const auto& element = queue.front();
                html << "<tr>"
                     << "<td>" << TEscapeHtml(tier) << "</td>"
                     << "<td>" << element.Time.ToString() << "</td>"
                     << "<td style=\"max-width:420px;\">" << TEscapeHtml(element.Reason) << "</td>"
                     << "</tr>";
                queue.pop();
            }
        }

        html << "</table><br />";
    }
}

TString TTxMonitoring::RenderMainPage() {
    const auto& cgi = HttpInfoEvent->Get()->Cgi();
    std::map<std::pair<ui64, ui64>, NJson::TJsonValue> schemaVersions;
    for (const auto& item : JsonReport["tables_manager"]["schema_versions"].GetArray()) {
        auto& schemaVersion = schemaVersions[std::make_pair<ui64, ui64>(item["SinceStep"].GetInteger(), item["SinceTxId"].GetInteger())];
        schemaVersion = item;
    }
    size_t countVersions = std::min(size_t(10), schemaVersions.size());
    if (const auto& countVersionsParam = cgi.Get("CountVersions")) {
        try {
            countVersions = std::min(size_t(std::stoul(countVersionsParam)), schemaVersions.size());
        } catch (...) {
            // nothing, use default value
        }
    }
    JsonReport["tables_manager"].EraseValue("schema_versions");
    TStringStream html;
    html << "<h3>Special Values</h3>";
    html << "<b>CurrentSchemeShardId:</b> " << Self->CurrentSchemeShardId << "<br />";
    html << "<b>ProcessingParams:</b> " << Self->ProcessingParams.value_or(NKikimrSubDomains::TProcessingParams{}).ShortDebugString()
         << "<br />";
    html << "<b>LastPlannedStep:</b> " << Self->LastPlannedStep << "<br />";
    html << "<b>LastPlannedTxId :</b> " << Self->LastPlannedTxId << "<br />";
    html << "<b>LastSchemaSeqNoGeneration :</b> " << Self->LastSchemaSeqNo.Generation << "<br />";
    html << "<b>LastSchemaSeqNoRound :</b> " << Self->LastSchemaSeqNo.Round << "<br />";
    html << "<b>LastExportNumber :</b> " << Self->LastExportNo << "<br />";
    if (const auto& tabletPathId = Self->TablesManager.GetTabletPathIdOptional()) {
        html << "<b>SchemeShardLocalPathId :</b> " << tabletPathId->SchemeShardLocalPathId << "<br />";
        html << "<b>InternalPathId :</b> " << tabletPathId->InternalPathId << "<br />";
    } else {
        html << "<b>SchemeShardLocalPathId :</b> " << "None" << "<br />";
        html << "<b>InternalPathId :</b> " << "None" << "<br />";
    }
    html << "<b>Table/Store Path :</b> " << Self->OwnerPath << "<br />";
    html << "<b>LastCompletedStep :</b> " << Self->LastCompletedTx.GetPlanStep() << "<br />";
    html << "<b>LastCompletedTxId :</b> " << Self->LastCompletedTx.GetTxId() << "<br />";
    html << "<b>LastNormalizerSequentialId :</b> " << Self->NormalizerController.GetLastSavedNormalizerId() << "<br />";
    html << "<b>SubDomainLocalPathId :</b> " << Self->SpaceWatcher->SubDomainPathId.value_or(0) << "<br />";
    html << "<b>SubDomainOutOfSpace :</b> " << Self->SpaceWatcher->SubDomainOutOfSpace << "<br />";
    html << "<b>SubDomainSmallBlobsQuotaExceeded :</b> " << Self->SpaceWatcher->SubDomainSmallBlobsQuotaExceeded << "<br />";
    html << "<h3>Tables Manager</h3>";
    html << "<h4>Status</h4>";
    html << "<pre>" << JsonReport << "</pre><br />";
    html << "<h4>Top " << countVersions << " of " << schemaVersions.size() << " Versions</h4>";
    size_t counter = 0;
    for (const auto& [_, schemaVersion] : schemaVersions) {
        html << counter;
        html << "<pre>" << schemaVersion << "</pre><br />";
        if (++counter == countVersions) {
            break;
        }
    }

    html << "<h3><a href=\"app?page=compaction&TabletID=" << cgi.Get("TabletID") << "\"> Compaction </a></h3>";
    html << "<h3><a href=\"app?page=scan&TabletID=" << cgi.Get("TabletID") << "\"> Scan </a></h3>";
    html << "<h3><a href=\"app?page=portions&TabletID=" << TEscapeHtml(cgi.Get("TabletID")) << "\"> Portions </a></h3>";
    html << "<h3>" << RenderLwTraceShardLinks("scan_traces", "YDB_CS_SCAN", "StartScan", Self->TabletID(), "Traces for all scans on shard")
         << "</h3>";
    html << "<h3><a href=\"" << TEscapeHtml(RenderLwTraceShardLogUrl("YDB_CS_DATA_SOURCE", "StartSourceProcessing", Self->TabletID()))
         << "\"> Traces for all portions on shard </a></h3>";

    html << "<h3>Tiering Errors</h3>";
    auto readErrors = Self->Counters.GetEvictionCounters().TieringErrors->GetAllReadErrors();
    auto writeErrors = Self->Counters.GetEvictionCounters().TieringErrors->GetAllWriteErrors();

    TPrintErrorTable(html, readErrors, "read");
    TPrintErrorTable(html, writeErrors, "write");

    return html.Str();
}

TString TTxMonitoring::RenderCompactionPage() {
    TStringStream html;
    const auto& cgi = HttpInfoEvent->Get()->Cgi();
    auto engine = Self->TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>();
    for (auto [tableId, _] : Self->TablesManager.GetTables()) {
        html << "<h3>TableId : " << tableId << "</h3>";
        auto& compaction = engine.GetGranuleVerified(tableId).GetOptimizerPlanner();
        auto json = compaction.SerializeToJsonVisual();
        html << "<pre>";
        NJson::WriteJson(&html, &json, true, true, true);
        html << "</pre>";
    }
    return html.Str();
}

TString TTxMonitoring::RenderPortionsPage() {
    const auto& cgi = HttpInfoEvent->Get()->Cgi();
    const TString tabletId = cgi.Get("TabletID");
    const bool jsonOnly = cgi.Has("format") && cgi.Get("format") == "json";

    auto engine = Self->TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>();
    const auto& tables = Self->TablesManager.GetTables();

    TInternalPathId selectedPathId;
    if (cgi.Has("pathId")) {
        try {
            selectedPathId = TInternalPathId::FromRawValue(std::stoull(cgi.Get("pathId")));
        } catch (...) {
            // use default
        }
    }
    if (!selectedPathId && tables.size() == 1) {
        selectedPathId = tables.begin()->first;
    }

    TPlanStepFilter planStepFilter;
    planStepFilter.All = cgi.Has("planStepAll");
    if (!planStepFilter.All) {
        planStepFilter.FromMs = ParsePlanStepMsParam(cgi.Get("planStepFrom"));
        planStepFilter.ToMs = ParsePlanStepMsParam(cgi.Get("planStepTo"));
    }

    TPortionsLimitFilter portionsLimitFilter;
    portionsLimitFilter.All = cgi.Has("portionsLimitAll");
    if (!portionsLimitFilter.All) {
        portionsLimitFilter.Limit = ParsePortionsLimitParam(cgi.Get("portionsLimit"));
    }

    NJson::TJsonValue data = NJson::JSON_MAP;
    if (selectedPathId && tables.find(selectedPathId) != tables.end()) {
        data = CollectPortionsData(engine, selectedPathId, planStepFilter, portionsLimitFilter);
    }

    if (jsonOnly) {
        TStringStream json;
        NJson::WriteJson(&json, &data, true, true, true);
        return json.Str();
    }

    TStringStream html;
    html << R"(
        <style>
            .portions-form label { margin-right: 0.5em; }
            .portions-form input, .portions-form select { margin-right: 1em; }
            .portions-form input.inactive { opacity: 0.5; }
            .portions-form-row { margin-bottom: 0.6em; }
            .portions-canvas { border: 1px solid #ccc; max-width: 100%; margin-bottom: 1em; }
            .portions-error { color: #a00; }
            .portions-info { color: #444; margin-bottom: 0.5em; }
        </style>
        <h3>Portions visualization</h3>
        <p>Visualize all portions for the selected table.</p>
        <form class="portions-form" method="get" action="app">
            <input type="hidden" name="page" value="portions" />
            <input type="hidden" name="TabletID" value=")"
         << TEscapeHtml(tabletId) << R"(" />
            <div class="portions-form-row">
                <label>Table pathId:</label>
                <select name="pathId">)";
    for (const auto& [pathId, _] : tables) {
        html << "<option value=\"" << pathId.GetRawValue() << "\"";
        if (pathId == selectedPathId) {
            html << " selected";
        }
        html << ">" << pathId.GetRawValue() << "</option>";
    }
    html << R"(</select>
            </div>
            <div class="portions-form-row">
                <label>Plan step from:</label>
                <input type="datetime-local" id="planStepFromLocal" />
                <input type="hidden" name="planStepFrom" id="planStepFrom" />
                <label>to:</label>
                <input type="datetime-local" id="planStepToLocal" title="empty = infinity" />
                <input type="hidden" name="planStepTo" id="planStepTo" />
                <label><input type="checkbox" name="planStepAll" id="planStepAll" value="1")";
    if (planStepFilter.All) {
        html << " checked";
    }
    html << R"( /> All range</label>
            </div>
            <div class="portions-form-row">
                <label>Portion limit:</label>
                <input type="number" name="portionsLimit" id="portionsLimit" min="1" value=")"
         << portionsLimitFilter.Limit << R"(" />
                <label><input type="checkbox" name="portionsLimitAll" id="portionsLimitAll" value="1")";
    if (portionsLimitFilter.All) {
        html << " checked";
    }
    html << R"( /> No portion limit</label>
            </div>
            <div class="portions-form-row">
                <button type="submit">Visualize</button>
            </div>
        </form>
        <script>
        (function() {
            const allRange = document.getElementById('planStepAll');
            const fromLocal = document.getElementById('planStepFromLocal');
            const toLocal = document.getElementById('planStepToLocal');
            const fromHidden = document.getElementById('planStepFrom');
            const toHidden = document.getElementById('planStepTo');
            const form = allRange.closest('form');
            const portionsLimitAll = document.getElementById('portionsLimitAll');
            const portionsLimit = document.getElementById('portionsLimit');

            function msToDatetimeLocal(ms) {
                const d = new Date(ms);
                const pad = (n) => String(n).padStart(2, '0');
                return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate())
                    + 'T' + pad(d.getHours()) + ':' + pad(d.getMinutes());
            }

            function datetimeLocalToMs(value) {
                if (!value) {
                    return '';
                }
                return String(new Date(value).getTime());
            }

            function syncPlanStepRangeState() {
                const inactive = allRange.checked;
                fromLocal.classList.toggle('inactive', inactive);
                toLocal.classList.toggle('inactive', inactive);
            }

            function syncPortionsLimitState() {
                portionsLimit.classList.toggle('inactive', portionsLimitAll.checked);
            }

            allRange.addEventListener('change', syncPlanStepRangeState);
            portionsLimitAll.addEventListener('change', syncPortionsLimitState);
            form.addEventListener('submit', function() {
                if (allRange.checked) {
                    fromHidden.value = '';
                    toHidden.value = '';
                    return;
                }
                fromHidden.value = datetimeLocalToMs(fromLocal.value);
                toHidden.value = datetimeLocalToMs(toLocal.value);
            });

            syncPlanStepRangeState();
            syncPortionsLimitState();
            window.initPlanStepRangeInputs = function(fromMs, toMs, toInf) {
                if (fromMs !== undefined && fromMs !== null) {
                    fromLocal.value = msToDatetimeLocal(fromMs);
                    fromHidden.value = String(fromMs);
                }
                if (toInf) {
                    toLocal.value = '';
                    toHidden.value = '';
                } else if (toMs !== undefined && toMs !== null) {
                    toLocal.value = msToDatetimeLocal(toMs);
                    toHidden.value = String(toMs);
                }
            };
        })();
        </script>
        <div id="portions-status" class="portions-info"></div>
        <h4>Column table portions</h4>
        <canvas id="portions-chart" class="portions-canvas" width="900" height="400"></canvas>
        <h4>Portion intersections</h4>
        <canvas id="portions-intersections" class="portions-canvas" width="900" height="200"></canvas>
        <script type="application/json" id="portions-data">)";
    WriteJsonForHtmlScript(html, data);
    html << R"(</script>
        <script>
        (function() {
            const data = JSON.parse(document.getElementById('portions-data').textContent);

            function levelColor(level, maxLevel) {
                if (level === 0) return 'rgba(255, 0, 0, 0.5)';
                if (level === maxLevel) return 'rgba(0, 180, 0, 0.5)';
                return 'rgba(0, 0, 255, 0.5)';
            }

            function pkToX(pk, xMin, xMax, width, padding) {
                return padding + (pk - xMin) / (xMax - xMin) * width;
            }

            function planStepToY(planStep, yMin, yMax, height, padding) {
                return padding + height - (planStep - yMin) / (yMax - yMin) * height;
            }

            function formatPk(pk, portions, pkLabels) {
                if (pkLabels && pkLabels.length) {
                    const idx = Math.round(pk);
                    if (idx >= 0 && idx < pkLabels.length) {
                        return pkLabels[idx];
                    }
                }
                if (portions && portions.length) {
                    for (const p of portions) {
                        if (p.pkMin === pk && p.pkMinStr) {
                            return p.pkMinStr;
                        }
                        if (p.pkMax === pk && p.pkMaxStr) {
                            return p.pkMaxStr;
                        }
                    }
                }
                return String(pk);
            }

            function formatPlanStep(planStep) {
                return new Date(planStep).toISOString();
            }

            function* generateIntersectionData(ranges) {
                const points = [];
                for (const [minPk, maxPk] of ranges) {
                    points.push([minPk, 1]);
                    points.push([maxPk, -1]);
                }
                if (!points.length) {
                    return;
                }
                points.sort((a, b) => a[0] !== b[0] ? a[0] - b[0] : b[1] - a[1]);
                let cur = points[0][1];
                let prevPk = points[0][0];
                let prevClosed = false;
                let lastUsedPk = prevPk;
                for (let i = 1; i < points.length; ++i) {
                    const [pk, delta] = points[i];
                    if (pk !== prevPk) {
                        if (cur > 1) {
                            yield [lastUsedPk, pk - lastUsedPk, cur - 1];
                        }
                        lastUsedPk = pk;
                    } else if (delta < 0 && !prevClosed) {
                        lastUsedPk = pk;
                        if (Number.isInteger(lastUsedPk)) {
                            lastUsedPk += 1;
                        }
                        if (cur > 1) {
                            yield [prevPk, lastUsedPk - prevPk, cur - 1];
                        }
                    }
                    cur += delta;
                    prevPk = pk;
                    prevClosed = delta < 0;
                }
            }

            function drawIntersections(canvas, portions, pkLabels) {
                const ctx = canvas.getContext('2d');
                const padding = 50;
                const width = canvas.width - 2 * padding;
                const height = canvas.height - 2 * padding;
                ctx.clearRect(0, 0, canvas.width, canvas.height);

                if (!portions.length) {
                    return;
                }

                const ranges = portions.map(p => [p.pkMin, p.pkMax]);
                const intersections = [...generateIntersectionData(ranges)];
                let maxIntersections = 0;
                for (const [_, __, n] of intersections) {
                    maxIntersections = Math.max(maxIntersections, n);
                }

                let pkMin = portions[0].pkMin;
                let pkMax = portions[0].pkMax;
                for (const p of portions) {
                    pkMin = Math.min(pkMin, p.pkMin);
                    pkMax = Math.max(pkMax, p.pkMax);
                }
                const pkDx = pkMax - pkMin || 1;
                pkMin -= 0.05 * pkDx;
                pkMax += 0.05 * pkDx;

                ctx.strokeStyle = '#ddd';
                ctx.strokeRect(padding, padding, width, height);

                if (!intersections.length || maxIntersections === 0) {
                    ctx.fillStyle = '#666';
                    ctx.fillText('No overlapping PK ranges', padding, padding + 20);
                    return;
                }

                const yMax = maxIntersections * 1.1 + 1;
                ctx.fillStyle = '#666';
                ctx.font = '11px sans-serif';
                for (let tick = 1; tick <= maxIntersections; ++tick) {
                    const y = padding + height - (tick / yMax) * height;
                    ctx.strokeStyle = '#eee';
                    ctx.beginPath();
                    ctx.moveTo(padding, y);
                    ctx.lineTo(padding + width, y);
                    ctx.stroke();
                    ctx.fillStyle = '#666';
                    ctx.fillText(String(tick), 8, y + 4);
                }

                for (const [pk, w, n] of intersections) {
                    const x = pkToX(pk, pkMin, pkMax, width, padding);
                    const barW = Math.max(1, pkToX(pk + w, pkMin, pkMax, width, padding) - x);
                    const barH = (n / yMax) * height;
                    const barTop = padding + height - barH;
                    ctx.fillStyle = 'rgba(100, 100, 200, 0.6)';
                    ctx.strokeStyle = 'black';
                    ctx.setLineDash([4, 2]);
                    ctx.fillRect(x, barTop, barW, barH);
                    ctx.strokeRect(x, barTop, barW, barH);
                    ctx.setLineDash([]);
                    if (barW >= 14) {
                        ctx.fillStyle = '#111';
                        ctx.fillText(String(n), x + barW / 2 - 4, barTop + barH / 2 + 4);
                    }
                }

                ctx.fillStyle = '#333';
                ctx.font = '12px sans-serif';
                ctx.save();
                ctx.translate(15, padding + height / 2);
                ctx.rotate(-Math.PI / 2);
                ctx.fillText('intersection count', 0, 0);
                ctx.restore();
                const actualPkMin = Math.min(...portions.map(p => p.pkMin));
                const actualPkMax = Math.max(...portions.map(p => p.pkMax));
                ctx.fillStyle = '#666';
                ctx.font = '11px sans-serif';
                ctx.fillText(formatPk(actualPkMin, portions, pkLabels), padding, padding + height + 20);
                const rightLabel = formatPk(actualPkMax, portions, pkLabels);
                ctx.fillText(rightLabel, padding + width - Math.min(200, rightLabel.length * 6), padding + height + 20);
            }

            function drawPortions(canvas, portions, maxLevel, pkLabels, compressed) {
                const ctx = canvas.getContext('2d');
                const padding = 50;
                const width = canvas.width - 2 * padding;
                const height = canvas.height - 2 * padding;
                ctx.clearRect(0, 0, canvas.width, canvas.height);

                if (!portions.length) {
                    ctx.fillStyle = '#666';
                    ctx.fillText('No portions', padding, padding + 20);
                    return;
                }

                let pkMin = portions[0].pkMin;
                let pkMax = portions[0].pkMax;
                let planMin = portions[0].planStepMin;
                let planMax = portions[0].planStepMax;
                for (const p of portions) {
                    pkMin = Math.min(pkMin, p.pkMin);
                    pkMax = Math.max(pkMax, p.pkMax);
                    planMin = Math.min(planMin, p.planStepMin);
                    planMax = Math.max(planMax, p.planStepMax);
                }
                const pkDx = pkMax - pkMin || 1;
                const planDy = planMax - planMin || 1;
                pkMin -= 0.05 * pkDx;
                pkMax += 0.05 * pkDx;
                planMin -= 0.05 * planDy;
                planMax += 0.05 * planDy;

                ctx.strokeStyle = '#ddd';
                ctx.strokeRect(padding, padding, width, height);
                ctx.fillStyle = '#333';
                const pkViewSuffix = compressed ? ', compressed' : '';
                const pkAxisLabel = 'pk (' + (data.pk0Name || '') + ', ' + (data.pk0ArrowType || '') + pkViewSuffix + ')';
                ctx.fillText(pkAxisLabel, padding, padding - 10);
                ctx.save();
                ctx.translate(15, padding + height / 2);
                ctx.rotate(-Math.PI / 2);
                ctx.fillText('plan_step', 0, 0);
                ctx.restore();

                for (const p of portions) {
                    const x = pkToX(p.pkMin, pkMin, pkMax, width, padding);
                    const w = Math.max(1, pkToX(p.pkMax, pkMin, pkMax, width, padding) - x);
                    const yTop = planStepToY(p.planStepMax, planMin, planMax, height, padding);
                    const yBottom = planStepToY(p.planStepMin, planMin, planMax, height, padding);
                    const h = Math.max(1, yBottom - yTop);
                    ctx.fillStyle = levelColor(p.compactionLevel, maxLevel);
                    ctx.strokeStyle = p.compactionLevel === 0 ? 'red' : 'black';
                    ctx.setLineDash([4, 2]);
                    ctx.fillRect(x, yTop, w, h);
                    ctx.strokeRect(x, yTop, w, h);
                    ctx.setLineDash([]);
                }

                ctx.fillStyle = '#666';
                ctx.font = '11px sans-serif';
                const actualPkMin = Math.min(...portions.map(p => p.pkMin));
                const actualPkMax = Math.max(...portions.map(p => p.pkMax));
                ctx.fillText(formatPk(actualPkMin, portions, pkLabels), padding, padding + height + 20);
                const rightLabel = formatPk(actualPkMax, portions, pkLabels);
                ctx.fillText(rightLabel, padding + width - Math.min(200, rightLabel.length * 6), padding + height + 20);
                ctx.fillText(formatPlanStep(planMin), 5, padding + height);
                ctx.fillText(formatPlanStep(planMax), 5, padding + 10);
                ctx.font = '12px sans-serif';
            }

            const status = document.getElementById('portions-status');
            if (data.error) {
                status.className = 'portions-error';
                status.textContent = data.error;
            } else if (data.portionsCount !== undefined) {
                status.className = 'portions-info';
                let msg = data.portionsCount + ' portions';
                if (!data.portionsLimitAll && data.portionsAfterPlanStep !== undefined
                    && data.portionsAfterPlanStep > data.portionsCount) {
                    msg += ' (limited from ' + data.portionsAfterPlanStep + ', max ' + data.portionsLimit + ')';
                } else if (data.portionsTotal !== undefined && data.portionsTotal !== data.portionsCount
                    && (data.portionsLimitAll || data.portionsAfterPlanStep === data.portionsCount)) {
                    msg += ' (filtered from ' + data.portionsTotal + ')';
                }
                if (!data.planStepAll && data.planStepFrom !== undefined) {
                    msg += ', plan_step: '
                        + formatPlanStep(data.planStepFrom)
                        + ' .. '
                        + (data.planStepToInf ? 'inf' : formatPlanStep(data.planStepTo));
                } else if (data.planStepAll) {
                    msg += ', plan_step: all range';
                }
                if (data.compressed && data.pkLabels) {
                    msg += ' (compressed ordinal view, ' + data.pkLabels.length + ' boundaries, type: '
                        + (data.pk0ArrowType || 'unknown') + ')';
                } else if (data.pk0ArrowType) {
                    msg += ' (native PK view, type: ' + data.pk0ArrowType + ')';
                }
                status.textContent = msg;
            }

            const portions = data.portions || [];
            const pkLabels = data.pkLabels || [];
            const compressed = !!data.compressed;
            if (window.initPlanStepRangeInputs && data.planStepFrom !== undefined) {
                window.initPlanStepRangeInputs(data.planStepFrom, data.planStepTo, data.planStepToInf);
            }
            drawPortions(
                document.getElementById('portions-chart'),
                portions,
                data.maxCompactionLevel || 0,
                pkLabels,
                compressed
            );
            drawIntersections(
                document.getElementById('portions-intersections'),
                portions,
                pkLabels
            );
        })();
        </script>)";

    return html.Str();
}

void TTxMonitoring::Complete(const TActorContext& ctx) {
    auto cgi = HttpInfoEvent->Get()->Cgi();
    auto path = HttpInfoEvent->Get()->PathInfo();
    TString htmlResult;

    if (cgi.Has("page") && cgi.Get("page") == "compaction") {
        htmlResult = RenderCompactionPage();
    } else if (cgi.Has("page") && cgi.Get("page") == "portions") {
        htmlResult = RenderPortionsPage();
    } else {
        htmlResult = RenderMainPage();
    }
    ctx.Send(HttpInfoEvent->Sender, new NMon::TEvRemoteHttpInfoRes(htmlResult));
}

bool TColumnShard::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) {
    if (!Executor() || !Executor()->GetStats().IsActive) {
        return false;
    }

    if (!ev) {
        return true;
    }

    auto cgi = ev->Get()->Cgi();
    if (cgi.Has("page") && cgi.Get("page") == "scan") {
        Send(ev->Forward(ScanDiagnosticsActorId));
        return true;
    }

    if (cgi.Has("page") && cgi.Get("page") == "scan_traces") {
        ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(RenderScanTracesPage(TabletID())));
        return true;
    }

    Execute(new TTxMonitoring(this, ev), ctx);
    return true;
}

}   // namespace NKikimr::NColumnShard
