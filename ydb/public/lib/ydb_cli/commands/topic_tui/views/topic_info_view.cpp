#include "topic_info_view.h"
#include "../app_interface.h"
#include "../widgets/sparkline.h"
#include "../common/async_utils.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <util/string/printf.h>
#include <util/string/builder.h>
#include <algorithm>
#include <functional>

using namespace ftxui;

namespace NYdb::NConsoleClient {

namespace {

TString FormatInstant(TInstant t) {
    if (t == TInstant::Zero()) {
        return "-";
    }
    auto ts = t.Seconds();
    time_t time = ts;
    struct tm* tm = localtime(&time);
    if (!tm) {
        return "-";
    }
    return Sprintf("%02d:%02d:%02d", tm->tm_hour, tm->tm_min, tm->tm_sec);
}

TString FormatTimestampMs(ui64 tsMs) {
    if (tsMs == 0) {
        return "-";
    }
    TInstant t = TInstant::MilliSeconds(tsMs);
    return t.FormatLocalTime("%Y-%m-%d %H:%M:%S");
}

TString FormatBool(bool value) {
    return value ? "true" : "false";
}

TString FormatSeconds(ui64 seconds) {
    return FormatDuration(TDuration::Seconds(seconds));
}

TString FormatJsonLeaf(const NJson::TJsonValue& value) {
    return NJson::WriteJson(value, false);
}

} // anonymous namespace

TTopicInfoView::TTopicInfoView(ITuiApp& app)
    : App_(app)
    , Table_({
        TTableColumn{"Partition", 10},
        TTableColumn{"StartOffset", 15},
        TTableColumn{"EndOffset", 15},
        TTableColumn{"Size", 12},
        TTableColumn{"WriteLag", 12},
        TTableColumn{"LastWrite", 12}
    })
{
}

TTopicInfoView::~TTopicInfoView() {
    if (StopFlag_) {
        *StopFlag_ = true;
    }
}

Component TTopicInfoView::Build() {
    auto renderer = Renderer([this] {
        CheckAsyncCompletion();
        return RenderContent();
    });
    
    return CatchEvent(renderer, [this](Event event) {
        return HandleJsonTreeEvent(event);
    });
}

void TTopicInfoView::SetTopic(const TString& topicPath) {
    TopicPath_ = topicPath;
    HasData_ = false;
    ErrorMessage_.clear();
    ViewerData_ = {};
    HasViewerData_ = false;
    JsonTreeError_.clear();
    JsonTreeReady_ = false;
    JsonRoot_.reset();
    JsonVisibleNodes_.clear();
    JsonSelected_ = 0;
    StartAsyncLoad();
    StartViewerLoad();
}

void TTopicInfoView::Refresh() {
    StartAsyncLoad();
    StartViewerLoad();
}

void TTopicInfoView::StartAsyncLoad() {
    if (Loading_ || TopicPath_.empty()) {
        return;
    }
    
    Loading_ = true;
    auto* topicClient = &App_.GetTopicClient();
    TString topicPath = TopicPath_;
    auto stopFlag = StopFlag_;
    
    DescribeFuture_ = std::async(std::launch::async, [topicClient, topicPath, stopFlag]() {
        NTopic::TDescribeTopicSettings settings;
        settings.IncludeStats(true);
        
        auto future = topicClient->DescribeTopic(std::string(topicPath.c_str()), settings);
        
        // Wait with timeout and cancellation check
        if (!WaitFor(future, stopFlag, TDuration::Seconds(5))) {
            throw std::runtime_error("Request timed out or cancelled");
        }
        
        return future.GetValueSync();
    });
}

void TTopicInfoView::StartViewerLoad() {
    if (ViewerLoading_ || TopicPath_.empty()) {
        return;
    }
    
    const TString& endpoint = App_.GetViewerEndpoint();
    if (endpoint.empty()) {
        ViewerData_ = {};
        ViewerData_.Error = "Viewer endpoint not configured";
        HasViewerData_ = false;
        ViewerLoading_ = false;
        return;
    }
    
    ViewerLoading_ = true;
    ViewerData_.Error.clear();
    TString topicPath = TopicPath_;
    
    ViewerDescribeFuture_ = std::async(std::launch::async, [endpoint, topicPath]() {
        TViewerHttpClient client(endpoint);
        return client.GetTopicDescribe(topicPath, false, true, true, false);
    });
}

void TTopicInfoView::CheckAsyncCompletion() {
    if (DescribeFuture_.valid() && 
        DescribeFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
        
        auto result = DescribeFuture_.get();
        Loading_ = false;
        
        if (result.IsSuccess()) {
            const auto& desc = result.GetTopicDescription();
            
            // Copy data to our struct
            Data_.PartitionCount = desc.GetPartitions().size();
            Data_.RetentionPeriod = desc.GetRetentionPeriod();
            Data_.RetentionStorageMb = desc.GetRetentionStorageMb().value_or(0);
            
            Data_.ConsumerNames.clear();
            for (const auto& c : desc.GetConsumers()) {
                Data_.ConsumerNames.push_back(TString(c.GetConsumerName()));
            }
            
            Data_.Codecs.clear();
            for (const auto& codec : desc.GetSupportedCodecs()) {
                TString codecName;
                switch (codec) {
                    case NTopic::ECodec::RAW: codecName = "RAW"; break;
                    case NTopic::ECodec::GZIP: codecName = "GZIP"; break;
                    case NTopic::ECodec::ZSTD: codecName = "ZSTD"; break;
                    case NTopic::ECodec::LZOP: codecName = "LZOP"; break;
                    default: codecName = "CUSTOM"; break;
                }
                Data_.Codecs.push_back(codecName);
            }
            
            Data_.Partitions.clear();
            for (const auto& partition : desc.GetPartitions()) {
                TTopicInfoData::TPartitionInfo info;
                info.PartitionId = partition.GetPartitionId();
                if (partition.GetPartitionStats()) {
                    const auto& stats = *partition.GetPartitionStats();
                    info.StartOffset = stats.GetStartOffset();
                    info.EndOffset = stats.GetEndOffset();
                    info.StoreSizeBytes = stats.GetStoreSizeBytes();
                    info.WriteLag = stats.GetMaxWriteTimeLag();
                    info.LastWriteTime = stats.GetLastWriteTime();
                }
                Data_.Partitions.push_back(info);
            }
            
            HasData_ = true;
            ErrorMessage_.clear();
            
            // Update table
            Table_.Clear();
            Table_.SetRowCount(Data_.Partitions.size());
            
            for (size_t i = 0; i < Data_.Partitions.size(); ++i) {
                const auto& p = Data_.Partitions[i];
                TVector<TString> row;
                row.push_back(Sprintf("%lu", p.PartitionId));
                row.push_back(Sprintf("%lu", p.StartOffset));
                row.push_back(Sprintf("%lu", p.EndOffset));
                row.push_back(FormatBytes(p.StoreSizeBytes));
                row.push_back(FormatDuration(p.WriteLag));
                row.push_back(FormatInstant(p.LastWriteTime));
                Table_.SetRow(i, row);
            }
        } else {
            ErrorMessage_ = result.GetIssues().ToString();
            HasData_ = false;
        }
    }
    
    if (ViewerLoading_ && ViewerDescribeFuture_.valid() &&
        ViewerDescribeFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
        try {
            ViewerData_ = ViewerDescribeFuture_.get();
            HasViewerData_ = ViewerData_.Error.empty();
        } catch (const std::exception& e) {
            ViewerData_ = {};
            ViewerData_.Error = e.what();
            HasViewerData_ = false;
        }
        
        JsonTreeReady_ = false;
        JsonTreeError_.clear();
        ViewerJson_ = NJson::TJsonValue();
        JsonRoot_.reset();
        JsonVisibleNodes_.clear();
        JsonSelected_ = 0;
        if (HasViewerData_ && !ViewerData_.RawJson.empty()) {
            NJson::TJsonReaderConfig config;
            config.DontValidateUtf8 = true;
            if (NJson::ReadJsonTree(ViewerData_.RawJson, &config, &ViewerJson_, false)) {
                BuildJsonTree();
            } else {
                JsonTreeError_ = "Failed to parse Viewer JSON";
            }
        }
        ViewerLoading_ = false;
    }
}

std::unique_ptr<TTopicInfoView::TJsonTreeNode> TTopicInfoView::MakeJsonNode(
    const TString& name,
    const NJson::TJsonValue& value,
    TJsonTreeNode* parent)
{
    auto node = std::make_unique<TJsonTreeNode>();
    node->Name = name;
    node->Value = &value;
    node->Parent = parent;
    
    if (value.IsMap()) {
        const auto& map = value.GetMap();
        for (const auto& item : map) {
            node->Children.push_back(MakeJsonNode(item.first, item.second, node.get()));
        }
    } else if (value.IsArray()) {
        const auto& array = value.GetArray();
        for (size_t i = 0; i < array.size(); ++i) {
            TString childName = TStringBuilder() << "[" << i << "]";
            node->Children.push_back(MakeJsonNode(childName, array[i], node.get()));
        }
    }
    
    return node;
}

void TTopicInfoView::BuildJsonTree() {
    JsonRoot_.reset();
    JsonVisibleNodes_.clear();
    JsonSelected_ = 0;
    JsonTreeReady_ = false;
    
    if (!ViewerJson_.IsDefined()) {
        JsonTreeError_ = "Viewer JSON is empty";
        return;
    }
    
    JsonRoot_ = MakeJsonNode("Viewer JSON", ViewerJson_, nullptr);
    if (JsonRoot_) {
        JsonRoot_->Open = true;
        BuildJsonVisible(JsonRoot_.get());
        JsonTreeReady_ = true;
    }
}

void TTopicInfoView::BuildJsonVisible(TJsonTreeNode* preferSelected) {
    TJsonTreeNode* target = preferSelected;
    if (!target && !JsonVisibleNodes_.empty()) {
        target = JsonVisibleNodes_[JsonSelected_].Node;
    }
    
    JsonVisibleNodes_.clear();
    if (JsonRoot_) {
        CollectVisibleNodes(JsonRoot_.get(), 0);
    }
    
    if (JsonVisibleNodes_.empty()) {
        JsonSelected_ = 0;
        return;
    }
    
    if (target) {
        for (size_t i = 0; i < JsonVisibleNodes_.size(); ++i) {
            if (JsonVisibleNodes_[i].Node == target) {
                JsonSelected_ = static_cast<int>(i);
                return;
            }
        }
    }
    
    JsonSelected_ = std::min<int>(JsonSelected_, static_cast<int>(JsonVisibleNodes_.size()) - 1);
}

void TTopicInfoView::CollectVisibleNodes(TJsonTreeNode* node, int depth) {
    JsonVisibleNodes_.push_back({node, depth});
    if (!node->Open) {
        return;
    }
    for (const auto& child : node->Children) {
        CollectVisibleNodes(child.get(), depth + 1);
    }
}

bool TTopicInfoView::HandleJsonTreeEvent(const Event& event) {
    if (App_.GetState().CurrentView != EViewType::TopicInfo) {
        return false;
    }
    if (!JsonTreeReady_ || JsonVisibleNodes_.empty()) {
        return false;
    }
    
    if (event == Event::ArrowDown || event == Event::Character('j')) {
        if (JsonSelected_ + 1 < static_cast<int>(JsonVisibleNodes_.size())) {
            ++JsonSelected_;
        }
        App_.PostRefresh();
        return true;
    }
    
    if (event == Event::ArrowUp || event == Event::Character('k')) {
        if (JsonSelected_ > 0) {
            --JsonSelected_;
        }
        App_.PostRefresh();
        return true;
    }
    
    if (event == Event::ArrowRight || event == Event::Character('l')) {
        auto* node = JsonVisibleNodes_[JsonSelected_].Node;
        if (!node->Children.empty()) {
            if (!node->Open) {
                node->Open = true;
                BuildJsonVisible(node);
            } else if (JsonSelected_ + 1 < static_cast<int>(JsonVisibleNodes_.size()) &&
                       JsonVisibleNodes_[JsonSelected_ + 1].Depth == JsonVisibleNodes_[JsonSelected_].Depth + 1) {
                ++JsonSelected_;
            }
            App_.PostRefresh();
            return true;
        }
    }
    
    if (event == Event::ArrowLeft || event == Event::Character('h')) {
        auto* node = JsonVisibleNodes_[JsonSelected_].Node;
        if (!node->Children.empty() && node->Open) {
            node->Open = false;
            BuildJsonVisible(node);
            App_.PostRefresh();
            return true;
        }
        if (node->Parent) {
            BuildJsonVisible(node->Parent);
            App_.PostRefresh();
            return true;
        }
    }
    
    return false;
}

Element TTopicInfoView::RenderContent() {
    std::string title = "Topic Info: " + std::string(TopicPath_.c_str());
    
    if (Loading_ && !HasData_) {
        return vbox({
            text(title) | bold | color(NTheme::AccentText),
            separator(),
            text("Loading...") | center | dim
        });
    }
    
    if (!ErrorMessage_.empty()) {
        return vbox({
            text(title) | bold | color(NTheme::AccentText),
            separator(),
            text("Error: " + std::string(ErrorMessage_.c_str())) | color(Color::Red)
        });
    }
    
    if (!HasData_) {
        return vbox({
            text("Topic Info") | bold | color(NTheme::AccentText),
            separator(),
            text("No topic selected") | center | dim
        });
    }
    
    return vbox({
        text(title) | bold | color(NTheme::AccentText),
        separator(),
        RenderTopicConfig(),
        separator(),
        RenderViewerConfig(),
        separator(),
        RenderViewerJsonTree()
    }) | yframe | vscroll_indicator;
}

Element TTopicInfoView::RenderTopicConfig() {
    Elements rows;
    
    rows.push_back(hbox({
        text(" Partitions: ") | dim,
        text(std::to_string(Data_.PartitionCount)) | bold,
        text("  ") | dim,
        text(" Retention: ") | dim,
        text(std::string(FormatDuration(Data_.RetentionPeriod).c_str())) | bold,
        text("  ") | dim,
        text(" Storage MB: ") | dim,
        text(std::to_string(Data_.RetentionStorageMb)) | bold
    }));
    
    rows.push_back(hbox({
        text(" Consumers: ") | dim,
        text(std::to_string(Data_.ConsumerNames.size())) | bold
    }));
    
    if (!Data_.ConsumerNames.empty()) {
        Elements consumerElems;
        for (const auto& name : Data_.ConsumerNames) {
            consumerElems.push_back(text(std::string(name.c_str())) | color(NTheme::AccentText));
            consumerElems.push_back(text("  "));
        }
        rows.push_back(hbox({text("   ") | dim, hbox(consumerElems)}));
    }
    
    if (!Data_.Codecs.empty()) {
        Elements codecElems;
        for (const auto& codec : Data_.Codecs) {
            codecElems.push_back(text(std::string(codec.c_str())) | color(Color::Cyan));
            codecElems.push_back(text(" "));
        }
        rows.push_back(hbox({text(" Codecs: ") | dim, hbox(codecElems)}));
    }
    
    return vbox(rows);
}

Element TTopicInfoView::RenderViewerConfig() {
    Elements rows;
    rows.push_back(NTheme::SectionHeader("Viewer Details"));
    
    if (ViewerLoading_ && !HasViewerData_) {
        rows.push_back(text(" Loading viewer info...") | dim);
        return vbox(rows);
    }
    
    if (!ViewerData_.Error.empty()) {
        rows.push_back(text(" Viewer error: " + std::string(ViewerData_.Error.c_str())) | color(Color::Red));
        return vbox(rows);
    }
    
    if (!HasViewerData_) {
        rows.push_back(text(" Viewer info not available") | dim);
        return vbox(rows);
    }
    
    const auto& info = ViewerData_;
    
    if (!info.Owner.empty() || !info.PathType.empty() || !info.PathState.empty()) {
        rows.push_back(hbox({
            text(" Owner: ") | dim, text(info.Owner.c_str()),
            text("  Type: ") | dim, text(info.PathType.c_str()),
            text("  State: ") | dim, text(info.PathState.c_str())
        }));
    }
    
    Elements idLine;
    if (info.PathId > 0) {
        idLine.push_back(text(" PathId: ") | dim);
        idLine.push_back(text(ToString(info.PathId).c_str()));
    }
    if (info.SchemeshardId > 0) {
        idLine.push_back(text("  SchemeShard: ") | dim);
        idLine.push_back(text(ToString(info.SchemeshardId).c_str()));
    }
    if (info.CreateTxId > 0) {
        idLine.push_back(text("  CreateTx: ") | dim);
        idLine.push_back(text(ToString(info.CreateTxId).c_str()));
    }
    if (info.CreateTime != TInstant::Zero()) {
        idLine.push_back(text("  Created: ") | dim);
        idLine.push_back(text(info.CreateTime.FormatLocalTime("%Y-%m-%d %H:%M:%S").c_str()));
    }
    if (!idLine.empty()) {
        rows.push_back(hbox(idLine));
    }
    
    Elements groupLine;
    if (info.PartitionsCount > 0) {
        groupLine.push_back(text(" Partitions: ") | dim);
        groupLine.push_back(text(ToString(info.PartitionsCount).c_str()));
    }
    if (info.NextPartitionId > 0) {
        groupLine.push_back(text("  NextId: ") | dim);
        groupLine.push_back(text(ToString(info.NextPartitionId).c_str()));
    }
    if (info.PartitionPerTablet > 0) {
        groupLine.push_back(text("  PerTablet: ") | dim);
        groupLine.push_back(text(ToString(info.PartitionPerTablet).c_str()));
    }
    if (info.TotalGroupCount > 0) {
        groupLine.push_back(text("  Groups: ") | dim);
        groupLine.push_back(text(ToString(info.TotalGroupCount).c_str()));
    }
    if (info.BalancerTabletId > 0) {
        groupLine.push_back(text("  Balancer: ") | dim);
        groupLine.push_back(text(ToString(info.BalancerTabletId).c_str()));
    }
    if (info.AlterVersion > 0) {
        groupLine.push_back(text("  AlterVer: ") | dim);
        groupLine.push_back(text(ToString(info.AlterVersion).c_str()));
    }
    if (!groupLine.empty()) {
        rows.push_back(hbox(groupLine));
    }
    
    Elements retentionLine;
    if (info.RetentionSeconds > 0) {
        retentionLine.push_back(text(" Retention: ") | dim);
        retentionLine.push_back(text(std::string(FormatSeconds(info.RetentionSeconds).c_str())));
    }
    if (info.RetentionBytes > 0) {
        retentionLine.push_back(text("  Storage: ") | dim);
        retentionLine.push_back(text(std::string(FormatBytes(info.RetentionBytes).c_str())));
    }
    if (!retentionLine.empty()) {
        rows.push_back(hbox(retentionLine));
    }
    
    Elements writeLine;
    if (info.WriteSpeedBytesPerSec > 0) {
        writeLine.push_back(text(" WriteSpeed: ") | dim);
        writeLine.push_back(text(std::string(FormatBytes(info.WriteSpeedBytesPerSec).c_str())));
        writeLine.push_back(text("/s"));
    }
    if (info.BurstBytes > 0) {
        writeLine.push_back(text("  Burst: ") | dim);
        writeLine.push_back(text(std::string(FormatBytes(info.BurstBytes).c_str())));
    }
    if (!writeLine.empty()) {
        rows.push_back(hbox(writeLine));
    }
    
    Elements configLine;
    if (info.MaxCountInPartition > 0) {
        configLine.push_back(text(" MaxCount: ") | dim);
        configLine.push_back(text(ToString(info.MaxCountInPartition).c_str()));
    }
    if (info.SourceIdLifetimeSeconds > 0) {
        configLine.push_back(text("  SourceIdTTL: ") | dim);
        configLine.push_back(text(std::string(FormatSeconds(info.SourceIdLifetimeSeconds).c_str())));
    }
    if (info.SourceIdMaxCounts > 0) {
        configLine.push_back(text("  SourceIdMax: ") | dim);
        configLine.push_back(text(ToString(info.SourceIdMaxCounts).c_str()));
    }
    if (!configLine.empty()) {
        rows.push_back(hbox(configLine));
    }
    
    Elements authLine;
    if (info.HasRequireAuthRead || info.HasRequireAuthWrite) {
        authLine.push_back(text(" Auth: ") | dim);
        if (info.HasRequireAuthRead) {
            authLine.push_back(text("read=" + std::string(FormatBool(info.RequireAuthRead).c_str())));
        }
        if (info.HasRequireAuthWrite) {
            if (info.HasRequireAuthRead) {
                authLine.push_back(text(" "));
            }
            authLine.push_back(text("write=" + std::string(FormatBool(info.RequireAuthWrite).c_str())));
        }
    }
    if (!info.MeteringMode.empty()) {
        authLine.push_back(text("  Metering: ") | dim);
        authLine.push_back(text(info.MeteringMode.c_str()));
    }
    if (!info.FormatVersion.empty()) {
        authLine.push_back(text("  Format: ") | dim);
        authLine.push_back(text(info.FormatVersion.c_str()));
    }
    if (!authLine.empty()) {
        rows.push_back(hbox(authLine));
    }
    
    if (!info.SupportedCodecs.empty()) {
        Elements codecElems;
        for (const auto& codec : info.SupportedCodecs) {
            codecElems.push_back(text(std::string(codec.c_str())) | color(Color::Cyan));
            codecElems.push_back(text(" "));
        }
        rows.push_back(hbox({text(" Viewer Codecs: ") | dim, hbox(codecElems)}));
    }
    
    if (!info.Consumers.empty()) {
        Elements consumerLine;
        consumerLine.push_back(text(" Consumer cfg: ") | dim);
        size_t limit = 3;
        for (size_t i = 0; i < info.Consumers.size() && i < limit; ++i) {
            const auto& c = info.Consumers[i];
            TStringBuilder item;
            item << c.Name;
            if (!c.Type.empty()) {
                item << "(" << c.Type << ")";
            }
            if (c.ReadFromTimestampMs > 0) {
                item << "@";
                item << FormatTimestampMs(c.ReadFromTimestampMs);
            }
            consumerLine.push_back(text(item.c_str()) | color(NTheme::AccentText));
            if (i + 1 < info.Consumers.size() && i + 1 < limit) {
                consumerLine.push_back(text(", "));
            }
        }
        if (info.Consumers.size() > limit) {
            consumerLine.push_back(text(" +"));
            consumerLine.push_back(text(ToString(info.Consumers.size() - limit).c_str()));
            consumerLine.push_back(text(" more"));
        }
        rows.push_back(hbox(consumerLine));
    }
    
    if (!info.YdbDatabasePath.empty()) {
        rows.push_back(hbox({
            text(" Database: ") | dim,
            text(info.YdbDatabasePath.c_str())
        }));
    }
    
    return vbox(rows);
}

Element TTopicInfoView::RenderViewerJsonTree() {
    Elements rows;
    rows.push_back(NTheme::SectionHeader("Viewer JSON"));
    
    if (ViewerLoading_ && !JsonTreeReady_) {
        rows.push_back(text(" Loading viewer JSON...") | dim);
        return vbox(rows);
    }
    
    if (!JsonTreeError_.empty()) {
        rows.push_back(text(" Viewer JSON error: " + std::string(JsonTreeError_.c_str())) | color(Color::Red));
        return vbox(rows);
    }
    
    if (!HasViewerData_ || !JsonTreeReady_ || JsonVisibleNodes_.empty()) {
        rows.push_back(text(" Viewer JSON not available") | dim);
        return vbox(rows);
    }
    
    Elements lines;
    lines.reserve(JsonVisibleNodes_.size());
    
    for (size_t i = 0; i < JsonVisibleNodes_.size(); ++i) {
        const auto& entry = JsonVisibleNodes_[i];
        const auto* node = entry.Node;
        TStringBuilder line;
        line << TString(entry.Depth * 2, ' ');
        if (!node->Children.empty()) {
            line << (node->Open ? "v " : "> ");
        } else {
            line << "  ";
        }
        line << node->Name;
        if (node->Value) {
            if (node->Value->IsMap()) {
                line << ": {" << node->Value->GetMap().size() << "}";
            } else if (node->Value->IsArray()) {
                line << ": [" << node->Value->GetArray().size() << "]";
            } else {
                line << ": " << FormatJsonLeaf(*node->Value);
            }
        }
        
        auto element = text(std::string(line.c_str()));
        if (static_cast<int>(i) == JsonSelected_) {
            element = element | bgcolor(NTheme::HighlightBg) | focus;
        }
        lines.push_back(element);
    }
    
    rows.push_back(vbox(std::move(lines)));
    return vbox(rows);
}

Element TTopicInfoView::RenderPartitionTable() {
    return Table_.Render();
}

} // namespace NYdb::NConsoleClient
