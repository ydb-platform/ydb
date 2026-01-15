#include "topic_info_view.h"
#include "../app_interface.h"
#include "../widgets/sparkline.h"
#include "../common/async_utils.h"

#include <util/string/printf.h>
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
    return Renderer([this] {
        CheckAsyncCompletion();
        return RenderContent();
    });
}

void TTopicInfoView::SetTopic(const TString& topicPath) {
    TopicPath_ = topicPath;
    HasData_ = false;
    ErrorMessage_.clear();
    StartAsyncLoad();
}

void TTopicInfoView::Refresh() {
    StartAsyncLoad();
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

void TTopicInfoView::CheckAsyncCompletion() {
    if (!Loading_) {
        return;
    }
    
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
}

Element TTopicInfoView::RenderContent() {
    std::string title = "Topic Info: " + std::string(TopicPath_.c_str());
    
    if (Loading_) {
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
        NTheme::SectionHeader("Partitions"),
        RenderPartitionTable() | flex
    });
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

Element TTopicInfoView::RenderPartitionTable() {
    return Table_.Render();
}

} // namespace NYdb::NConsoleClient
