#include "consumer_view.h"
#include "../topic_tui_app.h"
#include "../widgets/sparkline.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

// Define table columns using theme constants
static TVector<TTableColumn> CreateConsumerTableColumns() {
    return {
        {"Part", NTheme::ColPartitionId},
        {"Size", NTheme::ColBytes},
        {"Write/m", NTheme::ColBytes},
        {"WriteRate", NTheme::ColSparkline},  // Sparkline column
        {"WriteLag", NTheme::ColDurationShort},
        {"ReadLag", NTheme::ColDurationShort},
        {"Uncommit", NTheme::ColLag},
        {"Unread", NTheme::ColBytes},
        {"Start", NTheme::ColCount},
        {"End", NTheme::ColCount},
        {"Commit", NTheme::ColCount},
        {"SessionID", NTheme::ColSessionId},
        {"Reader", NTheme::ColReaderName},
        {"Node", NTheme::ColNodeId}
    };
}

TConsumerView::TConsumerView(TTopicTuiApp& app)
    : App_(app)
    , Table_(CreateConsumerTableColumns())
{
    // Set up table callbacks
    Table_.OnSelect = [this](int row) {
        if (row >= 0 && row < static_cast<int>(PartitionStats_.size())) {
            const auto& p = PartitionStats_[row];
            if (OnCommitOffset) {
                OnCommitOffset(p.PartitionId, p.CommittedOffset);
            }
        }
    };
    
    // Sort callback
    Table_.OnSortChanged = [this](int col, bool asc) {
        SortPartitions(col, asc);
        PopulateTable();
    };
}

Component TConsumerView::Build() {
    auto tableComponent = Table_.Build();
    
    return Renderer(tableComponent, [this, tableComponent] {
        CheckAsyncCompletion();
        
        // Only show spinner if loading AND we have no data yet
        if (Loading_ && PartitionStats_.empty()) {
            return vbox({
                hbox({
                    text(" Consumer: ") | bold,
                    text(std::string(ConsumerName_.c_str())) | color(NTheme::AccentText)
                }),
                separator(),
                NTheme::RenderSpinner(SpinnerFrame_, "Loading consumer details...") | center | flex
            }) | border;
        }
        
        if (!ErrorMessage_.empty() && PartitionStats_.empty()) {
            return vbox({
                text("Error: " + std::string(ErrorMessage_.c_str())) | color(NTheme::ErrorText) | center
            }) | border;
        }
        
        return vbox({
            RenderHeader(),
            separator(),
            tableComponent->Render() | flex
        }) | border;
    }) | CatchEvent([this](Event event) {
        if (App_.GetState().CurrentView != EViewType::ConsumerDetails) {
            return false;
        }
        if (Loading_) {
            return false;
        }
        
        // 'o' key for commit offset
        if (event == Event::Character('o') || event == Event::Character('O')) {
            int row = Table_.GetSelectedRow();
            if (row >= 0 && row < static_cast<int>(PartitionStats_.size())) {
                const auto& p = PartitionStats_[row];
                if (OnCommitOffset) {
                    OnCommitOffset(p.PartitionId, p.CommittedOffset);
                }
            }
            return true;
        }
        
        // 'd' key for drop consumer
        if (event == Event::Character('d') || event == Event::Character('D')) {
            if (OnDropConsumer) {
                OnDropConsumer();
            }
            return true;
        }
        
        // Let table handle navigation
        return Table_.HandleEvent(event);
    });
}

void TConsumerView::SetConsumer(const TString& topicPath, const TString& consumerName) {
    TopicPath_ = topicPath;
    ConsumerName_ = consumerName;
    StartAsyncLoad();
}

void TConsumerView::Refresh() {
    StartAsyncLoad();
}

void TConsumerView::CheckAsyncCompletion() {
    // Auto-refresh if enough time has passed and not already loading
    TDuration refreshRate = App_.GetRefreshRate();
    if (!Loading_ && LastRefreshTime_ != TInstant::Zero() && 
        TInstant::Now() - LastRefreshTime_ > refreshRate) {
        StartAsyncLoad();
    }
    
    if (!Loading_ || !LoadFuture_.valid()) {
        return;
    }
    
    if (LoadFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
        try {
            auto data = LoadFuture_.get();
            PartitionStats_ = std::move(data.PartitionStats);
            TotalLag_ = data.TotalLag;
            MaxLagTime_ = data.MaxLagTime;
            
            // Update per-partition sparkline histories
            for (const auto& p : PartitionStats_) {
                PartitionWriteRateHistory_[p.PartitionId].Update(
                    static_cast<double>(p.BytesWrittenPerMinute) / 60.0);
            }
            
            ErrorMessage_.clear();
            PopulateTable();
        } catch (const std::exception& e) {
            ErrorMessage_ = e.what();
        }
        Loading_ = false;
    } else {
        SpinnerFrame_++;
    }
}

Element TConsumerView::RenderHeader() {
    return vbox({
        hbox({
            text(" Consumer: ") | bold,
            text(std::string(ConsumerName_.c_str())) | color(NTheme::AccentText),
            text(" on ") | dim,
            text(std::string(TopicPath_.c_str())) | color(Color::White),
            filler(),
            Loading_ ? (text(" ⟳ ") | color(Color::Yellow) | dim) : text("")
        }),
        hbox({
            text(" Total Lag: ") | dim,
            text(std::string(FormatNumber(TotalLag_).c_str())) | bold | color(NTheme::GetLagColor(TotalLag_)),
            text("   Max Lag Time: ") | dim,
            text(std::string(FormatDuration(MaxLagTime_).c_str())) | bold
        })
    });
}

void TConsumerView::PopulateTable() {
    Table_.SetRowCount(PartitionStats_.size());
    
    for (size_t i = 0; i < PartitionStats_.size(); ++i) {
        const auto& p = PartitionStats_[i];
        
        // Truncate session ID and reader name for display
        TString sessionId = p.ReadSessionId.length() > 12 
            ? TString(p.ReadSessionId.c_str(), 12) + ".." 
            : p.ReadSessionId;
        TString readerName = p.ReaderName.length() > 12 
            ? TString(p.ReaderName.c_str(), 12) + ".." 
            : p.ReaderName;
        
        // Use UpdateCell for each column - this tracks changes automatically
        Table_.UpdateCell(i, 0, ToString(p.PartitionId));
        Table_.UpdateCell(i, 1, FormatBytes(p.StoreSizeBytes));
        Table_.UpdateCell(i, 2, FormatBytes(p.BytesWrittenPerMinute));
        
        // Render per-partition sparkline with zero-based scaling
        auto it = PartitionWriteRateHistory_.find(p.PartitionId);
        if (it != PartitionWriteRateHistory_.end() && !it->second.Empty()) {
            const auto& values = it->second.GetValues();
            TString sparkStr;
            if (!values.empty()) {
                double maxVal = it->second.GetMax();
                size_t start = values.size() > 15 ? values.size() - 15 : 0;
                for (size_t j = start; j < values.size(); ++j) {
                    double normalized = values[j] / maxVal;
                    int level = static_cast<int>(normalized * 8);
                    level = std::max(0, std::min(8, level));
                    sparkStr += SparklineChars[level];
                }
            }
            Table_.UpdateCell(i, 3, sparkStr.empty() ? "-" : sparkStr);
        } else {
            Table_.UpdateCell(i, 3, "-");
        }
        
        Table_.UpdateCell(i, 4, FormatDuration(p.WriteTimeLag));
        Table_.UpdateCell(i, 5, FormatDuration(p.ReadTimeLag));
        Table_.UpdateCell(i, 6, TTableCell(FormatNumber(p.Lag), NTheme::GetLagColor(p.Lag)));
        Table_.UpdateCell(i, 7, FormatNumber(p.UnreadMessages));
        Table_.UpdateCell(i, 8, FormatNumber(p.StartOffset));
        Table_.UpdateCell(i, 9, FormatNumber(p.EndOffset));
        Table_.UpdateCell(i, 10, FormatNumber(p.CommittedOffset));
        Table_.UpdateCell(i, 11, sessionId.empty() ? "-" : sessionId);
        Table_.UpdateCell(i, 12, readerName.empty() ? "-" : readerName);
        Table_.UpdateCell(i, 13, p.PartitionNodeId > 0 ? ToString(p.PartitionNodeId) : "-");
    }
}

void TConsumerView::StartAsyncLoad() {
    if (Loading_ || ConsumerName_.empty()) {
        return;
    }
    
    Loading_ = true;
    ErrorMessage_.clear();
    SpinnerFrame_ = 0;
    LastRefreshTime_ = TInstant::Now();
    
    TString topicPath = TopicPath_;
    TString consumerName = ConsumerName_;
    auto* topicClient = &App_.GetTopicClient();
    
    LoadFuture_ = std::async(std::launch::async, [topicPath, consumerName, topicClient]() -> TConsumerData {
        TConsumerData data;
        
        auto result = topicClient->DescribeConsumer(topicPath, consumerName,
            NTopic::TDescribeConsumerSettings().IncludeStats(true).IncludeLocation(true)).GetValueSync();
        
        if (!result.IsSuccess()) {
            throw std::runtime_error(result.GetIssues().ToString());
        }
        
        const auto& desc = result.GetConsumerDescription();
        
        for (const auto& part : desc.GetPartitions()) {
            TPartitionConsumerInfo info;
            info.PartitionId = part.GetPartitionId();
            
            if (part.GetPartitionStats()) {
                const auto& partStats = *part.GetPartitionStats();
                info.StartOffset = partStats.GetStartOffset();
                info.EndOffset = partStats.GetEndOffset();
                info.StoreSizeBytes = partStats.GetStoreSizeBytes();
                info.BytesWrittenPerMinute = partStats.GetBytesWrittenPerMinute();
                info.WriteTimeLag = partStats.GetMaxWriteTimeLag();
            }
            
            if (part.GetPartitionConsumerStats()) {
                const auto& consumerStats = *part.GetPartitionConsumerStats();
                info.CommittedOffset = consumerStats.GetCommittedOffset();
                info.Lag = info.EndOffset > info.CommittedOffset ? info.EndOffset - info.CommittedOffset : 0;
                info.UnreadMessages = info.EndOffset > consumerStats.GetLastReadOffset() 
                    ? info.EndOffset - consumerStats.GetLastReadOffset() : 0;
                info.LastReadTime = consumerStats.GetLastReadTime();
                info.ReadTimeLag = consumerStats.GetMaxReadTimeLag();
                info.CommitTimeLag = consumerStats.GetMaxCommittedTimeLag();
                info.ReaderName = consumerStats.GetReaderName();
                info.ReadSessionId = consumerStats.GetReadSessionId();
                
                data.TotalLag += info.Lag;
                if (info.CommitTimeLag > data.MaxLagTime) {
                    data.MaxLagTime = info.CommitTimeLag;
                }
            }
            
            if (part.GetPartitionLocation()) {
                info.PartitionNodeId = part.GetPartitionLocation()->GetNodeId();
            }
            
            data.PartitionStats.push_back(info);
        }
        
        return data;
    });
}

void TConsumerView::SortPartitions(int column, bool ascending) {
    // Columns: Part, Size, Write/m, WriteRate(sparkline), WriteLag, ReadLag, Uncommit, Unread, Start, End, Commit, SessionID, Reader, Node
    std::stable_sort(PartitionStats_.begin(), PartitionStats_.end(),
        [column, ascending](const TPartitionConsumerInfo& a, const TPartitionConsumerInfo& b) {
            int cmp = 0;
            switch (column) {
                case 0: cmp = (a.PartitionId < b.PartitionId) ? -1 : (a.PartitionId > b.PartitionId) ? 1 : 0; break;
                case 1: cmp = (a.StoreSizeBytes < b.StoreSizeBytes) ? -1 : (a.StoreSizeBytes > b.StoreSizeBytes) ? 1 : 0; break;
                case 2: cmp = (a.BytesWrittenPerMinute < b.BytesWrittenPerMinute) ? -1 : (a.BytesWrittenPerMinute > b.BytesWrittenPerMinute) ? 1 : 0; break;
                case 3: cmp = (a.BytesWrittenPerMinute < b.BytesWrittenPerMinute) ? -1 : (a.BytesWrittenPerMinute > b.BytesWrittenPerMinute) ? 1 : 0; break; // sparkline
                case 4: cmp = (a.WriteTimeLag < b.WriteTimeLag) ? -1 : (a.WriteTimeLag > b.WriteTimeLag) ? 1 : 0; break;
                case 5: cmp = (a.ReadTimeLag < b.ReadTimeLag) ? -1 : (a.ReadTimeLag > b.ReadTimeLag) ? 1 : 0; break;
                case 6: cmp = (a.Lag < b.Lag) ? -1 : (a.Lag > b.Lag) ? 1 : 0; break;
                case 7: cmp = (a.UnreadMessages < b.UnreadMessages) ? -1 : (a.UnreadMessages > b.UnreadMessages) ? 1 : 0; break;
                case 8: cmp = (a.StartOffset < b.StartOffset) ? -1 : (a.StartOffset > b.StartOffset) ? 1 : 0; break;
                case 9: cmp = (a.EndOffset < b.EndOffset) ? -1 : (a.EndOffset > b.EndOffset) ? 1 : 0; break;
                case 10: cmp = (a.CommittedOffset < b.CommittedOffset) ? -1 : (a.CommittedOffset > b.CommittedOffset) ? 1 : 0; break;
                case 11: cmp = a.ReadSessionId.compare(b.ReadSessionId); break;
                case 12: cmp = a.ReaderName.compare(b.ReaderName); break;
                case 13: cmp = (a.PartitionNodeId < b.PartitionNodeId) ? -1 : (a.PartitionNodeId > b.PartitionNodeId) ? 1 : 0; break;
                default: cmp = 0; break;
            }
            return ascending ? (cmp < 0) : (cmp > 0);
        });
}

} // namespace NYdb::NConsoleClient
