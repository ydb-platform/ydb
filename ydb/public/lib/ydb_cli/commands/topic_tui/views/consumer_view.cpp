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
}

Component TConsumerView::Build() {
    auto tableComponent = Table_.Build();
    
    return Renderer(tableComponent, [this, tableComponent] {
        CheckAsyncCompletion();
        
        if (Loading_) {
            return vbox({
                hbox({
                    text(" Consumer: ") | bold,
                    text(std::string(ConsumerName_.c_str())) | color(NTheme::AccentText)
                }),
                separator(),
                NTheme::RenderSpinner(SpinnerFrame_, "Loading consumer details...") | center | flex
            }) | border;
        }
        
        if (!ErrorMessage_.empty()) {
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
    if (!Loading_ || !LoadFuture_.valid()) {
        return;
    }
    
    if (LoadFuture_.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
        try {
            auto data = LoadFuture_.get();
            PartitionStats_ = std::move(data.PartitionStats);
            TotalLag_ = data.TotalLag;
            MaxLagTime_ = data.MaxLagTime;
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
            text(std::string(TopicPath_.c_str())) | color(Color::White)
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
        std::string sessionId = p.ReadSessionId.length() > 12 
            ? std::string(p.ReadSessionId.c_str(), 12) + ".." 
            : std::string(p.ReadSessionId.c_str());
        std::string readerName = p.ReaderName.length() > 12 
            ? std::string(p.ReaderName.c_str(), 12) + ".." 
            : std::string(p.ReaderName.c_str());
        
        TVector<TTableCell> cells = {
            TTableCell(ToString(p.PartitionId)),
            TTableCell(FormatBytes(p.StoreSizeBytes)),
            TTableCell(FormatBytes(p.BytesWrittenPerMinute)),
            TTableCell(FormatDuration(p.WriteTimeLag)),
            TTableCell(FormatDuration(p.ReadTimeLag)),
            TTableCell(FormatNumber(p.Lag), NTheme::GetLagColor(p.Lag)),
            TTableCell(FormatNumber(p.UnreadMessages)),
            TTableCell(FormatNumber(p.StartOffset)),
            TTableCell(FormatNumber(p.EndOffset)),
            TTableCell(FormatNumber(p.CommittedOffset)),
            TTableCell(sessionId.empty() ? "-" : sessionId),
            TTableCell(readerName.empty() ? "-" : readerName),
            TTableCell(p.PartitionNodeId > 0 ? ToString(p.PartitionNodeId) : "-")
        };
        
        Table_.SetRow(i, cells);
    }
}

void TConsumerView::StartAsyncLoad() {
    if (Loading_ || ConsumerName_.empty()) {
        return;
    }
    
    Loading_ = true;
    ErrorMessage_.clear();
    SpinnerFrame_ = 0;
    
    TString topicPath = TopicPath_;
    TString consumerName = ConsumerName_;
    auto* topicClient = &App_.GetTopicClient();
    
    LoadFuture_ = std::async(std::launch::async, [topicPath, consumerName, topicClient]() -> TConsumerData {
        TConsumerData data;
        
        auto result = topicClient->DescribeConsumer(topicPath, consumerName,
            NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
        
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

} // namespace NYdb::NConsoleClient
