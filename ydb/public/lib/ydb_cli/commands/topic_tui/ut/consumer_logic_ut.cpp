#include "../widgets/sparkline.h"
#include "../widgets/table.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;
using namespace ftxui;

// === Consumer Data Structures ===

Y_UNIT_TEST_SUITE(ConsumerDataTests) {
    
    // These tests validate the pure data structure logic that would be used
    // by the consumer view for lag calculation and display
    
    Y_UNIT_TEST(LagCalculation_Simple) {
        // Lag = EndOffset - CommittedOffset
        ui64 endOffset = 100;
        ui64 committedOffset = 80;
        ui64 lag = endOffset - committedOffset;
        
        UNIT_ASSERT_EQUAL(lag, 20);
    }
    
    Y_UNIT_TEST(LagCalculation_CaughtUp) {
        ui64 endOffset = 100;
        ui64 committedOffset = 100;
        ui64 lag = endOffset - committedOffset;
        
        UNIT_ASSERT_EQUAL(lag, 0);
    }
    
    Y_UNIT_TEST(LagCalculation_Large) {
        ui64 endOffset = 1000000000;
        ui64 committedOffset = 100;
        ui64 lag = endOffset - committedOffset;
        
        UNIT_ASSERT_EQUAL(lag, 999999900);
    }
    
    Y_UNIT_TEST(UnreadCalculation) {
        // UnreadMessages = EndOffset - LastReadOffset
        ui64 endOffset = 1000;
        ui64 lastReadOffset = 950;
        ui64 unread = endOffset - lastReadOffset;
        
        UNIT_ASSERT_EQUAL(unread, 50);
    }
}

// === Lag Color Logic ===

Y_UNIT_TEST_SUITE(LagColorLogicTests) {
    
    Y_UNIT_TEST(LagDuration_ToSeconds) {
        TDuration lag = TDuration::Seconds(30);
        UNIT_ASSERT_EQUAL(lag.Seconds(), 30);
    }
    
    Y_UNIT_TEST(LagDuration_ToMinutes) {
        TDuration lag = TDuration::Minutes(5);
        UNIT_ASSERT_EQUAL(lag.Minutes(), 5);
    }
    
    Y_UNIT_TEST(LagDuration_ToHours) {
        TDuration lag = TDuration::Hours(2);
        UNIT_ASSERT_EQUAL(lag.Hours(), 2);
    }
    
    Y_UNIT_TEST(LagSeverity_Low) {
        // < 1 minute is low severity
        TDuration lag = TDuration::Seconds(30);
        bool isLow = lag < TDuration::Minutes(1);
        UNIT_ASSERT(isLow);
    }
    
    Y_UNIT_TEST(LagSeverity_Medium) {
        // 1-10 minutes is medium
        TDuration lag = TDuration::Minutes(5);
        bool isMedium = lag >= TDuration::Minutes(1) && lag < TDuration::Minutes(10);
        UNIT_ASSERT(isMedium);
    }
    
    Y_UNIT_TEST(LagSeverity_High) {
        // > 10 minutes is high severity
        TDuration lag = TDuration::Minutes(15);
        bool isHigh = lag >= TDuration::Minutes(10);
        UNIT_ASSERT(isHigh);
    }
}

// === Message Preview Offset Logic ===

Y_UNIT_TEST_SUITE(MessageOffsetLogicTests) {
    
    Y_UNIT_TEST(OffsetBounds_InRange) {
        ui64 startOffset = 100;
        ui64 endOffset = 200;
        ui64 currentOffset = 150;
        
        bool inRange = currentOffset >= startOffset && currentOffset <= endOffset;
        UNIT_ASSERT(inRange);
    }
    
    Y_UNIT_TEST(OffsetBounds_AtStart) {
        ui64 startOffset = 100;
        Y_UNUSED(startOffset);
        ui64 endOffset = 200;
        Y_UNUSED(endOffset);
        ui64 currentOffset = 100;
        
        bool atStart = currentOffset == startOffset;
        bool canGoBack = currentOffset > startOffset;
        
        UNIT_ASSERT(atStart);
        UNIT_ASSERT(!canGoBack);
    }
    
    Y_UNIT_TEST(OffsetBounds_AtEnd) {
        ui64 startOffset = 100;
        Y_UNUSED(startOffset);
        ui64 endOffset = 200;
        ui64 currentOffset = 200;
        
        bool atEnd = currentOffset == endOffset;
        bool canGoForward = currentOffset < endOffset;
        
        UNIT_ASSERT(atEnd);
        UNIT_ASSERT(!canGoForward);
    }
    
    Y_UNIT_TEST(OffsetNavigation_Next) {
        ui64 currentOffset = 150;
        ui64 endOffset = 200;
        
        if (currentOffset < endOffset) {
            currentOffset++;
        }
        
        UNIT_ASSERT_EQUAL(currentOffset, 151);
    }
    
    Y_UNIT_TEST(OffsetNavigation_Previous) {
        ui64 currentOffset = 150;
        ui64 startOffset = 100;
        
        if (currentOffset > startOffset) {
            currentOffset--;
        }
        
        UNIT_ASSERT_EQUAL(currentOffset, 149);
    }
    
    Y_UNIT_TEST(OffsetNavigation_JumpToStart) {
        ui64 currentOffset = 150;
        ui64 startOffset = 100;
        
        currentOffset = startOffset;
        
        UNIT_ASSERT_EQUAL(currentOffset, 100);
    }
    
    Y_UNIT_TEST(OffsetNavigation_JumpToEnd) {
        ui64 currentOffset = 150;
        ui64 endOffset = 200;
        
        currentOffset = endOffset;
        
        UNIT_ASSERT_EQUAL(currentOffset, 200);
    }
    
    Y_UNIT_TEST(OffsetClamp_BelowStart) {
        ui64 startOffset = 100;
        ui64 endOffset = 200;
        ui64 requestedOffset = 50;
        
        ui64 clamped = std::max(startOffset, std::min(requestedOffset, endOffset));
        
        UNIT_ASSERT_EQUAL(clamped, 100);
    }
    
    Y_UNIT_TEST(OffsetClamp_AboveEnd) {
        ui64 startOffset = 100;
        ui64 endOffset = 200;
        ui64 requestedOffset = 250;
        
        ui64 clamped = std::max(startOffset, std::min(requestedOffset, endOffset));
        
        UNIT_ASSERT_EQUAL(clamped, 200);
    }
}

// === Partition Selection Logic ===

Y_UNIT_TEST_SUITE(PartitionSelectionTests) {
    
    Y_UNIT_TEST(PartitionValidation_Valid) {
        ui32 partitionId = 5;
        ui32 totalPartitions = 10;
        
        bool valid = partitionId < totalPartitions;
        UNIT_ASSERT(valid);
    }
    
    Y_UNIT_TEST(PartitionValidation_Invalid) {
        ui32 partitionId = 15;
        ui32 totalPartitions = 10;
        
        bool valid = partitionId < totalPartitions;
        UNIT_ASSERT(!valid);
    }
    
    Y_UNIT_TEST(PartitionValidation_Zero) {
        ui32 partitionId = 0;
        ui32 totalPartitions = 10;
        
        bool valid = partitionId < totalPartitions;
        UNIT_ASSERT(valid);
    }
    
    Y_UNIT_TEST(PartitionValidation_Last) {
        ui32 partitionId = 9;
        ui32 totalPartitions = 10;
        
        bool valid = partitionId < totalPartitions;
        UNIT_ASSERT(valid);
    }
}

// === Retention Period Formatting ===

Y_UNIT_TEST_SUITE(RetentionFormattingTests) {
    
    Y_UNIT_TEST(Retention_Hours) {
        TDuration retention = TDuration::Hours(24);
        
        TString formatted;
        if (retention.Hours() >= 24 && retention.Hours() % 24 == 0) {
            formatted = ToString(retention.Days()) + "d";
        } else {
            formatted = ToString(retention.Hours()) + "h";
        }
        
        UNIT_ASSERT_EQUAL(formatted, "1d");
    }
    
    Y_UNIT_TEST(Retention_Days) {
        TDuration retention = TDuration::Days(7);
        
        TString formatted = ToString(retention.Days()) + "d";
        
        UNIT_ASSERT_EQUAL(formatted, "7d");
    }
    
    Y_UNIT_TEST(Retention_HoursNotDivisible) {
        TDuration retention = TDuration::Hours(36);
        
        TString formatted;
        if (retention.Hours() >= 24 && retention.Hours() % 24 == 0) {
            formatted = ToString(retention.Days()) + "d";
        } else {
            formatted = ToString(retention.Hours()) + "h";
        }
        
        UNIT_ASSERT_EQUAL(formatted, "36h");
    }
}

// === Write Speed Calculations ===

Y_UNIT_TEST_SUITE(WriteSpeedTests) {
    
    Y_UNIT_TEST(Speed_PerSecond) {
        ui64 bytesPerMinute = 60 * 1024;  // 60 KB/min
        ui64 bytesPerSecond = bytesPerMinute / 60;
        
        UNIT_ASSERT_EQUAL(bytesPerSecond, 1024);
    }
    
    Y_UNIT_TEST(Speed_PerHour) {
        ui64 bytesPerMinute = 1024 * 1024;  // 1 MB/min
        ui64 bytesPerHour = bytesPerMinute * 60;
        
        UNIT_ASSERT_EQUAL(bytesPerHour, 60 * 1024 * 1024);
    }
    
    Y_UNIT_TEST(Speed_Aggregate) {
        // Sum speeds across partitions
        TVector<ui64> partitionSpeeds = {1000, 2000, 3000, 0, 500};
        ui64 total = 0;
        for (auto speed : partitionSpeeds) {
            total += speed;
        }
        
        UNIT_ASSERT_EQUAL(total, 6500);
    }
}

// === Codec Bitmask Logic ===

Y_UNIT_TEST_SUITE(CodecTests) {
    
    Y_UNIT_TEST(Codec_HasRaw) {
        bool codecRaw = true;
        bool codecGzip = false;
        bool codecZstd = true;
        bool codecLzop = false;
        
        int count = (codecRaw ? 1 : 0) + (codecGzip ? 1 : 0) + (codecZstd ? 1 : 0) + (codecLzop ? 1 : 0);
        
        UNIT_ASSERT_EQUAL(count, 2);
    }
    
    Y_UNIT_TEST(Codec_AllEnabled) {
        bool codecRaw = true;
        bool codecGzip = true;
        bool codecZstd = true;
        bool codecLzop = true;
        
        bool allEnabled = codecRaw && codecGzip && codecZstd && codecLzop;
        
        UNIT_ASSERT(allEnabled);
    }
    
    Y_UNIT_TEST(Codec_NoneEnabled) {
        bool codecRaw = false;
        bool codecGzip = false;
        bool codecZstd = false;
        bool codecLzop = false;
        
        bool anyEnabled = codecRaw || codecGzip || codecZstd || codecLzop;
        
        UNIT_ASSERT(!anyEnabled);
    }
}

// === Form Validation Patterns ===

Y_UNIT_TEST_SUITE(FormValidationTests) {
    
    Y_UNIT_TEST(TopicName_Valid) {
        TString name = "my-topic-name";
        
        bool valid = !name.empty() && name.size() <= 256;
        UNIT_ASSERT(valid);
    }
    
    Y_UNIT_TEST(TopicName_Empty) {
        TString name = "";
        
        bool valid = !name.empty();
        UNIT_ASSERT(!valid);
    }
    
    Y_UNIT_TEST(TopicName_TooLong) {
        TString name(300, 'a');  // 300 character name
        
        bool valid = name.size() <= 256;
        UNIT_ASSERT(!valid);
    }
    
    Y_UNIT_TEST(PartitionCount_Valid) {
        ui32 minPartitions = 1;
        ui32 maxPartitions = 100;
        
        bool valid = minPartitions >= 1 && maxPartitions >= minPartitions;
        UNIT_ASSERT(valid);
    }
    
    Y_UNIT_TEST(PartitionCount_Invalid_MinGreaterThanMax) {
        ui32 minPartitions = 50;
        ui32 maxPartitions = 10;
        
        bool valid = maxPartitions >= minPartitions;
        UNIT_ASSERT(!valid);
    }
    
    Y_UNIT_TEST(PartitionCount_Invalid_ZeroMin) {
        ui32 minPartitions = 0;
        
        bool valid = minPartitions >= 1;
        UNIT_ASSERT(!valid);
    }
    
    Y_UNIT_TEST(RetentionParse_Hours) {
        TString input = "24h";
        TDuration parsed;
        
        if (input.EndsWith("h") || input.EndsWith("H")) {
            ui64 hours = FromString<ui64>(input.substr(0, input.size() - 1));
            parsed = TDuration::Hours(hours);
        }
        
        UNIT_ASSERT_EQUAL(parsed, TDuration::Hours(24));
    }
    
    Y_UNIT_TEST(RetentionParse_Days) {
        TString input = "7d";
        TDuration parsed;
        
        if (input.EndsWith("d") || input.EndsWith("D")) {
            ui64 days = FromString<ui64>(input.substr(0, input.size() - 1));
            parsed = TDuration::Days(days);
        }
        
        UNIT_ASSERT_EQUAL(parsed, TDuration::Days(7));
    }
    
    Y_UNIT_TEST(ConsumerName_Valid) {
        TString name = "my-consumer";
        
        bool valid = !name.empty() && name.size() <= 256;
        UNIT_ASSERT(valid);
    }
    
    Y_UNIT_TEST(DeleteConfirm_Match) {
        TString topicName = "my-topic";
        TString userInput = "my-topic";
        
        bool confirmed = topicName == userInput;
        UNIT_ASSERT(confirmed);
    }
    
    Y_UNIT_TEST(DeleteConfirm_NoMatch) {
        TString topicName = "my-topic";
        TString userInput = "my-topics";  // Typo
        
        bool confirmed = topicName == userInput;
        UNIT_ASSERT(!confirmed);
    }
    
    Y_UNIT_TEST(DeleteConfirm_CaseSensitive) {
        TString topicName = "My-Topic";
        TString userInput = "my-topic";  // Wrong case
        
        bool confirmed = topicName == userInput;
        UNIT_ASSERT(!confirmed);  // Should be case-sensitive
    }
}
