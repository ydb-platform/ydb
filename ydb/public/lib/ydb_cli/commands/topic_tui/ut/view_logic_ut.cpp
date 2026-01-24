#include "view_logic_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(ViewLogicSortingTests) {
    
    // === Parent Entry (..) Always First ===
    
    Y_UNIT_TEST(Sort_ParentAlwaysFirst_NameAscending) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("zzz"),
            CreateParentEntry(),
            CreateDirEntry("aaa"),
        };
        
        SortTestEntries(entries, 0, true);  // Sort by name ascending
        
        UNIT_ASSERT_EQUAL(entries[0].Name, "..");
    }
    
    Y_UNIT_TEST(Sort_ParentAlwaysFirst_NameDescending) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("aaa"),
            CreateParentEntry(),
            CreateDirEntry("zzz"),
        };
        
        SortTestEntries(entries, 0, false);  // Sort by name descending
        
        UNIT_ASSERT_EQUAL(entries[0].Name, "..");
    }
    
    Y_UNIT_TEST(Sort_ParentAlwaysFirst_BySizeDescending) {
        TVector<TTestEntry> entries = {
            CreateTopicWithSize("big", 1000000),
            CreateParentEntry(),
            CreateTopicWithSize("small", 100),
        };
        
        SortTestEntries(entries, 3, false);  // Sort by size descending
        
        UNIT_ASSERT_EQUAL(entries[0].Name, "..");
    }
    
    // === Directories Before Topics ===
    
    Y_UNIT_TEST(Sort_DirectoriesBeforeTopics_Ascending) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("a-topic"),
            CreateDirEntry("z-dir"),
            CreateTopicEntry("b-topic"),
        };
        
        SortTestEntries(entries, 0, true);
        
        // Directories first, then topics
        UNIT_ASSERT(entries[0].IsDirectory);
        UNIT_ASSERT_EQUAL(entries[0].Name, "z-dir");
        UNIT_ASSERT(entries[1].IsTopic);
        UNIT_ASSERT(entries[2].IsTopic);
    }
    
    Y_UNIT_TEST(Sort_DirectoriesBeforeTopics_Descending) {
        TVector<TTestEntry> entries = {
            CreateDirEntry("a-dir"),
            CreateTopicEntry("z-topic"),
            CreateDirEntry("b-dir"),
        };
        
        SortTestEntries(entries, 0, false);  // Descending
        
        // Directories still first (sorted desc), then topics
        UNIT_ASSERT(entries[0].IsDirectory);
        UNIT_ASSERT_EQUAL(entries[0].Name, "b-dir");
        UNIT_ASSERT(entries[1].IsDirectory);
        UNIT_ASSERT_EQUAL(entries[1].Name, "a-dir");
        UNIT_ASSERT(entries[2].IsTopic);
    }
    
    Y_UNIT_TEST(Sort_MixedWithParent) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("topic1"),
            CreateDirEntry("dir1"),
            CreateParentEntry(),
            CreateTopicEntry("topic2"),
            CreateDirEntry("dir2"),
        };
        
        SortTestEntries(entries, 0, true);
        
        // Expected order: .., dir1, dir2, topic1, topic2
        UNIT_ASSERT_EQUAL(entries[0].Name, "..");
        UNIT_ASSERT_EQUAL(entries[1].Name, "dir1");
        UNIT_ASSERT_EQUAL(entries[2].Name, "dir2");
        UNIT_ASSERT_EQUAL(entries[3].Name, "topic1");
        UNIT_ASSERT_EQUAL(entries[4].Name, "topic2");
    }
    
    // === Sort By Different Columns ===
    
    Y_UNIT_TEST(Sort_ByPartitionCount_Ascending) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("topic-10", 10),
            CreateTopicEntry("topic-1", 1),
            CreateTopicEntry("topic-5", 5),
        };
        
        SortTestEntries(entries, 2, true);  // Column 2 = Parts
        
        UNIT_ASSERT_EQUAL(entries[0].PartitionCount, 1);
        UNIT_ASSERT_EQUAL(entries[1].PartitionCount, 5);
        UNIT_ASSERT_EQUAL(entries[2].PartitionCount, 10);
    }
    
    Y_UNIT_TEST(Sort_ByPartitionCount_Descending) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("topic-10", 10),
            CreateTopicEntry("topic-1", 1),
            CreateTopicEntry("topic-5", 5),
        };
        
        SortTestEntries(entries, 2, false);  // Column 2 = Parts, descending
        
        UNIT_ASSERT_EQUAL(entries[0].PartitionCount, 10);
        UNIT_ASSERT_EQUAL(entries[1].PartitionCount, 5);
        UNIT_ASSERT_EQUAL(entries[2].PartitionCount, 1);
    }
    
    Y_UNIT_TEST(Sort_BySize) {
        TVector<TTestEntry> entries = {
            CreateTopicWithSize("medium", 5000),
            CreateTopicWithSize("small", 100),
            CreateTopicWithSize("large", 1000000),
        };
        
        SortTestEntries(entries, 3, true);  // Column 3 = Size
        
        UNIT_ASSERT_EQUAL(entries[0].Name, "small");
        UNIT_ASSERT_EQUAL(entries[1].Name, "medium");
        UNIT_ASSERT_EQUAL(entries[2].Name, "large");
    }
    
    Y_UNIT_TEST(Sort_ByConsumers) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("topic-a", 1, 5),
            CreateTopicEntry("topic-b", 1, 2),
            CreateTopicEntry("topic-c", 1, 10),
        };
        
        SortTestEntries(entries, 4, true);  // Column 4 = Consumers
        
        UNIT_ASSERT_EQUAL(entries[0].ConsumerCount, 2);
        UNIT_ASSERT_EQUAL(entries[1].ConsumerCount, 5);
        UNIT_ASSERT_EQUAL(entries[2].ConsumerCount, 10);
    }
    
    Y_UNIT_TEST(Sort_ByRetention) {
        TVector<TTestEntry> entries = {
            CreateTopicWithRetention("medium", TDuration::Hours(24)),
            CreateTopicWithRetention("short", TDuration::Hours(1)),
            CreateTopicWithRetention("long", TDuration::Days(7)),
        };
        
        SortTestEntries(entries, 5, true);  // Column 5 = Retention
        
        UNIT_ASSERT_EQUAL(entries[0].Name, "short");
        UNIT_ASSERT_EQUAL(entries[1].Name, "medium");
        UNIT_ASSERT_EQUAL(entries[2].Name, "long");
    }
    
    Y_UNIT_TEST(Sort_ByWriteSpeed) {
        TVector<TTestEntry> entries = {
            CreateTopicWithWriteSpeed("medium", 5000),
            CreateTopicWithWriteSpeed("slow", 100),
            CreateTopicWithWriteSpeed("fast", 1000000),
        };
        
        SortTestEntries(entries, 6, true);  // Column 6 = Write speed
        
        UNIT_ASSERT_EQUAL(entries[0].Name, "slow");
        UNIT_ASSERT_EQUAL(entries[1].Name, "medium");
        UNIT_ASSERT_EQUAL(entries[2].Name, "fast");
    }
    
    // === Stability ===
    
    Y_UNIT_TEST(Sort_Stable_PreservesOrderForEqual) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("first", 5),
            CreateTopicEntry("second", 5),
            CreateTopicEntry("third", 5),
        };
        
        SortTestEntries(entries, 2, true);  // Sort by partitions (all equal)
        
        // Order should be preserved due to stable_sort
        UNIT_ASSERT_EQUAL(entries[0].Name, "first");
        UNIT_ASSERT_EQUAL(entries[1].Name, "second");
        UNIT_ASSERT_EQUAL(entries[2].Name, "third");
    }
    
    // === Edge Cases ===
    
    Y_UNIT_TEST(Sort_Empty) {
        TVector<TTestEntry> entries;
        
        SortTestEntries(entries, 0, true);
        
        UNIT_ASSERT(entries.empty());
    }
    
    Y_UNIT_TEST(Sort_SingleEntry) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("only-one"),
        };
        
        SortTestEntries(entries, 0, true);
        
        UNIT_ASSERT_EQUAL(entries.size(), 1);
        UNIT_ASSERT_EQUAL(entries[0].Name, "only-one");
    }
    
    Y_UNIT_TEST(Sort_OnlyParent) {
        TVector<TTestEntry> entries = {
            CreateParentEntry(),
        };
        
        SortTestEntries(entries, 0, true);
        
        UNIT_ASSERT_EQUAL(entries[0].Name, "..");
    }
    
    Y_UNIT_TEST(Sort_OnlyDirectories) {
        TVector<TTestEntry> entries = {
            CreateDirEntry("z"),
            CreateDirEntry("a"),
            CreateDirEntry("m"),
        };
        
        SortTestEntries(entries, 0, true);
        
        UNIT_ASSERT_EQUAL(entries[0].Name, "a");
        UNIT_ASSERT_EQUAL(entries[1].Name, "m");
        UNIT_ASSERT_EQUAL(entries[2].Name, "z");
    }
    
    Y_UNIT_TEST(Sort_OnlyTopics) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("z"),
            CreateTopicEntry("a"),
            CreateTopicEntry("m"),
        };
        
        SortTestEntries(entries, 0, true);
        
        UNIT_ASSERT_EQUAL(entries[0].Name, "a");
        UNIT_ASSERT_EQUAL(entries[1].Name, "m");
        UNIT_ASSERT_EQUAL(entries[2].Name, "z");
    }
}

Y_UNIT_TEST_SUITE(ViewLogicFilteringTests) {
    
    // === Basic Filtering ===
    
    Y_UNIT_TEST(Filter_EmptyQuery_ReturnsAll) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("topic1"),
            CreateTopicEntry("topic2"),
            CreateDirEntry("dir1"),
        };
        
        auto indices = FilterEntriesByName(entries, "");
        
        UNIT_ASSERT_EQUAL(indices.size(), 3);
        UNIT_ASSERT_EQUAL(indices[0], 0);
        UNIT_ASSERT_EQUAL(indices[1], 1);
        UNIT_ASSERT_EQUAL(indices[2], 2);
    }
    
    Y_UNIT_TEST(Filter_ExactMatch) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("my-topic"),
            CreateTopicEntry("other"),
            CreateDirEntry("my-dir"),
        };
        
        auto indices = FilterEntriesByName(entries, "my-topic");
        
        UNIT_ASSERT_EQUAL(indices.size(), 1);
        UNIT_ASSERT_EQUAL(indices[0], 0);
    }
    
    Y_UNIT_TEST(Filter_SubstringMatch) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("my-topic"),
            CreateTopicEntry("another-topic"),
            CreateDirEntry("not-matching"),
        };
        
        auto indices = FilterEntriesByName(entries, "topic");
        
        UNIT_ASSERT_EQUAL(indices.size(), 2);
        UNIT_ASSERT_EQUAL(indices[0], 0);  // my-topic
        UNIT_ASSERT_EQUAL(indices[1], 1);  // another-topic
    }
    
    // === Case Insensitivity ===
    
    Y_UNIT_TEST(Filter_CaseInsensitive_LowerQuery) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("MyTopic"),
            CreateTopicEntry("MYTOPIC2"),
            CreateTopicEntry("other"),
        };
        
        auto indices = FilterEntriesByName(entries, "mytopic");
        
        UNIT_ASSERT_EQUAL(indices.size(), 2);
    }
    
    Y_UNIT_TEST(Filter_CaseInsensitive_UpperQuery) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("mytopic"),
            CreateTopicEntry("MyTopic"),
            CreateTopicEntry("other"),
        };
        
        auto indices = FilterEntriesByName(entries, "MYTOPIC");
        
        UNIT_ASSERT_EQUAL(indices.size(), 2);
    }
    
    Y_UNIT_TEST(Filter_CaseInsensitive_MixedQuery) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("MyTopic"),
            CreateTopicEntry("MYTOPIC"),
            CreateTopicEntry("mytopic"),
        };
        
        auto indices = FilterEntriesByName(entries, "MyToPiC");
        
        UNIT_ASSERT_EQUAL(indices.size(), 3);
    }
    
    // === No Matches ===
    
    Y_UNIT_TEST(Filter_NoMatches) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("topic1"),
            CreateTopicEntry("topic2"),
        };
        
        auto indices = FilterEntriesByName(entries, "xyz");
        
        UNIT_ASSERT(indices.empty());
    }
    
    // === Edge Cases ===
    
    Y_UNIT_TEST(Filter_EmptyEntries) {
        TVector<TTestEntry> entries;
        
        auto indices = FilterEntriesByName(entries, "query");
        
        UNIT_ASSERT(indices.empty());
    }
    
    Y_UNIT_TEST(Filter_SingleChar) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("abc"),
            CreateTopicEntry("xyz"),
            CreateTopicEntry("bcd"),
        };
        
        auto indices = FilterEntriesByName(entries, "b");
        
        UNIT_ASSERT_EQUAL(indices.size(), 2);  // abc and bcd
    }
    
    Y_UNIT_TEST(Filter_IncludesParent) {
        TVector<TTestEntry> entries = {
            CreateParentEntry(),
            CreateTopicEntry("topic"),
        };
        
        auto indices = FilterEntriesByName(entries, "..");
        
        UNIT_ASSERT_EQUAL(indices.size(), 1);
        UNIT_ASSERT_EQUAL(indices[0], 0);
    }
    
    Y_UNIT_TEST(Filter_IncludesDirectories) {
        TVector<TTestEntry> entries = {
            CreateDirEntry("my-dir"),
            CreateTopicEntry("my-topic"),
        };
        
        auto indices = FilterEntriesByName(entries, "my");
        
        UNIT_ASSERT_EQUAL(indices.size(), 2);
    }
    
    Y_UNIT_TEST(Filter_MiddleOfString) {
        TVector<TTestEntry> entries = {
            CreateTopicEntry("prefix-match-suffix"),
            CreateTopicEntry("just-prefix"),
            CreateTopicEntry("suffix-only"),
        };
        
        auto indices = FilterEntriesByName(entries, "match");
        
        UNIT_ASSERT_EQUAL(indices.size(), 1);
        UNIT_ASSERT_EQUAL(indices[0], 0);
    }
}

Y_UNIT_TEST_SUITE(ViewLogicHelperTests) {
    
    // === MatchesPrefix ===
    
    Y_UNIT_TEST(MatchesPrefix_EmptyPrefix) {
        UNIT_ASSERT(MatchesPrefix("anything", ""));
    }
    
    Y_UNIT_TEST(MatchesPrefix_ExactMatch) {
        UNIT_ASSERT(MatchesPrefix("hello", "hello"));
    }
    
    Y_UNIT_TEST(MatchesPrefix_PrefixMatch) {
        UNIT_ASSERT(MatchesPrefix("hello-world", "hello"));
    }
    
    Y_UNIT_TEST(MatchesPrefix_CaseInsensitive) {
        UNIT_ASSERT(MatchesPrefix("Hello", "HELLO"));
        UNIT_ASSERT(MatchesPrefix("HELLO", "hello"));
        UNIT_ASSERT(MatchesPrefix("HeLLo", "hElLo"));
    }
    
    Y_UNIT_TEST(MatchesPrefix_NoMatch) {
        UNIT_ASSERT(!MatchesPrefix("hello", "world"));
    }
    
    Y_UNIT_TEST(MatchesPrefix_PrefixLongerThanText) {
        UNIT_ASSERT(!MatchesPrefix("hi", "hello"));
    }
    
    // === Factory Functions ===
    
    Y_UNIT_TEST(Factory_CreateParentEntry) {
        auto entry = CreateParentEntry();
        UNIT_ASSERT_EQUAL(entry.Name, "..");
        UNIT_ASSERT(entry.IsDirectory);
        UNIT_ASSERT(!entry.IsTopic);
    }
    
    Y_UNIT_TEST(Factory_CreateDirEntry) {
        auto entry = CreateDirEntry("my-dir", 10);
        UNIT_ASSERT_EQUAL(entry.Name, "my-dir");
        UNIT_ASSERT(entry.IsDirectory);
        UNIT_ASSERT(!entry.IsTopic);
        UNIT_ASSERT_EQUAL(entry.ChildCount, 10);
    }
    
    Y_UNIT_TEST(Factory_CreateTopicEntry) {
        auto entry = CreateTopicEntry("my-topic", 5, 3);
        UNIT_ASSERT_EQUAL(entry.Name, "my-topic");
        UNIT_ASSERT(!entry.IsDirectory);
        UNIT_ASSERT(entry.IsTopic);
        UNIT_ASSERT_EQUAL(entry.PartitionCount, 5);
        UNIT_ASSERT_EQUAL(entry.ConsumerCount, 3);
    }
    
    Y_UNIT_TEST(Factory_CreateTopicWithSize) {
        auto entry = CreateTopicWithSize("sized", 12345);
        UNIT_ASSERT_EQUAL(entry.TotalSizeBytes, 12345);
    }
    
    Y_UNIT_TEST(Factory_CreateTopicWithRetention) {
        auto entry = CreateTopicWithRetention("retained", TDuration::Days(7));
        UNIT_ASSERT_EQUAL(entry.RetentionPeriod, TDuration::Days(7));
    }
    
    Y_UNIT_TEST(Factory_CreateTopicWithWriteSpeed) {
        auto entry = CreateTopicWithWriteSpeed("speedy", 1000000);
        UNIT_ASSERT_EQUAL(entry.WriteSpeedBytesPerSec, 1000000);
    }
}
