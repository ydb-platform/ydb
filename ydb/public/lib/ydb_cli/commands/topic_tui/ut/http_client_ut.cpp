#include "../http_client.h"
#include "../widgets/table.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(HttpClientStructsTests) {
    
    // === TTopicMessage Tests ===
    
    Y_UNIT_TEST(TTopicMessage_DefaultConstructor) {
        TTopicMessage msg;
        UNIT_ASSERT_EQUAL(msg.Offset, 0);
        UNIT_ASSERT_EQUAL(msg.SeqNo, 0);
        UNIT_ASSERT_EQUAL(msg.StorageSize, 0);
        UNIT_ASSERT_EQUAL(msg.OriginalSize, 0);
        UNIT_ASSERT_EQUAL(msg.Codec, 0);
        UNIT_ASSERT(msg.ProducerId.empty());
        UNIT_ASSERT(msg.Data.empty());
    }
    
    Y_UNIT_TEST(TTopicMessage_FieldAssignment) {
        TTopicMessage msg;
        msg.Offset = 12345;
        msg.SeqNo = 100;
        msg.ProducerId = "producer-1";
        msg.Data = "test message data";
        msg.StorageSize = 17;
        msg.OriginalSize = 100;
        msg.Codec = 1;  // GZIP
        
        UNIT_ASSERT_EQUAL(msg.Offset, 12345);
        UNIT_ASSERT_EQUAL(msg.SeqNo, 100);
        UNIT_ASSERT_STRINGS_EQUAL(msg.ProducerId.c_str(), "producer-1");
        UNIT_ASSERT_STRINGS_EQUAL(msg.Data.c_str(), "test message data");
        UNIT_ASSERT_EQUAL(msg.StorageSize, 17);
        UNIT_ASSERT_EQUAL(msg.OriginalSize, 100);
        UNIT_ASSERT_EQUAL(msg.Codec, 1);
    }
    
    // === TTabletInfo Tests ===
    
    Y_UNIT_TEST(TTabletInfo_DefaultConstructor) {
        TTabletInfo tablet;
        UNIT_ASSERT_EQUAL(tablet.TabletId, 0);
        UNIT_ASSERT_EQUAL(tablet.NodeId, 0);
        UNIT_ASSERT_EQUAL(tablet.Generation, 0);
        UNIT_ASSERT_EQUAL(tablet.HiveId, 0);
        UNIT_ASSERT(!tablet.Leader);
        UNIT_ASSERT(tablet.Type.empty());
        UNIT_ASSERT(tablet.State.empty());
    }
    
    Y_UNIT_TEST(TTabletInfo_FieldAssignment) {
        TTabletInfo tablet;
        tablet.TabletId = 12345678901234ULL;
        tablet.Type = "PersQueue";
        tablet.State = "Active";
        tablet.NodeId = 50004;
        tablet.Generation = 42;
        tablet.Leader = true;
        tablet.Overall = "Green";
        
        UNIT_ASSERT_EQUAL(tablet.TabletId, 12345678901234ULL);
        UNIT_ASSERT_STRINGS_EQUAL(tablet.Type.c_str(), "PersQueue");
        UNIT_ASSERT_STRINGS_EQUAL(tablet.State.c_str(), "Active");
        UNIT_ASSERT_EQUAL(tablet.NodeId, 50004);
        UNIT_ASSERT_EQUAL(tablet.Generation, 42);
        UNIT_ASSERT(tablet.Leader);
        UNIT_ASSERT_STRINGS_EQUAL(tablet.Overall.c_str(), "Green");
    }
    
    // === TTopicDescribeResult Tests ===
    
    Y_UNIT_TEST(TTopicDescribeResult_DefaultConstructor) {
        TTopicDescribeResult result;
        UNIT_ASSERT_EQUAL(result.PathId, 0);
        UNIT_ASSERT_EQUAL(result.SchemeshardId, 0);
        UNIT_ASSERT_EQUAL(result.PartitionsCount, 0);
        UNIT_ASSERT_EQUAL(result.RetentionSeconds, 0);
        UNIT_ASSERT_EQUAL(result.RetentionBytes, 0);
        UNIT_ASSERT_EQUAL(result.WriteSpeedBytesPerSec, 0);
        UNIT_ASSERT(result.Path.empty());
        UNIT_ASSERT(result.Error.empty());
        UNIT_ASSERT(result.Tablets.empty());
        UNIT_ASSERT(result.SupportedCodecs.empty());
    }
    
    Y_UNIT_TEST(TTopicDescribeResult_FieldAssignment) {
        TTopicDescribeResult result;
        result.Path = "/Root/my-topic";
        result.Owner = "user@domain";
        result.PathType = "Topic";
        result.PartitionsCount = 10;
        result.RetentionSeconds = 86400;
        result.WriteSpeedBytesPerSec = 1024 * 1024;
        result.SupportedCodecs = {"RAW", "GZIP", "ZSTD"};
        result.MeteringMode = "REQUEST_UNITS";
        
        UNIT_ASSERT_STRINGS_EQUAL(result.Path.c_str(), "/Root/my-topic");
        UNIT_ASSERT_STRINGS_EQUAL(result.Owner.c_str(), "user@domain");
        UNIT_ASSERT_EQUAL(result.PartitionsCount, 10);
        UNIT_ASSERT_EQUAL(result.RetentionSeconds, 86400);
        UNIT_ASSERT_EQUAL(result.WriteSpeedBytesPerSec, 1024 * 1024);
        UNIT_ASSERT_EQUAL(result.SupportedCodecs.size(), 3);
        UNIT_ASSERT_STRINGS_EQUAL(result.SupportedCodecs[0].c_str(), "RAW");
    }
    
    Y_UNIT_TEST(TTopicDescribeResult_TabletsList) {
        TTopicDescribeResult result;
        
        TTabletInfo tablet1;
        tablet1.TabletId = 1;
        tablet1.Type = "PersQueue";
        
        TTabletInfo tablet2;
        tablet2.TabletId = 2;
        tablet2.Type = "PersQueueReadBalancer";
        
        result.Tablets.push_back(tablet1);
        result.Tablets.push_back(tablet2);
        
        UNIT_ASSERT_EQUAL(result.Tablets.size(), 2);
        UNIT_ASSERT_EQUAL(result.Tablets[0].TabletId, 1);
        UNIT_ASSERT_EQUAL(result.Tablets[1].TabletId, 2);
    }
}

Y_UNIT_TEST_SUITE(TTableColumnTests) {
    
    Y_UNIT_TEST(TTableColumn_BasicConstructor) {
        TTableColumn col("Name", 20);
        UNIT_ASSERT_STRINGS_EQUAL(col.Header.c_str(), "Name");
        UNIT_ASSERT_EQUAL(col.Width, 20);
        UNIT_ASSERT_EQUAL(col.Align, TTableColumn::Left);
    }
    
    Y_UNIT_TEST(TTableColumn_FlexWidth) {
        TTableColumn col("Flexible", -1);  // -1 means flex
        UNIT_ASSERT_EQUAL(col.Width, -1);
    }
    
    Y_UNIT_TEST(TTableColumn_WithAlignment) {
        TTableColumn col("Amount", 10, TTableColumn::Right);
        UNIT_ASSERT_EQUAL(col.Width, 10);
        UNIT_ASSERT_EQUAL(col.Align, TTableColumn::Right);
    }
    
    Y_UNIT_TEST(TTableColumn_CenterAlignment) {
        TTableColumn col("Status", 8, TTableColumn::Center);
        UNIT_ASSERT_EQUAL(col.Align, TTableColumn::Center);
    }
    
    Y_UNIT_TEST(TTableColumn_ZeroWidth) {
        TTableColumn col("Zero", 0);
        UNIT_ASSERT_EQUAL(col.Width, 0);
    }
}

Y_UNIT_TEST_SUITE(MoreTableTests) {
    
    static TVector<TTableColumn> CreateTestColumns() {
        return {
            {"Name", -1},
            {"Value", 10},
            {"Status", 8}
        };
    }
    
    // === Additional TTable Tests ===
    
    Y_UNIT_TEST(Table_MultipleRowOperations) {
        TTable table(CreateTestColumns());
        table.SetRowCount(3);
        
        table.SetCell(0, 0, "row0");
        table.SetCell(1, 0, "row1");
        table.SetCell(2, 0, "row2");
        
        UNIT_ASSERT_STRINGS_EQUAL(table.GetRow(0).Cells[0].Text.c_str(), "row0");
        UNIT_ASSERT_STRINGS_EQUAL(table.GetRow(1).Cells[0].Text.c_str(), "row1");
        UNIT_ASSERT_STRINGS_EQUAL(table.GetRow(2).Cells[0].Text.c_str(), "row2");
    }
    
    Y_UNIT_TEST(Table_SetRowCountPreservesCells) {
        TTable table(CreateTestColumns());
        table.SetRowCount(3);
        table.SetCell(0, 0, "preserved");
        
        // Adding more rows shouldn't affect existing
        table.SetRowCount(5);
        
        UNIT_ASSERT_STRINGS_EQUAL(table.GetRow(0).Cells[0].Text.c_str(), "preserved");
    }
    
    Y_UNIT_TEST(Table_ConstGetRow) {
        TTable table(CreateTestColumns());
        table.SetRowCount(1);
        table.SetCell(0, 0, "test");
        
        const TTable& constTable = table;
        const TTableRow& row = constTable.GetRow(0);
        
        UNIT_ASSERT_STRINGS_EQUAL(row.Cells[0].Text.c_str(), "test");
    }
    
    Y_UNIT_TEST(Table_HandleEvent_PageDown) {
        TTable table(CreateTestColumns());
        table.SetRowCount(20);
        table.SetFocused(true);
        table.SetSelectedRow(5);
        
        // PageDown should jump multiple rows
        bool handled = table.HandleEvent(ftxui::Event::PageDown);
        
        UNIT_ASSERT(handled);
        UNIT_ASSERT(table.GetSelectedRow() > 5);
    }
    
    Y_UNIT_TEST(Table_HandleEvent_PageUp) {
        TTable table(CreateTestColumns());
        table.SetRowCount(20);
        table.SetFocused(true);
        table.SetSelectedRow(15);
        
        bool handled = table.HandleEvent(ftxui::Event::PageUp);
        
        UNIT_ASSERT(handled);
        UNIT_ASSERT(table.GetSelectedRow() < 15);
    }
    
    Y_UNIT_TEST(Table_HandleEvent_Home) {
        TTable table(CreateTestColumns());
        table.SetRowCount(20);
        table.SetFocused(true);
        table.SetSelectedRow(10);
        
        bool handled = table.HandleEvent(ftxui::Event::Home);
        
        UNIT_ASSERT(handled);
        UNIT_ASSERT_EQUAL(table.GetSelectedRow(), 0);
    }
    
    Y_UNIT_TEST(Table_HandleEvent_End) {
        TTable table(CreateTestColumns());
        table.SetRowCount(20);
        table.SetFocused(true);
        table.SetSelectedRow(5);
        
        bool handled = table.HandleEvent(ftxui::Event::End);
        
        UNIT_ASSERT(handled);
        UNIT_ASSERT_EQUAL(table.GetSelectedRow(), 19);
    }
    
    Y_UNIT_TEST(Table_HandleEvent_VimKeys_j) {
        TTable table(CreateTestColumns());
        table.SetRowCount(5);
        table.SetFocused(true);
        table.SetSelectedRow(0);
        
        bool handled = table.HandleEvent(ftxui::Event::Character('j'));
        
        UNIT_ASSERT(handled);
        UNIT_ASSERT_EQUAL(table.GetSelectedRow(), 1);
    }
    
    Y_UNIT_TEST(Table_HandleEvent_VimKeys_k) {
        TTable table(CreateTestColumns());
        table.SetRowCount(5);
        table.SetFocused(true);
        table.SetSelectedRow(3);
        
        bool handled = table.HandleEvent(ftxui::Event::Character('k'));
        
        UNIT_ASSERT(handled);
        UNIT_ASSERT_EQUAL(table.GetSelectedRow(), 2);
    }
    
    Y_UNIT_TEST(Table_HandleEvent_SortKeys_LessThan) {
        TTable table(CreateTestColumns());
        table.SetRowCount(5);
        table.SetFocused(true);
        
        int callbackCol = -1;
        table.OnSortChanged = [&](int col, bool) { callbackCol = col; };
        
        table.SetSort(1, true);  // Start at column 1
        bool handled = table.HandleEvent(ftxui::Event::Character('<'));
        
        UNIT_ASSERT(handled);
        UNIT_ASSERT_EQUAL(table.GetSortColumn(), 0);  // Should move to column 0
    }
    
    Y_UNIT_TEST(Table_HandleEvent_SortKeys_GreaterThan) {
        TTable table(CreateTestColumns());
        table.SetRowCount(5);
        table.SetFocused(true);
        
        table.SetSort(0, true);  // Start at column 0
        bool handled = table.HandleEvent(ftxui::Event::Character('>'));
        
        UNIT_ASSERT(handled);
        UNIT_ASSERT_EQUAL(table.GetSortColumn(), 1);  // Should move to column 1
    }
}
