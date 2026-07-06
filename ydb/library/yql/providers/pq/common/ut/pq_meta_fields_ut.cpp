#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TPqMetaFieldsTest) {
    Y_UNIT_TEST(SkipYdbSystemPrefix) {
        {
            auto result = SkipYdbSystemPrefix("__ydb_write_time");
            UNIT_ASSERT(result.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*result, "write_time");
        }
        {
            auto result = SkipYdbSystemPrefix("__ydb_partition_id");
            UNIT_ASSERT(result.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*result, "partition_id");
        }
        {
            auto result = SkipYdbSystemPrefix("__ydb_offset");
            UNIT_ASSERT(result.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*result, "offset");
        }
        {
            auto result = SkipYdbSystemPrefix("_yql_sys_write_time");
            UNIT_ASSERT(!result.has_value());
        }
        {
            auto result = SkipYdbSystemPrefix("some_column");
            UNIT_ASSERT(!result.has_value());
        }
    }

    Y_UNIT_TEST(GetPqMetaFieldDescriptorByYdbSysColumn) {
        {
            auto desc = GetPqMetaFieldDescriptorByYdbSysColumn("__ydb_write_time", false);
            UNIT_ASSERT(desc.has_value());
            UNIT_ASSERT_VALUES_EQUAL(desc->Key, "write_time");
            UNIT_ASSERT_VALUES_EQUAL(desc->SysColumn, "__ydb_write_time");
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(desc->Type), static_cast<int>(EMetaFieldType::Timestamp));
        }
        {
            auto desc = GetPqMetaFieldDescriptorByYdbSysColumn("__ydb_partition_id", false);
            UNIT_ASSERT(desc.has_value());
            UNIT_ASSERT_VALUES_EQUAL(desc->Key, "partition_id");
            UNIT_ASSERT_VALUES_EQUAL(desc->SysColumn, "__ydb_partition_id");
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(desc->Type), static_cast<int>(EMetaFieldType::Uint64));
        }
        {
            auto desc = GetPqMetaFieldDescriptorByYdbSysColumn("__ydb_offset", false);
            UNIT_ASSERT(desc.has_value());
            UNIT_ASSERT_VALUES_EQUAL(desc->Key, "offset");
            UNIT_ASSERT_VALUES_EQUAL(desc->SysColumn, "__ydb_offset");
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(desc->Type), static_cast<int>(EMetaFieldType::Uint64));
        }
        {
            auto desc = GetPqMetaFieldDescriptorByYdbSysColumn("__ydb_create_time", false);
            UNIT_ASSERT(desc.has_value());
            UNIT_ASSERT_VALUES_EQUAL(desc->Key, "create_time");
            UNIT_ASSERT_VALUES_EQUAL(desc->SysColumn, "__ydb_create_time");
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(desc->Type), static_cast<int>(EMetaFieldType::Timestamp));
        }
        {
            auto desc = GetPqMetaFieldDescriptorByYdbSysColumn("__ydb_message_group_id", false);
            UNIT_ASSERT(desc.has_value());
            UNIT_ASSERT_VALUES_EQUAL(desc->Key, "message_group_id");
            UNIT_ASSERT_VALUES_EQUAL(desc->SysColumn, "__ydb_message_group_id");
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(desc->Type), static_cast<int>(EMetaFieldType::String));
        }
        {
            auto desc = GetPqMetaFieldDescriptorByYdbSysColumn("__ydb_seq_no", false);
            UNIT_ASSERT(desc.has_value());
            UNIT_ASSERT_VALUES_EQUAL(desc->Key, "seq_no");
            UNIT_ASSERT_VALUES_EQUAL(desc->SysColumn, "__ydb_seq_no");
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(desc->Type), static_cast<int>(EMetaFieldType::Uint64));
        }
        {
            auto desc = GetPqMetaFieldDescriptorByYdbSysColumn("__ydb_cluster", false);
            UNIT_ASSERT(desc.has_value());
            UNIT_ASSERT_VALUES_EQUAL(desc->Key, "cluster");
            UNIT_ASSERT_VALUES_EQUAL(desc->SysColumn, "__ydb_cluster");
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(desc->Type), static_cast<int>(EMetaFieldType::String));
        }
        {
            // user_attributes excluded by default
            auto desc = GetPqMetaFieldDescriptorByYdbSysColumn("__ydb_user_attributes", false);
            UNIT_ASSERT(!desc.has_value());
        }
        {
            // user_attributes included when enabled
            auto desc = GetPqMetaFieldDescriptorByYdbSysColumn("__ydb_user_attributes", true);
            UNIT_ASSERT(desc.has_value());
            UNIT_ASSERT_VALUES_EQUAL(desc->Key, "user_attributes");
            UNIT_ASSERT_VALUES_EQUAL(desc->SysColumn, "__ydb_user_attributes");
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(desc->Type), static_cast<int>(EMetaFieldType::DictStringString));
        }
        {
            // Unknown __ydb_ column
            auto desc = GetPqMetaFieldDescriptorByYdbSysColumn("__ydb_unknown_field", false);
            UNIT_ASSERT(!desc.has_value());
        }
        {
            // Not a __ydb_ column at all
            auto desc = GetPqMetaFieldDescriptorByYdbSysColumn("_yql_sys_write_time", false);
            UNIT_ASSERT(!desc.has_value());
        }
    }

    Y_UNIT_TEST(GetPqMetaFieldDescriptorBySysColumnHandlesYdbPrefix) {
        // GetPqMetaFieldDescriptorBySysColumn should also handle __ydb_ prefix
        {
            auto desc = GetPqMetaFieldDescriptorBySysColumn("__ydb_write_time", false);
            UNIT_ASSERT(desc.has_value());
            UNIT_ASSERT_VALUES_EQUAL(desc->Key, "write_time");
            UNIT_ASSERT_VALUES_EQUAL(desc->SysColumn, "__ydb_write_time");
        }
        {
            auto desc = GetPqMetaFieldDescriptorBySysColumn("__ydb_offset", false);
            UNIT_ASSERT(desc.has_value());
            UNIT_ASSERT_VALUES_EQUAL(desc->Key, "offset");
            UNIT_ASSERT_VALUES_EQUAL(desc->SysColumn, "__ydb_offset");
        }
        // Old prefix still works
        {
            auto desc = GetPqMetaFieldDescriptorBySysColumn("_yql_sys_write_time", false);
            UNIT_ASSERT(desc.has_value());
            UNIT_ASSERT_VALUES_EQUAL(desc->Key, "write_time");
            UNIT_ASSERT_VALUES_EQUAL(desc->SysColumn, "_yql_sys_write_time");
        }
        {
            auto desc = GetPqMetaFieldDescriptorBySysColumn("_yql_sys_tsp_write_time", false);
            UNIT_ASSERT(desc.has_value());
            UNIT_ASSERT_VALUES_EQUAL(desc->Key, "write_time");
            UNIT_ASSERT_VALUES_EQUAL(desc->SysColumn, "_yql_sys_tsp_write_time");
        }
    }

    Y_UNIT_TEST(GetAllowedYdbSysColumns) {
        auto columns = GetAllowedYdbSysColumns(false);
        UNIT_ASSERT(!columns.empty());

        // Check that all columns have __ydb_ prefix
        for (const auto& col : columns) {
            UNIT_ASSERT_C(col.StartsWith("__ydb_"), "Column should start with __ydb_: " + col);
        }

        // user_attributes should not be present
        for (const auto& col : columns) {
            UNIT_ASSERT_C(col != "__ydb_user_attributes", "user_attributes should not be in the list");
        }

        // Check known columns are present
        bool hasWriteTime = false;
        bool hasPartitionId = false;
        bool hasOffset = false;
        for (const auto& col : columns) {
            if (col == "__ydb_write_time") hasWriteTime = true;
            if (col == "__ydb_partition_id") hasPartitionId = true;
            if (col == "__ydb_offset") hasOffset = true;
        }
        UNIT_ASSERT(hasWriteTime);
        UNIT_ASSERT(hasPartitionId);
        UNIT_ASSERT(hasOffset);
    }

    Y_UNIT_TEST(GetAllowedYdbSysColumnsWithUserAttributes) {
        auto columns = GetAllowedYdbSysColumns(true);

        bool hasUserAttributes = false;
        for (const auto& col : columns) {
            if (col == "__ydb_user_attributes") hasUserAttributes = true;
        }
        UNIT_ASSERT(hasUserAttributes);
    }

    Y_UNIT_TEST(YdbSysColumnToOldSysColumn) {
        {
            // Non-transparent field
            auto result = YdbSysColumnToOldSysColumn("__ydb_offset", false);
            UNIT_ASSERT(result.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*result, "_yql_sys_offset");
        }
        {
            // Transparent field without transparent prefix
            auto result = YdbSysColumnToOldSysColumn("__ydb_write_time", false);
            UNIT_ASSERT(result.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*result, "_yql_sys_write_time");
        }
        {
            // Transparent field with transparent prefix
            auto result = YdbSysColumnToOldSysColumn("__ydb_write_time", true);
            UNIT_ASSERT(result.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*result, "_yql_sys_tsp_write_time");
        }
        {
            // Non-__ydb_ column returns nullopt
            auto result = YdbSysColumnToOldSysColumn("_yql_sys_offset", false);
            UNIT_ASSERT(!result.has_value());
        }
        {
            // Unknown __ydb_ field returns nullopt
            auto result = YdbSysColumnToOldSysColumn("__ydb_unknown", false);
            UNIT_ASSERT(!result.has_value());
        }
    }

    Y_UNIT_TEST(SkipPqSystemPrefixStillWorks) {
        {
            bool isTransparent = false;
            auto result = SkipPqSystemPrefix("_yql_sys_offset", &isTransparent);
            UNIT_ASSERT(result.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*result, "offset");
            UNIT_ASSERT(!isTransparent);
        }
        {
            bool isTransparent = false;
            auto result = SkipPqSystemPrefix("_yql_sys_tsp_write_time", &isTransparent);
            UNIT_ASSERT(result.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*result, "write_time");
            UNIT_ASSERT(isTransparent);
        }
        {
            // __ydb_ prefix is NOT handled by SkipPqSystemPrefix
            auto result = SkipPqSystemPrefix("__ydb_offset");
            UNIT_ASSERT(!result.has_value());
        }
    }
}

} // namespace NYql
