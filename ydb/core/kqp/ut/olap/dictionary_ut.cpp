#include "combinatory/variator.h"
#include "helpers/get_value.h"
#include "helpers/local.h"
#include "helpers/query_executor.h"
#include "helpers/typed_local.h"
#include "helpers/writer.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/limiter/grouped_memory/service/process.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <ydb/library/signals/object_counter.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/strip.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapDictionary) {
    Y_UNIT_TEST(EmptyStringVariants) {
        TString script = R"(
            SCHEMA:            
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 Utf8,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`DICTIONARY`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col1, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`DICTIONARY`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1) VALUES (1u)
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;#]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(SimpleStringVariants) {
        TString script = R"(
            STOP_COMPACTION
            ------
            SCHEMA:            
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 Utf8,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2$$);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`DICTIONARY`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col1, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`DICTIONARY`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (1u, 'abc')
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1) VALUES (2u)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (3u, 'abc')
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (4u, 'ab')
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;["abc"]];[2u;#];[3u;["abc"]];[4u;["ab"]]]
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;["abc"]];[2u;#];[3u;["abc"]];[4u;["ab"]]]
            ------
            ONE_COMPACTION
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;["abc"]];[2u;#];[3u;["abc"]];[4u;["ab"]]]
        )";
        TScriptVariator(script).Execute();
    }
}

}   // namespace NKikimr::NKqp
