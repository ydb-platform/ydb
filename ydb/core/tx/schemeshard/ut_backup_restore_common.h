#include <ydb/core/tx/datashard/backup_restore_traits.h>

using EDataFormat = NKikimr::NDataShard::NBackupRestoreTraits::EDataFormat;
using ECompressionCodec = NKikimr::NDataShard::NBackupRestoreTraits::ECompressionCodec;

#define Y_UNIT_TEST_WITH_COMPRESSION(N)                                                                                               \
    template<ECompressionCodec Codec> void N(NUnitTest::TTestContext&);                                                               \
    struct TTestRegistration##N {                                                                                                     \
        TTestRegistration##N() {                                                                                                      \
            TCurrentTest::AddTest(#N "[Raw]",  static_cast<void (*)(NUnitTest::TTestContext&)>(&N<ECompressionCodec::None>), false);  \
            TCurrentTest::AddTest(#N "[Zstd]", static_cast<void (*)(NUnitTest::TTestContext&)>(&N<ECompressionCodec::Zstd>),  false); \
        }                                                                                                                             \
    };                                                                                                                                \
    static TTestRegistration##N testRegistration##N;                                                                                  \
    template<ECompressionCodec Codec>                                                                                                 \
    void N(NUnitTest::TTestContext&)
