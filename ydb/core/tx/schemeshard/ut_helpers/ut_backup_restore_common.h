#include <ydb/core/protos/flat_scheme_op.pb.h>
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

struct TTypedScheme {
    NKikimrSchemeOp::EPathType Type;
    TString Scheme;

    TTypedScheme(const char* scheme)
        : Type(NKikimrSchemeOp::EPathTypeTable)
        , Scheme(scheme)
    {}

    TTypedScheme(const TString& scheme)
        : Type(NKikimrSchemeOp::EPathTypeTable)
        , Scheme(scheme)
    {}

    TTypedScheme(NKikimrSchemeOp::EPathType type, TString scheme)
        : Type(type)
        , Scheme(std::move(scheme))
    {}
};
