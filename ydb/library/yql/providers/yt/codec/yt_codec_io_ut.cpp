#include "yt_codec_io.h"

#include <ydb/library/yql/providers/yt/lib/yson_helpers/yson_helpers.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_mem_info.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <library/cpp/yson/node/node_visitor.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/buffer.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/buffer.h>
#include <util/generic/ylimits.h>

using namespace NYql;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

namespace {

class TTestInput: public NYT::TRawTableReader {
public:
    TTestInput(const NYT::TNode::TListType& records, size_t fails, size_t retries, bool failAtStart, bool omitLastSemicolon)
        : Records(records)
        , Fails(fails)
        , Retries(retries)
        , FailAtStart(failAtStart)
        , OmitLastSemicolon(omitLastSemicolon)
    {
        Prepare();
    }

    bool Retry(const TMaybe<ui32>& /*rangeIndex*/, const TMaybe<ui64>& rowIndex, const std::exception_ptr& /*error*/) override {
        if (0 == Retries) {
            return false;
        }
        --Retries;
        Prepare(rowIndex.GetOrElse(0));
        CurrentOffset = 0;
        return true;
    }

    void ResetRetries() override {
    }

    // Returns 'true' if the input stream may contain table ranges.
    // The TRawTableReader user is responsible to track active range index in this case
    // in order to pass it to Retry().
    bool HasRangeIndices() const override {
        return false;
    }

protected:
    void Prepare(size_t fromRowIndex = 0) {
        Y_ABORT_UNLESS(fromRowIndex < Records.size());
        AtStart = true;
        Data.Buffer().Clear();
        Data.Rewind();
        TBinaryYsonWriter writer(&Data);
        NYT::TNodeVisitor visitor(&writer);
        auto entity = NYT::TNode::CreateEntity();
        entity.Attributes()("row_index", i64(fromRowIndex));
        visitor.Visit(entity);
        for (size_t i = fromRowIndex; i < Records.size(); ++i) {
            Data.Write(';');
            visitor.Visit(Records[i]);
        }
        if (!OmitLastSemicolon) {
            Data.Write(';');
        }
    }

    size_t DoRead(void* buf, size_t len) override {
        size_t read = Data.Read(buf, len);
        if (AtStart == FailAtStart && Fails > 0 && read > 0) {
            --Fails;
            throw yexception() << "Fail";
        }
        AtStart = false;
        return read;
    }

private:
    const NYT::TNode::TListType& Records;
    TBufferStream Data;
    size_t Fails;
    size_t Retries;
    const bool FailAtStart;
    size_t CurrentOffset = 0;
    bool AtStart = true;
    bool OmitLastSemicolon = false;
};

struct TMkqlCodecFixture {
    TIntrusivePtr<IFunctionRegistry> FunctionRegistry;
    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    NCommon::TCodecContext CodecCtx;
    TMkqlIOSpecs Specs;

    static const TString INPUT_SPEC;
    static const TString VALUE;

    TMkqlCodecFixture()
        : FunctionRegistry(CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr()))
        , Alloc(__LOCATION__)
        , Env(Alloc)
        , MemInfo("Test")
        , HolderFactory(Alloc.Ref(), MemInfo, FunctionRegistry.Get())
        , CodecCtx(Env, *FunctionRegistry, &HolderFactory)
    {
        Specs.Init(CodecCtx, INPUT_SPEC, {}, Nothing());
    }

    static TVector<NYT::TNode> Generate(size_t numRecords) {
        TVector<NYT::TNode> data;
        for (size_t i = 0; i < numRecords; i++) {
            data.push_back(NYT::TNode()("key", i)("value", VALUE));
        }
        return data;
    }

    static void Validate(size_t rowIndex, const NUdf::TUnboxedValue& row) {
        UNIT_ASSERT_VALUES_EQUAL(row.GetElement(0).Get<ui64>(), rowIndex + 1);
        UNIT_ASSERT_VALUES_EQUAL(row.GetElement(1).Get<ui64>(), rowIndex);
        auto value = row.GetElement(2);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(value.AsStringRef()), VALUE);
    }
};

const TString TMkqlCodecFixture::INPUT_SPEC = R"({
    tables = [{
        "_yql_row_spec" = {
            "Type" = [
                "StructType"; [
                    ["key"; ["DataType"; "Uint64"]];
                    ["value"; ["DataType"; "String"]]
                ]
            ]
        };
        "_yql_sys_table" = [
            "record"
        ]
    }]
})";

const TString TMkqlCodecFixture::VALUE = TString().append(100, 'z');

} // unnamed

Y_UNIT_TEST_SUITE(TMkqlCodec) {

    void TestRead(size_t blockCount) {
        TMkqlCodecFixture fixture;
        auto data = TMkqlCodecFixture::Generate(10);
        // With semicolon in end of input
        {
            TTestInput input(data, 0, 0, false, false);
            TMkqlReaderImpl reader(input, blockCount, TMkqlCodecFixture::VALUE.size());
            reader.SetSpecs(fixture.Specs, fixture.HolderFactory);
            reader.Next();

            for (size_t i = 0; i < 10; reader.Next(), i++) {
                UNIT_ASSERT(reader.IsValid());
                TMkqlCodecFixture::Validate(i, reader.GetRow());
            }
            UNIT_ASSERT(!reader.IsValid());
        }
        // Without semicolon in end of input
        {
            TTestInput input(data, 0, 0, false, true);
            TMkqlReaderImpl reader(input, blockCount, TMkqlCodecFixture::VALUE.size());
            reader.SetSpecs(fixture.Specs, fixture.HolderFactory);
            reader.Next();

            for (size_t i = 0; i < 10; reader.Next(), i++) {
                UNIT_ASSERT(reader.IsValid());
                TMkqlCodecFixture::Validate(i, reader.GetRow());
            }
            UNIT_ASSERT(!reader.IsValid());
        }
    }

    Y_UNIT_TEST(ReadSync) {
        TestRead(0);
    }

    Y_UNIT_TEST(ReadAsync) {
        TestRead(4);
    }

    void TestReadFail(size_t blockCount) {
        TMkqlCodecFixture fixture;
        auto data = TMkqlCodecFixture::Generate(10);
        // Before first record
        {
            TTestInput input(data, 1, 0, true, false);
            TMkqlReaderImpl reader(input, blockCount, TMkqlCodecFixture::VALUE.size());
            reader.SetSpecs(fixture.Specs, fixture.HolderFactory);
            UNIT_ASSERT_EXCEPTION(reader.Next(), yexception);
        }
        // In the middle
        {
            TTestInput input(data, 1, 0, false, false);
            TMkqlReaderImpl reader(input, blockCount, 2 * TMkqlCodecFixture::VALUE.size());
            reader.SetSpecs(fixture.Specs, fixture.HolderFactory);
            reader.Next();
            UNIT_ASSERT(reader.IsValid());
            UNIT_ASSERT_EXCEPTION(reader.Next(), yexception);
        }
    }

    Y_UNIT_TEST(ReadSyncFail) {
        TestReadFail(0);
    }

    Y_UNIT_TEST(ReadAsyncFail) {
        TestReadFail(4);
    }

    void TestReadRetry(size_t blockCount) {
        TMkqlCodecFixture fixture;
        auto data = TMkqlCodecFixture::Generate(10);
        // Before first record
        {
            TTestInput input(data, 1, 1, true, false);
            TMkqlReaderImpl reader(input, blockCount, TMkqlCodecFixture::VALUE.size());
            reader.SetSpecs(fixture.Specs, fixture.HolderFactory);
            reader.Next();

            for (size_t i = 0; i < 10; reader.Next(), i++) {
                UNIT_ASSERT(reader.IsValid());
                TMkqlCodecFixture::Validate(i, reader.GetRow());
            }
            UNIT_ASSERT(!reader.IsValid());
        }
        // In the middle
        {
            TTestInput input(data, 3, 3, false, false);
            TMkqlReaderImpl reader(input, blockCount, TMkqlCodecFixture::VALUE.size());
            reader.SetSpecs(fixture.Specs, fixture.HolderFactory);
            reader.Next();

            for (size_t i = 0; i < 10; reader.Next(), i++) {
                UNIT_ASSERT(reader.IsValid());
                TMkqlCodecFixture::Validate(i, reader.GetRow());
            }
            UNIT_ASSERT(!reader.IsValid());
        }
        // Fail through time
        {
            TTestInput input(data, Max(), Max(), false, false);
            TMkqlReaderImpl reader(input, blockCount, 2 * TMkqlCodecFixture::VALUE.size());
            reader.SetSpecs(fixture.Specs, fixture.HolderFactory);
            reader.Next();

            for (size_t i = 0; i < 10; reader.Next(), i++) {
                UNIT_ASSERT(reader.IsValid());
                TMkqlCodecFixture::Validate(i, reader.GetRow());
            }
            UNIT_ASSERT(!reader.IsValid());
        }
        // Small buffer
        {
            TTestInput input(data, blockCount + 1, blockCount + 1, false, false);
            TMkqlReaderImpl reader(input, blockCount, TMkqlCodecFixture::VALUE.size() / 2);
            reader.SetSpecs(fixture.Specs, fixture.HolderFactory);
            reader.Next();

            for (size_t i = 0; i < 10; reader.Next(), i++) {
                UNIT_ASSERT(reader.IsValid());
                TMkqlCodecFixture::Validate(i, reader.GetRow());
            }
            UNIT_ASSERT(!reader.IsValid());
        }
        // Large buffer
        {
            TTestInput input(data, blockCount + 1, blockCount + 1, false, false);
            TMkqlReaderImpl reader(input, blockCount, TMkqlCodecFixture::VALUE.size() * 2);
            reader.SetSpecs(fixture.Specs, fixture.HolderFactory);
            reader.Next();

            for (size_t i = 0; i < 10; reader.Next(), i++) {
                UNIT_ASSERT(reader.IsValid());
                TMkqlCodecFixture::Validate(i, reader.GetRow());
            }
            UNIT_ASSERT(!reader.IsValid());
        }
    }

    Y_UNIT_TEST(ReadSyncRetry) {
        TestReadRetry(0);
    }

    Y_UNIT_TEST(ReadAsyncRetry) {
        TestReadRetry(4);
    }

}
