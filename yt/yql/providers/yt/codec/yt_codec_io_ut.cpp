#include "yt_codec_io.h"

#include <yt/yql/providers/yt/lib/yson_helpers/yson_helpers.h>
#include <yql/essentials/providers/common/codec/yql_codec.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_mem_info.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

#include <library/cpp/yson/node/node_visitor.h>
#include <library/cpp/yson/public.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/buffer.h>
#include <util/stream/str.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/generic/buffer.h>
#include <util/generic/ylimits.h>
#include <util/string/cast.h>

#include <cstring>
#include <utility>

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

class TSkiffTestInput: public NYT::TRawTableReader {
public:
    TSkiffTestInput(TStringBuf data)
        : Data(data)
    {
    }

    bool Retry(const TMaybe<ui32>& /*rangeIndex*/, const TMaybe<ui64>& /*rowIndex*/, const std::exception_ptr& /*error*/) override {
        return false;
    }

    void ResetRetries() override {
    }

    bool HasRangeIndices() const override {
        return false;
    }

protected:
    size_t DoRead(void* buf, size_t len) override {
        const size_t avail = Min(len, Data.size() - Pos);
        std::memcpy(buf, Data.data() + Pos, avail);
        Pos += avail;
        return avail;
    }

private:
    const TStringBuf Data;
    size_t Pos = 0;
};

struct TSkiffCodecFixture {
    TIntrusivePtr<IFunctionRegistry> FunctionRegistry;
    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    NCommon::TCodecContext CodecCtx;

    static const TString ROW_SPEC;
    static constexpr size_t BLOCK_SIZE = 1ULL << 20;

    struct TRow {
        bool Bool;
        i8 Int8;
        i32 Int32;
        ui32 Uint32;
        double Double;
        NDecimal::TInt128 Decimal;
        TString Str;
        TString Utf8;
        i32 NestedNum;
        TString NestedStr;
        TVector<i32> List;
    };

    TSkiffCodecFixture()
        : FunctionRegistry(CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr()))
        , Alloc(__LOCATION__)
        , Env(Alloc)
        , MemInfo("Test")
        , HolderFactory(Alloc.Ref(), MemInfo, FunctionRegistry.Get())
        , CodecCtx(Env, *FunctionRegistry, &HolderFactory)
    {
    }

    static TVector<TRow> Generate(size_t numRecords) {
        TVector<TRow> rows;
        for (size_t i = 0; i < numRecords; ++i) {
            TRow row;
            const i64 n = i64(i);
            row.Bool = i % 2 == 0;
            row.Int8 = i8(-100 + n);
            row.Int32 = i32(-2000000000 + n * 13);
            row.Uint32 = ui32(4000000000u + n);
            row.Double = double(i) / 7;
            row.Decimal = NDecimal::TInt128(n * 100 + 42);
            row.Str = TString().append(i * 7 % 40, char('a' + i % 26));
            row.Utf8 = TString("значение-") + ToString(i);
            row.NestedNum = i32(n * 3);
            row.NestedStr = TString().append(i % 20, 'x');
            for (size_t j = 0; j <= i % 3; ++j) {
                row.List.push_back(i32(i * 10 + j));
            }
            rows.push_back(std::move(row));
        }
        return rows;
    }

    TStructType* RowType() {
        if (!RowType_) {
            TMkqlIOSpecs specs;
            specs.Init(CodecCtx, ROW_SPEC);
            RowType_ = specs.Outputs.at(0).RowType;
        }
        return RowType_;
    }

    NUdf::TUnboxedValue MakeRow(const TRow& row) {
        auto* rowType = RowType();
        NUdf::TUnboxedValue* items = nullptr;
        NUdf::TUnboxedValue result = HolderFactory.CreateDirectArrayHolder(rowType->GetMembersCount(), items);
        const auto set = [&](TStringBuf name, NUdf::TUnboxedValue item) {
            items[rowType->GetMemberIndex(name)] = std::move(item);
        };

        set("b", NUdf::TUnboxedValuePod(row.Bool));
        set("i8", NUdf::TUnboxedValuePod(row.Int8));
        set("i32", NUdf::TUnboxedValuePod(row.Int32));
        set("u32", NUdf::TUnboxedValuePod(row.Uint32));
        set("dbl", NUdf::TUnboxedValuePod(row.Double));
        set("dec", NUdf::TUnboxedValuePod(row.Decimal));
        set("str", MakeString(row.Str));
        set("utf8", MakeString(row.Utf8));
        set("vd", NUdf::TUnboxedValuePod::Void());

        auto* nestedType = AS_TYPE(TStructType, rowType->GetMemberType(rowType->GetMemberIndex("nested")));
        NUdf::TUnboxedValue* nestedItems = nullptr;
        NUdf::TUnboxedValue nested = HolderFactory.CreateDirectArrayHolder(nestedType->GetMembersCount(), nestedItems);
        nestedItems[nestedType->GetMemberIndex("num")] = NUdf::TUnboxedValuePod(row.NestedNum);
        nestedItems[nestedType->GetMemberIndex("str")] = MakeString(row.NestedStr);
        set("nested", std::move(nested));

        NUdf::TUnboxedValue* listItems = nullptr;
        NUdf::TUnboxedValue list = HolderFactory.CreateDirectArrayHolder(row.List.size(), listItems);
        for (size_t i = 0; i < row.List.size(); ++i) {
            listItems[i] = NUdf::TUnboxedValuePod(row.List[i]);
        }
        set("list", std::move(list));

        return result;
    }

    TString ToYson(const NUdf::TUnboxedValuePod& value) {
        return NCommon::WriteYsonValue(value, RowType(), nullptr, NYson::EYsonFormat::Text);
    }

    TString Encode(const TString& optLLVM, const TVector<TRow>& rows) {
        TMkqlIOSpecs specs;
        specs.SetUseSkiff(optLLVM);
        specs.Init(CodecCtx, ROW_SPEC);

        TStringStream out;
        TMkqlWriterImpl writer(out, 0, BLOCK_SIZE);
        writer.SetSpecs(specs);
        for (const auto& row : rows) {
            const NUdf::TUnboxedValue value = MakeRow(row);
            writer.AddRow(value);
        }
        writer.Finish();
        out.Finish();
        return out.Str();
    }

    void DecodeAndValidate(const TString& optLLVM, const TString& data, const TVector<TRow>& rows) {
        TMkqlIOSpecs specs;
        specs.SetUseSkiff(optLLVM);
        specs.Init(CodecCtx, ROW_SPEC, {}, Nothing());

        TSkiffTestInput input(data);
        TMkqlReaderImpl reader(input, 0, BLOCK_SIZE);
        reader.SetSpecs(specs, HolderFactory);
        reader.Next();

        for (size_t i = 0; i < rows.size(); reader.Next(), ++i) {
            UNIT_ASSERT(reader.IsValid());
            UNIT_ASSERT_VALUES_EQUAL_C(ToYson(reader.GetRow()), ToYson(MakeRow(rows[i])), "row " << i);
        }
        UNIT_ASSERT(!reader.IsValid());
    }

private:
    TStructType* RowType_ = nullptr;
};

const TString TSkiffCodecFixture::ROW_SPEC = R"({
    tables = [{
        "_yql_row_spec" = {
            "Type" = [
                "StructType"; [
                    ["b"; ["DataType"; "Bool"]];
                    ["dbl"; ["DataType"; "Double"]];
                    ["dec"; ["DataType"; "Decimal"; "10"; "2"]];
                    ["i32"; ["DataType"; "Int32"]];
                    ["i8"; ["DataType"; "Int8"]];
                    ["list"; ["ListType"; ["DataType"; "Int32"]]];
                    ["nested"; ["StructType"; [
                        ["num"; ["DataType"; "Int32"]];
                        ["str"; ["DataType"; "String"]]
                    ]]];
                    ["str"; ["DataType"; "String"]];
                    ["u32"; ["DataType"; "Uint32"]];
                    ["utf8"; ["DataType"; "Utf8"]];
                    ["vd"; ["VoidType"]]
                ]
            ]
        }
    }]
})";

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

Y_UNIT_TEST_SUITE(TMkqlSkiffCodec) {

    Y_UNIT_TEST(WriterLLVMSameAsNoLLVM) {
        TSkiffCodecFixture fixture;
        const auto rows = TSkiffCodecFixture::Generate(32);
        const auto data = fixture.Encode("", rows);
        UNIT_ASSERT(!data.empty());
        UNIT_ASSERT_STRINGS_EQUAL(data, fixture.Encode("OFF", rows));
    }

    Y_UNIT_TEST(ReaderLLVMSameAsNoLLVM) {
        TSkiffCodecFixture fixture;
        const auto rows = TSkiffCodecFixture::Generate(32);
        const auto data = fixture.Encode("OFF", rows);
        fixture.DecodeAndValidate("", data, rows);
        fixture.DecodeAndValidate("OFF", data, rows);
    }

}
