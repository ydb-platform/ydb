#ifndef MKQL_DISABLE_CODEGEN
#include "yt_codec_cg.h"
#include <ydb/library/yql/providers/common/codec/yql_codec_buf.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/library/yql/providers/yt/codec/yt_codec_io.h>
#include <ydb/library/yql/minikql/codegen/codegen.h>

#include <llvm/IR/Module.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

using namespace NCommon;
using namespace NCodegen;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace std::string_view_literals;

class TTestWriter : public IBlockWriter {
public:
    TTestWriter()
    {
        Buffer_.ReserveAndResize(1024);
    }

    void SetRecordBoundaryCallback(std::function<void()> /*callback*/) override {
    }

    std::pair<char*, char*> NextEmptyBlock() override {
        if (FirstBuffer_) {
            FirstBuffer_ = false;
            auto ptr = Buffer_.Detach();
            return {ptr, ptr + Buffer_.capacity()};
        } else {
            return {nullptr, nullptr};
        }
    }

    void ReturnBlock(size_t avail, std::optional<size_t> /*lastRecordBoundary*/) override {
        Buffer_.resize(avail);
    }

    void Finish() override {
    }

    TString Str() const {
        return Buffer_;
    }

private:
    TString Buffer_;
    bool FirstBuffer_ = true;
};

struct TWriteSetup {
    TWriteSetup(const char* funcName)
        : Buf_(TestWriter_, nullptr)
    {
        Codegen_ = ICodegen::Make(ETarget::Native);
        Codegen_->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        Func_ = Codegen_->GetModule().getFunction(funcName);
        UNIT_ASSERT(Func_);
        Codegen_->Verify();
        YtCodecAddMappings(*Codegen_);
        Codegen_->ExportSymbol(Func_);
        Codegen_->Compile();
        //Codegen_->GetModule().dump();
    }

    ICodegen::TPtr Codegen_;
    llvm::Function* Func_;
    TTestWriter TestWriter_;
    TOutputBuf Buf_;
};

class TTestReader : public IBlockReader {
public:
    TTestReader(TStringBuf data)
        : Data_(data)
    {}

    void SetDeadline(TInstant deadline)  override {
        Y_UNUSED(deadline);
    }

    std::pair<const char*, const char*> NextFilledBlock() override {
        if (FirstBuffer_) {
            FirstBuffer_ = false;
            return std::make_pair(Data_.begin(), Data_.end());
        } else {
            return { nullptr, nullptr };
        }
    }

    void ReturnBlock() override {
        YQL_ENSURE(!FirstBuffer_);
    }

    bool Retry(const TMaybe<ui32>& rangeIndex, const TMaybe<ui64>& rowIndex, const std::exception_ptr& error) override {
        Y_UNUSED(rangeIndex);
        Y_UNUSED(rowIndex);
        Y_UNUSED(error);
        return false;
    }

private:
    TStringBuf Data_;
    bool FirstBuffer_ = true;
};

struct TReadSetup {
    TReadSetup(const char* funcName, TStringBuf readData)
        : TestReader_(readData)
        , Buf_(TestReader_, nullptr)
    {
        Codegen_ = ICodegen::Make(ETarget::Native);
        Codegen_->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        Func_ = Codegen_->GetModule().getFunction(funcName);
        UNIT_ASSERT(Func_);
        Codegen_->Verify();
        YtCodecAddMappings(*Codegen_);
        Codegen_->ExportSymbol(Func_);
        Codegen_->Compile();
        //Codegen_->GetModule().dump();
    }

    ICodegen::TPtr Codegen_;
    llvm::Function* Func_;
    TTestReader TestReader_;
    TInputBuf Buf_;
};

Y_UNIT_TEST_SUITE(TYtCodegenCodec) {
    Y_UNIT_TEST(TestWriteJust) {
        TWriteSetup setup("WriteJust");
        typedef void(*TFunc)(TOutputBuf&);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        funcPtr(setup.Buf_);
        setup.Buf_.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(setup.TestWriter_.Str().Quote(), R"("\1")");
    }

    Y_UNIT_TEST(TestWriteNothing) {
        TWriteSetup setup("WriteNothing");
        typedef void(*TFunc)(TOutputBuf&);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        funcPtr(setup.Buf_);
        setup.Buf_.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(setup.TestWriter_.Str().Quote(), R"("\0")");
    }

    Y_UNIT_TEST(TestWriteBool) {
        TWriteSetup setup("WriteBool");
        typedef void(*TFunc)(TOutputBuf&, bool);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        funcPtr(setup.Buf_, true);
        funcPtr(setup.Buf_, false);
        setup.Buf_.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(setup.TestWriter_.Str().Quote(), R"("\1\0")");
    }

    Y_UNIT_TEST(TestWrite8) {
        TWriteSetup setup("Write8");
        typedef void(*TFunc)(TOutputBuf&, ui8);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        funcPtr(setup.Buf_, 3);
        setup.Buf_.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(setup.TestWriter_.Str().Quote(), R"("\3")");
    }
#ifndef _win_
    Y_UNIT_TEST(TestWrite16) {
        TWriteSetup setup("Write16");
        typedef void(*TFunc)(TOutputBuf&, ui16);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        funcPtr(setup.Buf_, 258);
        setup.Buf_.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(setup.TestWriter_.Str().Quote(), R"("\2\1")");
    }

    Y_UNIT_TEST(TestWrite32) {
        TWriteSetup setup("Write32");
        typedef void(*TFunc)(TOutputBuf&, ui32);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        funcPtr(setup.Buf_, 258);
        setup.Buf_.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(setup.TestWriter_.Str().Quote(), R"("\2\1\0\0")");
    }

    Y_UNIT_TEST(TestWrite64) {
        TWriteSetup setup("Write64");
        typedef void(*TFunc)(TOutputBuf&, ui64);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        funcPtr(setup.Buf_, 258);
        setup.Buf_.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(setup.TestWriter_.Str().Quote(), R"("\2\1\0\0\0\0\0\0")");
    }
#endif
    Y_UNIT_TEST(TestWriteString) {
        TWriteSetup setup("WriteString");
        typedef void(*TFunc)(TOutputBuf&, const char*, ui32);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        funcPtr(setup.Buf_, "foo", 3);
        setup.Buf_.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(setup.TestWriter_.Str().Quote(), R"("foo")");
    }
#ifndef _win_
    Y_UNIT_TEST(TestWriteDataRowSmall) {
        auto codegen = ICodegen::Make(ETarget::Native);
        codegen->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        auto writer = MakeYtCodecCgWriter<false>(codegen);
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto boolType = TDataType::Create(NUdf::TDataType<bool>::Id, env);
        auto int8Type = TDataType::Create(NUdf::TDataType<i8>::Id, env);
        auto uint8Type = TDataType::Create(NUdf::TDataType<ui8>::Id, env);
        auto int16Type = TDataType::Create(NUdf::TDataType<i16>::Id, env);
        auto uint16Type = TDataType::Create(NUdf::TDataType<ui16>::Id, env);
        auto int32Type = TDataType::Create(NUdf::TDataType<i32>::Id, env);
        auto uint32Type = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
        auto int64Type = TDataType::Create(NUdf::TDataType<i64>::Id, env);
        auto uint64Type = TDataType::Create(NUdf::TDataType<ui64>::Id, env);
        auto floatType = TDataType::Create(NUdf::TDataType<float>::Id, env);
        auto doubleType = TDataType::Create(NUdf::TDataType<double>::Id, env);

        writer->AddField(boolType, false);
        writer->AddField(int8Type, false);
        writer->AddField(uint8Type, false);
        writer->AddField(int16Type, false);
        writer->AddField(uint16Type, false);
        writer->AddField(int32Type, false);
        writer->AddField(uint32Type, false);
        writer->AddField(int64Type, false);
        writer->AddField(uint64Type, false);
        writer->AddField(floatType, false);
        writer->AddField(doubleType, false);

        auto func = writer->Build();
        codegen->Verify();
        YtCodecAddMappings(*codegen);
        codegen->Compile();

        TMemoryUsageInfo memInfo("test");
        THolderFactory holderFactory(alloc.Ref(), memInfo);
        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue row = holderFactory.CreateDirectArrayHolder(11, items);
        items[0] = NUdf::TUnboxedValuePod(true);
        items[1] = NUdf::TUnboxedValuePod(i8(-1));
        items[2] = NUdf::TUnboxedValuePod(ui8(2));
        items[3] = NUdf::TUnboxedValuePod(i16(-3));
        items[4] = NUdf::TUnboxedValuePod(ui16(4));
        items[5] = NUdf::TUnboxedValuePod(i32(-5));
        items[6] = NUdf::TUnboxedValuePod(ui32(6));
        items[7] = NUdf::TUnboxedValuePod(i64(-7));
        items[8] = NUdf::TUnboxedValuePod(ui64(8));
        items[9] = NUdf::TUnboxedValuePod(float(9));
        items[10] = NUdf::TUnboxedValuePod(double(10));
        typedef void(*TFunc)(const NUdf::TUnboxedValuePod, TOutputBuf&);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);

        TTestWriter testWriter;
        TOutputBuf buf(testWriter, nullptr);
        funcPtr(row, buf);
        buf.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(testWriter.Str().Quote(), R"("\1\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\2\0\0\0\0\0\0\0\xFD\xFF\xFF\xFF\xFF\xFF\xFF\xFF\4\0\0\0\0\0\0\0\xFB\xFF\xFF\xFF\xFF\xFF\xFF\xFF\6\0\0\0\0\0\0\0\xF9\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x08\0\0\0\0\0\0\0\0\0\0\0\0\0\"@\0\0\0\0\0\0$@")");
    }

    Y_UNIT_TEST(TestWriteDataRowString) {
        auto codegen = ICodegen::Make(ETarget::Native);
        codegen->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        auto writer = MakeYtCodecCgWriter<false>(codegen);
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto stringType = TDataType::Create(NUdf::TDataType<char*>::Id, env);
        auto utf8Type = TDataType::Create(NUdf::TDataType<NUdf::TUtf8>::Id, env);
        auto jsonType = TDataType::Create(NUdf::TDataType<NUdf::TJson>::Id, env);
        auto ysonType = TDataType::Create(NUdf::TDataType<NUdf::TYson>::Id, env);

        writer->AddField(stringType, false);
        writer->AddField(utf8Type, false);
        writer->AddField(jsonType, false);
        writer->AddField(ysonType, false);

        auto func = writer->Build();
        codegen->Verify();
        YtCodecAddMappings(*codegen);
        codegen->Compile();

        TMemoryUsageInfo memInfo("test");
        THolderFactory holderFactory(alloc.Ref(), memInfo);
        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue row = holderFactory.CreateDirectArrayHolder(4, items);
        items[0] = NUdf::TUnboxedValue(MakeString("aaa"));
        items[1] = NUdf::TUnboxedValue(MakeString("VERY LOOOONG STRING"));
        items[2] = NUdf::TUnboxedValue(MakeString("[1,2]"));
        items[3] = NUdf::TUnboxedValue(MakeString("{foo=bar}"));
        typedef void(*TFunc)(const NUdf::TUnboxedValuePod, TOutputBuf&);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);

        TTestWriter testWriter;
        TOutputBuf buf(testWriter, nullptr);
        funcPtr(row, buf);
        buf.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(testWriter.Str().Quote(), R"("\3\0\0\0aaa\x13\0\0\0VERY LOOOONG STRING\5\0\0\0[1,2]\t\0\0\0{foo=bar}")");
    }

    Y_UNIT_TEST(TestWriteDataRowDecimal) {
        auto codegen = ICodegen::Make(ETarget::Native);
        codegen->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        auto writer = MakeYtCodecCgWriter<false>(codegen);
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto longintType = TDataDecimalType::Create(10, 4, env);

        writer->AddField(longintType, false);

        auto func = writer->Build();
        codegen->Verify();
        YtCodecAddMappings(*codegen);
        codegen->Compile();

        TMemoryUsageInfo memInfo("test");
        THolderFactory holderFactory(alloc.Ref(), memInfo);
        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue row = holderFactory.CreateDirectArrayHolder(1, items);
        items[0] = NUdf::TUnboxedValuePod(NDecimal::TInt128(-5));
        typedef void(*TFunc)(const NUdf::TUnboxedValuePod, TOutputBuf&);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);

        TTestWriter testWriter;
        TOutputBuf buf(testWriter, nullptr);
        funcPtr(row, buf);
        buf.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(testWriter.Str().Quote(), R"("\2\0\0\0~\xFB")");
    }

    Y_UNIT_TEST(TestWriteDataRowDecimalNan) {
        auto codegen = ICodegen::Make(ETarget::Native);
        codegen->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        auto writer = MakeYtCodecCgWriter<false>(codegen);
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto longintType = TDataDecimalType::Create(10, 4, env);

        writer->AddField(longintType, false);

        auto func = writer->Build();
        codegen->Verify();
        YtCodecAddMappings(*codegen);
        codegen->Compile();

        TMemoryUsageInfo memInfo("test");
        THolderFactory holderFactory(alloc.Ref(), memInfo);
        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue row = holderFactory.CreateDirectArrayHolder(1, items);
        items[0] = NUdf::TUnboxedValuePod(NDecimal::Nan());
        typedef void(*TFunc)(const NUdf::TUnboxedValuePod, TOutputBuf&);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);

        TTestWriter testWriter;
        TOutputBuf buf(testWriter, nullptr);
        funcPtr(row, buf);
        buf.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(testWriter.Str().Quote(), R"("\1\0\0\0\xFF")");
    }

    Y_UNIT_TEST(TestWriteOptionalRow) {
        auto codegen = ICodegen::Make(ETarget::Native);
        codegen->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        auto writer = MakeYtCodecCgWriter<false>(codegen);
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto int32Type = TDataType::Create(NUdf::TDataType<i32>::Id, env);
        auto optInt32Type = TOptionalType::Create(int32Type, env);

        writer->AddField(optInt32Type, false);
        writer->AddField(optInt32Type, false);

        auto func = writer->Build();
        codegen->Verify();
        YtCodecAddMappings(*codegen);
        codegen->Compile();

        TMemoryUsageInfo memInfo("test");
        THolderFactory holderFactory(alloc.Ref(), memInfo);
        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue row = holderFactory.CreateDirectArrayHolder(2, items);
        items[0] = NUdf::TUnboxedValuePod();
        items[1] = NUdf::TUnboxedValuePod(i32(5));
        typedef void(*TFunc)(const NUdf::TUnboxedValuePod, TOutputBuf&);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);

        TTestWriter testWriter;
        TOutputBuf buf(testWriter, nullptr);
        funcPtr(row, buf);
        buf.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(testWriter.Str().Quote(), R"("\0\1\5\0\0\0\0\0\0\0")");
    }

    Y_UNIT_TEST(TestWriteContainerRow) {
        auto codegen = ICodegen::Make(ETarget::Native);
        codegen->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        auto writer = MakeYtCodecCgWriter<false>(codegen);
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto uint32Type = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
        auto optUint32Type = TOptionalType::Create(uint32Type, env);
        auto list1Type = TListType::Create(optUint32Type, env);
        auto list2Type = TOptionalType::Create(TListType::Create(optUint32Type, env), env);

        writer->AddField(list1Type, false);
        writer->AddField(list2Type, false);
        writer->AddField(list2Type, false);

        auto func = writer->Build();
        codegen->Verify();
        YtCodecAddMappings(*codegen);
        codegen->Compile();

        TMemoryUsageInfo memInfo("test");
        THolderFactory holderFactory(alloc.Ref(), memInfo);

        NUdf::TUnboxedValue* list1items;
        NUdf::TUnboxedValue list1 = holderFactory.CreateDirectArrayHolder(2, list1items);
        list1items[0] = NUdf::TUnboxedValuePod(ui32(2));
        list1items[1] = NUdf::TUnboxedValuePod(ui32(3));

        NUdf::TUnboxedValue* list2items;
        NUdf::TUnboxedValue list2 = holderFactory.CreateDirectArrayHolder(3, list2items);
        list2items[0] = NUdf::TUnboxedValuePod(ui32(4));
        list2items[1] = NUdf::TUnboxedValuePod(ui32(5));
        list2items[2] = NUdf::TUnboxedValue();

        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue row = holderFactory.CreateDirectArrayHolder(3, items);
        items[0] = list1;
        items[1] = NUdf::TUnboxedValue();
        items[2] = list2;
        typedef void(*TFunc)(const NUdf::TUnboxedValuePod, TOutputBuf&);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);

        TTestWriter testWriter;
        TOutputBuf buf(testWriter, nullptr);
        funcPtr(row, buf);
        buf.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(testWriter.Str().Quote(), R"("\x0E\0\0\0[[\6\2;];[\6\3;];]\0\1\x10\0\0\0[[\6\4;];[\6\5;];#;]")");
    }
#endif
    Y_UNIT_TEST(TestReadBool) {
        TReadSetup setup("ReadBool", "\1");
        typedef void(*TFunc)(TInputBuf&, void*);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        NUdf::TUnboxedValue val;
        funcPtr(setup.Buf_, &val);
        UNIT_ASSERT(val.Get<bool>());
    }
#ifndef _win_
    Y_UNIT_TEST(TestReadInt8) {
        TReadSetup setup("ReadInt8", "\xff\xff\xff\xff\xff\xff\xff\xff");
        typedef void(*TFunc)(TInputBuf&, void*);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        NUdf::TUnboxedValue val;
        funcPtr(setup.Buf_, &val);
        UNIT_ASSERT_VALUES_EQUAL(val.Get<i8>(), -1);
    }

    Y_UNIT_TEST(TestReadUint8) {
        TReadSetup setup("ReadUint8", "\2\0\0\0\0\0\0\0"sv);
        typedef void(*TFunc)(TInputBuf&, void*);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        NUdf::TUnboxedValue val;
        funcPtr(setup.Buf_, &val);
        UNIT_ASSERT_VALUES_EQUAL(val.Get<ui8>(), 2);
    }

    Y_UNIT_TEST(TestReadInt16) {
        TReadSetup setup("ReadInt16", "\xff\xff\xff\xff\xff\xff\xff\xff");
        typedef void(*TFunc)(TInputBuf&, void*);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        NUdf::TUnboxedValue val;
        funcPtr(setup.Buf_, &val);
        UNIT_ASSERT_VALUES_EQUAL(val.Get<i16>(), -1);
    }

    Y_UNIT_TEST(TestReadUint16) {
        TReadSetup setup("ReadUint16", "\2\1\0\0\0\0\0\0"sv);
        typedef void(*TFunc)(TInputBuf&, void*);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        NUdf::TUnboxedValue val;
        funcPtr(setup.Buf_, &val);
        UNIT_ASSERT_VALUES_EQUAL(val.Get<ui16>(), 258);
    }

    Y_UNIT_TEST(TestReadInt32) {
        TReadSetup setup("ReadInt32", "\xff\xff\xff\xff\xff\xff\xff\xff");
        typedef void(*TFunc)(TInputBuf&, void*);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        NUdf::TUnboxedValue val;
        funcPtr(setup.Buf_, &val);
        UNIT_ASSERT_VALUES_EQUAL(val.Get<i32>(), -1);
    }

    Y_UNIT_TEST(TestReadUint32) {
        TReadSetup setup("ReadUint32", "\2\1\0\0\0\0\0\0"sv);
        typedef void(*TFunc)(TInputBuf&, void*);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        NUdf::TUnboxedValue val;
        funcPtr(setup.Buf_, &val);
        UNIT_ASSERT_VALUES_EQUAL(val.Get<ui32>(), 258);
    }

    Y_UNIT_TEST(TestReadInt64) {
        TReadSetup setup("ReadInt64", "\xff\xff\xff\xff\xff\xff\xff\xff");
        typedef void(*TFunc)(TInputBuf&, void*);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        NUdf::TUnboxedValue val;
        funcPtr(setup.Buf_, &val);
        UNIT_ASSERT_VALUES_EQUAL(val.Get<i32>(), -1);
    }

    Y_UNIT_TEST(TestReadUint64) {
        TReadSetup setup("ReadUint64", "\2\1\0\0\0\0\0\0"sv);
        typedef void(*TFunc)(TInputBuf&, void*);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        NUdf::TUnboxedValue val;
        funcPtr(setup.Buf_, &val);
        UNIT_ASSERT_VALUES_EQUAL(val.Get<ui64>(), 258);
    }

    Y_UNIT_TEST(TestReadDataRowSmall) {
        auto codegen = ICodegen::Make(ETarget::Native);
        codegen->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        TScopedAlloc alloc(__LOCATION__);
        TMemoryUsageInfo memInfo("test");
        THolderFactory holderFactory(alloc.Ref(), memInfo);

        auto reader = MakeYtCodecCgReader(codegen, holderFactory);
        TTypeEnvironment env(alloc);
        auto boolType = TDataType::Create(NUdf::TDataType<bool>::Id, env);
        auto int8Type = TDataType::Create(NUdf::TDataType<i8>::Id, env);
        auto uint8Type = TDataType::Create(NUdf::TDataType<ui8>::Id, env);
        auto int16Type = TDataType::Create(NUdf::TDataType<i16>::Id, env);
        auto uint16Type = TDataType::Create(NUdf::TDataType<ui16>::Id, env);
        auto int32Type = TDataType::Create(NUdf::TDataType<i32>::Id, env);
        auto uint32Type = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
        auto int64Type = TDataType::Create(NUdf::TDataType<i64>::Id, env);
        auto uint64Type = TDataType::Create(NUdf::TDataType<ui64>::Id, env);
        auto floatType = TDataType::Create(NUdf::TDataType<float>::Id, env);
        auto doubleType = TDataType::Create(NUdf::TDataType<double>::Id, env);

        reader->AddField(boolType, {}, false);
        reader->AddField(int8Type, {}, false);
        reader->AddField(uint8Type, {}, false);
        reader->AddField(int16Type, {}, false);
        reader->AddField(uint16Type, {}, false);
        reader->AddField(int32Type, {}, false);
        reader->AddField(uint32Type, {}, false);
        reader->AddField(int64Type, {}, false);
        reader->AddField(uint64Type, {}, false);
        reader->AddField(floatType, {}, false);
        reader->AddField(doubleType, {}, false);

        auto func = reader->Build();
        codegen->Verify();
        YtCodecAddMappings(*codegen);
        codegen->Compile();

        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue row = holderFactory.CreateDirectArrayHolder(11, items);
        typedef void(*TFunc)(NUdf::TUnboxedValue*, TInputBuf&);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);

        TTestReader testReader("\1\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\2\0\0\0\0\0\0\0\xFD\xFF\xFF\xFF\xFF\xFF\xFF\xFF\4\0\0\0\0\0\0\0\xFB\xFF\xFF\xFF\xFF\xFF\xFF\xFF\6\0\0\0\0\0\0\0\xF9\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x08\0\0\0\0\0\0\0\0\0\0\0\0\0\"@\0\0\0\0\0\0$@"sv);
        TInputBuf buf(testReader, nullptr);
        funcPtr(items, buf);
        UNIT_ASSERT(items[0].Get<bool>());
        UNIT_ASSERT_VALUES_EQUAL(items[1].Get<i8>(), i8(-1));
        UNIT_ASSERT_VALUES_EQUAL(items[2].Get<ui8>(), ui8(2));
        UNIT_ASSERT_VALUES_EQUAL(items[3].Get<i16>(), i16(-3));
        UNIT_ASSERT_VALUES_EQUAL(items[4].Get<ui16>(), ui16(4));
        UNIT_ASSERT_VALUES_EQUAL(items[5].Get<i32>(), i32(-5));
        UNIT_ASSERT_VALUES_EQUAL(items[6].Get<ui32>(), ui32(6));
        UNIT_ASSERT_VALUES_EQUAL(items[7].Get<i64>(), i64(-7));
        UNIT_ASSERT_VALUES_EQUAL(items[8].Get<ui64>(), ui64(8));
        UNIT_ASSERT_VALUES_EQUAL(items[9].Get<float>(), float(9));
        UNIT_ASSERT_VALUES_EQUAL(items[10].Get<double>(), double(10));
    }
#endif
    Y_UNIT_TEST(TestReadDataRowString) {
        auto codegen = ICodegen::Make(ETarget::Native);
        codegen->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        TMemoryUsageInfo memInfo("test");
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memInfo);

        auto reader = MakeYtCodecCgReader(codegen, holderFactory);
        TTypeEnvironment env(alloc);
        auto stringType = TDataType::Create(NUdf::TDataType<char*>::Id, env);
        auto utf8Type = TDataType::Create(NUdf::TDataType<NUdf::TUtf8>::Id, env);
        auto jsonType = TDataType::Create(NUdf::TDataType<NUdf::TJson>::Id, env);
        auto ysonType = TDataType::Create(NUdf::TDataType<NUdf::TYson>::Id, env);

        reader->AddField(stringType, {}, false);
        reader->AddField(utf8Type, {}, false);
        reader->AddField(jsonType, {}, false);
        reader->AddField(ysonType, {}, false);

        auto func = reader->Build();
        codegen->Verify();
        YtCodecAddMappings(*codegen);
        codegen->Compile();

        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue row = holderFactory.CreateDirectArrayHolder(4, items);
        typedef void(*TFunc)(NUdf::TUnboxedValue*, TInputBuf&);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);

        TTestReader testReader("\3\0\0\0aaa\x13\0\0\0VERY LOOOONG STRING\5\0\0\0[1,2]\t\0\0\0{foo=bar}"sv);
        TInputBuf buf(testReader, nullptr);
        funcPtr(items, buf);
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(items[0].AsStringRef()), "aaa");
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(items[1].AsStringRef()), "VERY LOOOONG STRING");
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(items[2].AsStringRef()), "[1,2]");
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(items[3].AsStringRef()), "{foo=bar}");
    }
#ifndef _win_
    Y_UNIT_TEST(TestReadDataRowDecimal) {
        auto codegen = ICodegen::Make(ETarget::Native);
        codegen->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        TMemoryUsageInfo memInfo("test");
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memInfo);

        auto reader = MakeYtCodecCgReader(codegen, holderFactory);
        TTypeEnvironment env(alloc);
        auto longintType = TDataDecimalType::Create(10, 4, env);

        reader->AddField(longintType, {}, false);

        auto func = reader->Build();
        codegen->Verify();
        YtCodecAddMappings(*codegen);
        codegen->Compile();

        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue row = holderFactory.CreateDirectArrayHolder(1, items);
        typedef void(*TFunc)(NUdf::TUnboxedValue*, TInputBuf&);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);

        TTestReader testReader("\2\0\0\0~\xFB"sv);
        TInputBuf buf(testReader, nullptr);
        funcPtr(items, buf);
        UNIT_ASSERT_EQUAL(items[0].GetInt128(), -5);
    }

    Y_UNIT_TEST(TestReadDataRowDecimalNan) {
        auto codegen = ICodegen::Make(ETarget::Native);
        codegen->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        TMemoryUsageInfo memInfo("test");
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memInfo);

        auto reader = MakeYtCodecCgReader(codegen, holderFactory);
        TTypeEnvironment env(alloc);
        auto longintType = TDataDecimalType::Create(10, 4, env);

        reader->AddField(longintType, {}, false);

        auto func = reader->Build();
        codegen->Verify();
        YtCodecAddMappings(*codegen);
        codegen->Compile();

        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue row = holderFactory.CreateDirectArrayHolder(1, items);
        typedef void(*TFunc)(NUdf::TUnboxedValue*, TInputBuf&);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);

        TTestReader testReader("\1\0\0\0\xFF"sv);
        TInputBuf buf(testReader, nullptr);
        funcPtr(items, buf);
        UNIT_ASSERT_EQUAL(items[0].GetInt128(), NDecimal::Nan());
    }

    Y_UNIT_TEST(TestReadOptionalRow) {
        auto codegen = ICodegen::Make(ETarget::Native);
        codegen->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        TMemoryUsageInfo memInfo("test");
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memInfo);

        auto reader = MakeYtCodecCgReader(codegen, holderFactory);
        TTypeEnvironment env(alloc);
        auto int32Type = TDataType::Create(NUdf::TDataType<i32>::Id, env);
        auto optInt32Type = TOptionalType::Create(int32Type, env);

        reader->AddField(optInt32Type, {}, false);
        reader->AddField(optInt32Type, {}, false);

        auto func = reader->Build();
        codegen->Verify();
        YtCodecAddMappings(*codegen);
        codegen->Compile();

        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue row = holderFactory.CreateDirectArrayHolder(2, items);
        typedef void(*TFunc)(NUdf::TUnboxedValue*, TInputBuf&);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);

        TTestReader testReader("\0\1\5\0\0\0\0\0\0\0"sv);
        TInputBuf buf(testReader, nullptr);
        funcPtr(items, buf);
        UNIT_ASSERT(!items[0]);
        UNIT_ASSERT_VALUES_EQUAL(items[1].Get<i32>(), 5);
    }
#endif
    Y_UNIT_TEST(TestReadContainerRow) {
        auto codegen = ICodegen::Make(ETarget::Native);
        codegen->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        TMemoryUsageInfo memInfo("test");
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memInfo);

        auto reader = MakeYtCodecCgReader(codegen, holderFactory);
        TTypeEnvironment env(alloc);

        auto uint32Type = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
        auto optUint32Type = TOptionalType::Create(uint32Type, env);
        auto list1Type = TListType::Create(optUint32Type, env);
        auto list2Type = TOptionalType::Create(TListType::Create(optUint32Type, env), env);

        reader->AddField(list1Type, {}, false);
        reader->AddField(list2Type, {}, false);
        reader->AddField(list2Type, {}, false);

        auto func = reader->Build();
        codegen->Verify();
        YtCodecAddMappings(*codegen);
        codegen->Compile();

        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue row = holderFactory.CreateDirectArrayHolder(3, items);
        typedef void(*TFunc)(NUdf::TUnboxedValue*, TInputBuf&);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);

        TTestReader testReader("\x0E\0\0\0[[\6\2;];[\6\3;];]\0\1\x10\0\0\0[[\6\4;];[\6\5;];#;]"sv);
        TInputBuf buf(testReader, nullptr);
        funcPtr(items, buf);

        UNIT_ASSERT_VALUES_EQUAL(items[0].GetListLength(), 2);
        UNIT_ASSERT_VALUES_EQUAL(items[0].GetElements()[0].Get<ui32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(items[0].GetElements()[1].Get<ui32>(), 3);

        UNIT_ASSERT(!items[1]);

        UNIT_ASSERT_VALUES_EQUAL(items[2].GetListLength(), 3);
        UNIT_ASSERT_VALUES_EQUAL(items[2].GetElements()[0].Get<ui32>(), 4);
        UNIT_ASSERT_VALUES_EQUAL(items[2].GetElements()[1].Get<ui32>(), 5);
        UNIT_ASSERT(!items[2].GetElements()[2]);
    }
#ifndef _win_
    Y_UNIT_TEST(TestReadDefaultRow) {
        auto codegen = ICodegen::Make(ETarget::Native);
        codegen->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        TMemoryUsageInfo memInfo("test");
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memInfo);

        auto reader = MakeYtCodecCgReader(codegen, holderFactory);
        TTypeEnvironment env(alloc);
        auto int32Type = TDataType::Create(NUdf::TDataType<i32>::Id, env);

        reader->AddField(int32Type, NUdf::TUnboxedValuePod(7), false);
        reader->AddField(int32Type, NUdf::TUnboxedValuePod(7), false);

        auto func = reader->Build();
        codegen->Verify();
        YtCodecAddMappings(*codegen);
        codegen->Compile();

        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue row = holderFactory.CreateDirectArrayHolder(2, items);
        typedef void(*TFunc)(NUdf::TUnboxedValue*, TInputBuf&);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);

        TTestReader testReader("\0\1\5\0\0\0\0\0\0\0"sv);
        TInputBuf buf(testReader, nullptr);
        funcPtr(items, buf);
        UNIT_ASSERT_VALUES_EQUAL(items[0].Get<i32>(), 7);
        UNIT_ASSERT_VALUES_EQUAL(items[1].Get<i32>(), 5);
    }
#endif
    Y_UNIT_TEST(TestReadDefaultRowString) {
        auto codegen = ICodegen::Make(ETarget::Native);
        codegen->LoadBitCode(GetYtCodecBitCode(), "YtCodecFuncs");
        TMemoryUsageInfo memInfo("test");
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memInfo);

        auto reader = MakeYtCodecCgReader(codegen, holderFactory);
        TTypeEnvironment env(alloc);
        auto stringType = TDataType::Create(NUdf::TDataType<char*>::Id, env);

        const NUdf::TUnboxedValue defStr = MakeString(NUdf::TStringRef::Of("VERY LOOONG STRING"));
        reader->AddField(stringType, defStr, false);
        reader->AddField(stringType, defStr, false);

        auto func = reader->Build();
        codegen->Verify();
        YtCodecAddMappings(*codegen);
        codegen->Compile();

        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue row = holderFactory.CreateDirectArrayHolder(2, items);
        typedef void(*TFunc)(NUdf::TUnboxedValue*, TInputBuf&);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);

        TTestReader testReader("\0\1\3\0\0\0foo"sv);
        TInputBuf buf(testReader, nullptr);
        funcPtr(items, buf);
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(items[0].AsStringRef()), "VERY LOOONG STRING");
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(items[1].AsStringRef()), "foo");
    }
#ifndef _win_
    Y_UNIT_TEST(TestReadTzDate) {
        // full size = 2 + 2 = 4
        TStringBuf buf = "\x04\0\0\0\1\2\0\1"sv;
        TReadSetup setup("ReadTzDate", buf);
        typedef void(*TFunc)(TInputBuf&, void*);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        NUdf::TUnboxedValue val;
        funcPtr(setup.Buf_, &val);
        UNIT_ASSERT_VALUES_EQUAL(val.Get<ui16>(), 258);
        UNIT_ASSERT_VALUES_EQUAL(val.GetTimezoneId(), 1);
    }

    Y_UNIT_TEST(TestReadTzDatetime) {
        // full size = 4 + 2 = 6
        TStringBuf buf = "\x06\0\0\0\0\0\1\3\0\1"sv;
        TReadSetup setup("ReadTzDatetime", buf);
        typedef void(*TFunc)(TInputBuf&, void*);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        NUdf::TUnboxedValue val;
        funcPtr(setup.Buf_, &val);
        UNIT_ASSERT_VALUES_EQUAL(val.Get<ui32>(), 259);
        UNIT_ASSERT_VALUES_EQUAL(val.GetTimezoneId(), 1);
    }

    Y_UNIT_TEST(TestReadTzTimestamp) {
        // full size = 8 + 2 = 10
        TStringBuf buf = "\x0a\0\0\0\0\0\0\0\0\0\1\4\0\1"sv;
        TReadSetup setup("ReadTzTimestamp", buf);
        typedef void(*TFunc)(TInputBuf&, void*);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        NUdf::TUnboxedValue val;
        funcPtr(setup.Buf_, &val);
        UNIT_ASSERT_VALUES_EQUAL(val.Get<ui64>(), 260);
        UNIT_ASSERT_VALUES_EQUAL(val.GetTimezoneId(), 1);
    }

    Y_UNIT_TEST(TestReadTzDate32) {
        // full size = 4 + 2 = 6
        TStringBuf buf = "\6\0\0\0\x7F\xFF\xFE\xFE\0\1"sv;
        TReadSetup setup("ReadTzDate32", buf);
        typedef void(*TFunc)(TInputBuf&, void*);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        NUdf::TUnboxedValue val;
        funcPtr(setup.Buf_, &val);
        UNIT_ASSERT_VALUES_EQUAL(val.Get<i32>(), -258);
        UNIT_ASSERT_VALUES_EQUAL(val.GetTimezoneId(), 1);
    }

    Y_UNIT_TEST(TestReadTzDatetime64) {
        // full size = 8 + 2 = 10
        TStringBuf buf = "\n\0\0\0\x7F\xFF\xFF\xFF\xFF\xFF\xFE\xFD\0\1"sv;
        TReadSetup setup("ReadTzDatetime64", buf);
        typedef void(*TFunc)(TInputBuf&, void*);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        NUdf::TUnboxedValue val;
        funcPtr(setup.Buf_, &val);
        UNIT_ASSERT_VALUES_EQUAL(val.Get<i64>(), -259);
        UNIT_ASSERT_VALUES_EQUAL(val.GetTimezoneId(), 1);
    }

    Y_UNIT_TEST(TestReadTzTimestamp64) {
        // full size = 8 + 2 = 10
        TStringBuf buf = "\n\0\0\0\x7F\xFF\xFF\xFF\xFF\xFF\xFE\xFC\0\1"sv;
        TReadSetup setup("ReadTzTimestamp64", buf);
        typedef void(*TFunc)(TInputBuf&, void*);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        NUdf::TUnboxedValue val;
        funcPtr(setup.Buf_, &val);
        UNIT_ASSERT_VALUES_EQUAL(val.Get<i64>(), -260);
        UNIT_ASSERT_VALUES_EQUAL(val.GetTimezoneId(), 1);
    }

    Y_UNIT_TEST(TestWriteTzDate) {
        TWriteSetup setup("WriteTzDate");
        typedef void(*TFunc)(TOutputBuf&, ui16, ui16);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        funcPtr(setup.Buf_, 258, 1);
        setup.Buf_.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(setup.TestWriter_.Str().Quote(), TString("\x04\0\0\0\1\2\0\1"sv).Quote());
    }

    Y_UNIT_TEST(TestWriteTzDatetime) {
        TWriteSetup setup("WriteTzDatetime");
        typedef void(*TFunc)(TOutputBuf&, ui32, ui16);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        funcPtr(setup.Buf_, 259, 1);
        setup.Buf_.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(setup.TestWriter_.Str().Quote(), TString("\x06\0\0\0\0\0\1\3\0\1"sv).Quote());
    }

    Y_UNIT_TEST(TestWriteTzTimestamp) {
        TWriteSetup setup("WriteTzTimestamp");
        typedef void(*TFunc)(TOutputBuf&, ui64, ui16);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        funcPtr(setup.Buf_, 260, 1);
        setup.Buf_.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(setup.TestWriter_.Str().Quote(), TString("\x0a\0\0\0\0\0\0\0\0\0\1\4\0\1"sv).Quote());
    }

    Y_UNIT_TEST(TestWriteTzDate32) {
        TWriteSetup setup("WriteTzDate32");
        typedef void(*TFunc)(TOutputBuf&, i32, ui16);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        funcPtr(setup.Buf_, -258, 1);
        setup.Buf_.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(setup.TestWriter_.Str().Quote(), TString("\6\0\0\0\x7F\xFF\xFE\xFE\0\1"sv).Quote());
    }

    Y_UNIT_TEST(TestWriteTzDatetime64) {
        TWriteSetup setup("WriteTzDatetime64");
        typedef void(*TFunc)(TOutputBuf&, i64, ui16);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        funcPtr(setup.Buf_, -259, 1);
        setup.Buf_.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(setup.TestWriter_.Str().Quote(), TString("\n\0\0\0\x7F\xFF\xFF\xFF\xFF\xFF\xFE\xFD\0\1"sv).Quote());
    }

    Y_UNIT_TEST(TestWriteTzTimestamp64) {
        TWriteSetup setup("WriteTzTimestamp64");
        typedef void(*TFunc)(TOutputBuf&, i64, ui16);
        auto funcPtr = (TFunc)setup.Codegen_->GetPointerToFunction(setup.Func_);
        funcPtr(setup.Buf_, -260, 1);
        setup.Buf_.Finish();
        UNIT_ASSERT_STRINGS_EQUAL(setup.TestWriter_.Str().Quote(), TString("\n\0\0\0\x7F\xFF\xFF\xFF\xFF\xFF\xFE\xFC\0\1"sv).Quote());
    }
#endif
}

}
#endif
