#include "yt_codec_cg.h"

#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/codec.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_buf.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>

#ifndef MKQL_DISABLE_CODEGEN
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/codegen/codegen.h>

#include <ydb/library/binary_json/read.h>
#include <ydb/library/binary_json/write.h>

#include <library/cpp/resource/resource.h>

#include <llvm/IR/Module.h>
#include <llvm/IR/Instructions.h>

#endif

namespace NYql {

#ifndef MKQL_DISABLE_CODEGEN

using namespace llvm;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

extern "C" void ThrowBadDecimal() {
    throw yexception() << "Invalid decimal data";
}

extern "C" void YtCodecReadString(void* vbuf, void* vpod) {
    NYql::NCommon::TInputBuf& buf = *(NYql::NCommon::TInputBuf*)vbuf;
    NUdf::TUnboxedValue& pod = *(NUdf::TUnboxedValue*)vpod;
    ui32 size;
    buf.ReadMany((char*)&size, sizeof(size));
    CHECK_STRING_LENGTH_UNSIGNED(size);
    auto str = NUdf::TUnboxedValue(MakeStringNotFilled(size));
    buf.ReadMany(str.AsStringRef().Data(), size);
    pod = std::move(str);
}

extern "C" void YtCodecWriteJsonDocument(void* vbuf, const char* buffer, ui32 len) {
    NCommon::TOutputBuf& buf = *(NCommon::TOutputBuf*)vbuf;
    TStringBuf binaryJson(buffer, len);
    const TString json = NBinaryJson::SerializeToJson(binaryJson);
    const ui32 size = json.Size();
    buf.WriteMany((const char*)&size, sizeof(size));
    buf.WriteMany(json.Data(), size);
}

extern "C" void YtCodecReadJsonDocument(void* vbuf, void* vpod) {
    NYql::NCommon::TInputBuf& buf = *(NYql::NCommon::TInputBuf*)vbuf;
    NUdf::TUnboxedValue& pod = *(NUdf::TUnboxedValue*)vpod;

    ui32 size;
    buf.ReadMany((char*)&size, sizeof(size));
    CHECK_STRING_LENGTH_UNSIGNED(size);

    auto json = NUdf::TUnboxedValue(MakeStringNotFilled(size));
    buf.ReadMany(json.AsStringRef().Data(), size);

    const auto binaryJson = NBinaryJson::SerializeToBinaryJson(json.AsStringRef());
    if (!binaryJson.Defined()) {
        YQL_ENSURE(false, "Invalid JSON stored for JsonDocument type");
    }

    TStringBuf binaryJsonRef(binaryJson->Data(), binaryJson->Size());
    pod = MakeString(binaryJsonRef);
}

template<bool Flat>
class TYtCodecCgWriter : public IYtCodecCgWriter {
public:
    TYtCodecCgWriter(const std::unique_ptr<NCodegen::ICodegen>& codegen, const void* cookie)
        : Codegen_(codegen)
    {
        auto& module = Codegen_->GetModule();
        auto& context = Codegen_->GetContext();
        // input - pointer to struct UnboxedValue as int128 and instance of buffer, output - void
        const auto funcType = Flat || Codegen_->GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ?
            FunctionType::get(Type::getVoidTy(context), {PointerType::getUnqual(Type::getInt128Ty(context)), PointerType::getUnqual(Type::getInt8Ty(context))}, false):
            FunctionType::get(Type::getVoidTy(context), {Type::getInt128Ty(context), PointerType::getUnqual(Type::getInt8Ty(context))}, false);
        Func_ = cast<Function>(module.getOrInsertFunction((TStringBuilder() << (Flat ? "YtCodecCgWriterFlat." : "YtCodecCgWriter.") << cookie).data(), funcType).getCallee());
        Block_ = BasicBlock::Create(context, "EntryBlock", Func_);
    }

    void AddField(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags) override {
        auto& module = Codegen_->GetModule();
        auto& context = Codegen_->GetContext();
        auto args = Func_->arg_begin();
        const auto valueType = Type::getInt128Ty(context);
        const auto valueArg = &*args++;
        const auto buf = &*args++;
        const auto value = Flat || !valueArg->getType()->isPointerTy() ?
            static_cast<Value*>(valueArg) : new LoadInst(valueType, valueArg, "row", Block_);
        const auto index = ConstantInt::get(Type::getInt32Ty(context), Index_++);

        const auto elemPtr = Flat ?
            GetElementPtrInst::CreateInBounds(valueType, value, { index }, "elemPtr", Block_):
            static_cast<Value*>(new AllocaInst(valueType, 0U, "elemPtr", Block_));

        if constexpr (!Flat) {
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElement>(elemPtr, value, *Codegen_, Block_, index);
        }

        bool isOptional;
        auto unwrappedType = UnpackOptional(type, isOptional);
        if (!isOptional) {
            GenerateRequired(elemPtr, buf, type, nativeYtTypeFlags, false);
        } else {
            const auto just = BasicBlock::Create(context, "just", Func_);
            const auto nothing = BasicBlock::Create(context, "nothing", Func_);
            const auto done = BasicBlock::Create(context, "done", Func_);

            const auto zero = ConstantInt::get(valueType, 0ULL);
            const auto elem = new LoadInst(valueType, elemPtr, "elem", Block_);
            const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, elem, zero, "exists", Block_);
            BranchInst::Create(just, nothing, check, Block_);

            {
                Block_ = just;
                CallInst::Create(module.getFunction("WriteJust"), { buf }, "", Block_);
                if (unwrappedType->IsOptional() || unwrappedType->IsPg()) {
                    const auto unwrappedElem = GetOptionalValue(context, elem, Block_);
                    const auto unwrappedElemPtr = new AllocaInst(valueType, 0U, "unwrapped", Block_);
                    new StoreInst(unwrappedElem, unwrappedElemPtr, Block_);
                    GenerateRequired(unwrappedElemPtr, buf, unwrappedType, nativeYtTypeFlags, true);
                } else {
                    GenerateRequired(elemPtr, buf, unwrappedType, nativeYtTypeFlags, true);
                }

                BranchInst::Create(done, Block_);
            }

            {
                Block_ = nothing;
                CallInst::Create(module.getFunction("WriteNothing"), { buf }, "", Block_);
                BranchInst::Create(done, Block_);
            }

            Block_ = done;
        }
    }

    Function* Build() override {
        ReturnInst::Create(Codegen_->GetContext(), Block_);
        Codegen_->ExportSymbol(Func_);
        return Func_;
    }

    void GenerateRequired(Value* elemPtr, Value* buf, NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags, bool wasOptional) {
        if (type->IsData()) {
            return GenerateData(elemPtr, buf, type, nativeYtTypeFlags);
        }

        if (!wasOptional && type->IsPg()) {
            return GeneratePg(elemPtr, buf, static_cast<NKikimr::NMiniKQL::TPgType*>(type));
        }

        // call external writer
        auto& context = Codegen_->GetContext();
        const auto typeConst = ConstantInt::get(Type::getInt64Ty(context), (ui64)type);
        const auto valType = Type::getInt128Ty(context);
        const auto flagsConst = ConstantInt::get(Type::getInt64Ty(context), nativeYtTypeFlags);
        if (nativeYtTypeFlags) {
            const auto funcAddr = ConstantInt::get(Type::getInt64Ty(context), (ui64)&NYql::NCommon::WriteContainerNativeYtValue);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {
                Type::getInt64Ty(context), Type::getInt64Ty(context), PointerType::getUnqual(valType),
                PointerType::getUnqual(Type::getInt8Ty(context))
            }, false);

            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, funcAddr, PointerType::getUnqual(funType), "ptr", Block_);
            CallInst::Create(funType, funcPtr, { typeConst, flagsConst, elemPtr, buf }, "", Block_);
        } else {
            const auto funcAddr = ConstantInt::get(Type::getInt64Ty(context), (ui64)&NYql::NCommon::WriteYsonContainerValue);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {
                Type::getInt64Ty(context), Type::getInt64Ty(context), PointerType::getUnqual(valType),
                PointerType::getUnqual(Type::getInt8Ty(context))
            }, false);

            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, funcAddr, PointerType::getUnqual(funType), "ptr", Block_);
            CallInst::Create(funType, funcPtr, { typeConst, flagsConst, elemPtr, buf }, "", Block_);
        }
        if constexpr (!Flat) {
            TCodegenContext ctx(*Codegen_);
            ctx.Func = Func_;
            ValueUnRef(GetValueRepresentation(type), elemPtr, ctx, Block_);
        }
    }

    void GenerateData(Value* elemPtr, Value* buf, NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags) {
        auto& module = Codegen_->GetModule();
        auto& context = Codegen_->GetContext();
        const auto elem = new LoadInst(Type::getInt128Ty(context), elemPtr, "elem", Block_);
        const auto schemeType = static_cast<TDataType*>(type)->GetSchemeType();
        switch (schemeType) {
        case NUdf::TDataType<bool>::Id: {
            const auto data = CastInst::Create(Instruction::Trunc, elem, Type::getInt1Ty(context), "data", Block_);
            CallInst::Create(module.getFunction("WriteBool"), { buf, data }, "", Block_);
            break;
        }

        case NUdf::TDataType<i8>::Id: {
            const auto data = CastInst::Create(Instruction::Trunc, elem, Type::getInt8Ty(context), "data", Block_);
            const auto ext = CastInst::Create(Instruction::SExt, data, Type::getInt64Ty(context), "ext", Block_);
            CallInst::Create(module.getFunction("Write64"), { buf, ext }, "", Block_);
            break;
        }

        case NUdf::TDataType<ui8>::Id: {
            const auto data = CastInst::Create(Instruction::Trunc, elem, Type::getInt8Ty(context), "data", Block_);
            const auto ext = CastInst::Create(Instruction::ZExt, data, Type::getInt64Ty(context), "ext", Block_);
            CallInst::Create(module.getFunction("Write64"), { buf, ext }, "", Block_);
            break;
        }

        case NUdf::TDataType<i16>::Id: {
            const auto data = CastInst::Create(Instruction::Trunc, elem, Type::getInt16Ty(context), "data", Block_);
            const auto ext = CastInst::Create(Instruction::SExt, data, Type::getInt64Ty(context), "ext", Block_);
            CallInst::Create(module.getFunction("Write64"), { buf, ext }, "", Block_);
            break;
        }

        case NUdf::TDataType<NUdf::TDate>::Id:
        case NUdf::TDataType<ui16>::Id: {
            const auto data = CastInst::Create(Instruction::Trunc, elem, Type::getInt16Ty(context), "data", Block_);
            const auto ext = CastInst::Create(Instruction::ZExt, data, Type::getInt64Ty(context), "ext", Block_);
            CallInst::Create(module.getFunction("Write64"), { buf, ext }, "", Block_);
            break;
        }

        case NUdf::TDataType<NUdf::TDate32>::Id:
        case NUdf::TDataType<i32>::Id: {
            const auto data = CastInst::Create(Instruction::Trunc, elem, Type::getInt32Ty(context), "data", Block_);
            const auto ext = CastInst::Create(Instruction::SExt, data, Type::getInt64Ty(context), "ext", Block_);
            CallInst::Create(module.getFunction("Write64"), { buf, ext }, "", Block_);
            break;
        }

        case NUdf::TDataType<NUdf::TDatetime>::Id:
        case NUdf::TDataType<ui32>::Id: {
            const auto data = CastInst::Create(Instruction::Trunc, elem, Type::getInt32Ty(context), "data", Block_);
            const auto ext = CastInst::Create(Instruction::ZExt, data, Type::getInt64Ty(context), "ext", Block_);
            CallInst::Create(module.getFunction("Write64"), { buf, ext }, "", Block_);
            break;
        }

        case NUdf::TDataType<float>::Id: {
            const auto data = CastInst::Create(Instruction::Trunc, elem, Type::getInt32Ty(context), "data", Block_);
            CallInst::Create(module.getFunction("WriteFloat"), { buf, data }, "", Block_);
            break;
        }

        case NUdf::TDataType<double>::Id: {
            const auto data = CastInst::Create(Instruction::Trunc, elem, Type::getInt64Ty(context), "data", Block_);
            CallInst::Create(module.getFunction("WriteDouble"), { buf, data }, "", Block_);
            break;
        }

        case NUdf::TDataType<NUdf::TTimestamp>::Id:
        case NUdf::TDataType<NUdf::TInterval>::Id:
        case NUdf::TDataType<NUdf::TInterval64>::Id:
        case NUdf::TDataType<NUdf::TDatetime64>::Id:
        case NUdf::TDataType<NUdf::TTimestamp64>::Id:
        case NUdf::TDataType<ui64>::Id:
        case NUdf::TDataType<i64>::Id: {
            const auto data = CastInst::Create(Instruction::Trunc, elem, Type::getInt64Ty(context), "data", Block_);
            CallInst::Create(module.getFunction("Write64"), { buf, data }, "", Block_);
            break;
        }

        case NUdf::TDataType<NUdf::TDecimal>::Id: {
            const auto ptr = new AllocaInst(Type::getInt128Ty(context), 0U, "ptr", Block_);
            new StoreInst(GetterForInt128(elem, Block_), ptr, Block_);
            const auto velemPtr = CastInst::Create(Instruction::BitCast, ptr, PointerType::getUnqual(Type::getInt8Ty(context)), "cast", Block_);
            if (nativeYtTypeFlags & NTCF_DECIMAL) {
                auto const params = static_cast<TDataDecimalType*>(type)->GetParams();
                if (params.first < 10) {
                    CallInst::Create(module.getFunction("WriteDecimal32"), { buf, velemPtr }, "", Block_);
                } else if (params.first < 19) {
                    CallInst::Create(module.getFunction("WriteDecimal64"), { buf, velemPtr }, "", Block_);
                } else {
                    CallInst::Create(module.getFunction("WriteDecimal128"), { buf, velemPtr }, "", Block_);
                }
            } else {
                CallInst::Create(module.getFunction("Write120"), { buf, velemPtr }, "", Block_);
            }
            break;
        }

        case NUdf::TDataType<char*>::Id:
        case NUdf::TDataType<NUdf::TUtf8>::Id:
        case NUdf::TDataType<NUdf::TJson>::Id:
        case NUdf::TDataType<NUdf::TYson>::Id:
        case NUdf::TDataType<NUdf::TUuid>::Id:
        case NUdf::TDataType<NUdf::TDyNumber>::Id:
        case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
            const auto type = Type::getInt8Ty(context);
            const auto embType = FixedVectorType::get(type, 16);
            const auto cast = CastInst::Create(Instruction::BitCast, elem, embType, "cast", Block_);
            const auto mark = ExtractElementInst::Create(cast, ConstantInt::get(type, 15), "mark", Block_);

            const auto bsize = ExtractElementInst::Create(cast, ConstantInt::get(type, 14), "bsize", Block_);
            const auto esize = CastInst::Create(Instruction::ZExt, bsize, Type::getInt32Ty(context), "esize", Block_);

            const auto sizeType = Type::getInt32Ty(context);
            const auto strType = FixedVectorType::get(sizeType, 4);
            const auto four = CastInst::Create(Instruction::BitCast, elem, strType, "four", Block_);
            const auto ssize = ExtractElementInst::Create(four, ConstantInt::get(type, 2), "ssize", Block_);

            const auto cemb = CastInst::Create(Instruction::Trunc, mark, Type::getInt1Ty(context), "cemb", Block_);
            const auto size = SelectInst::Create(cemb, esize, ssize, "size", Block_);

            const auto emb = BasicBlock::Create(context, "emb", Func_);
            const auto str = BasicBlock::Create(context, "str", Func_);
            const auto done = BasicBlock::Create(context, "done", Func_);

            const auto scond = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, mark, ConstantInt::get(type, 2), "scond", Block_);
            BranchInst::Create(str, emb, scond, Block_);

            {
                Block_ = emb;

                const auto bytePtr = CastInst::Create(Instruction::BitCast, elemPtr, PointerType::getUnqual(Type::getInt8Ty(context)), "cast", Block_);
                if (schemeType == NUdf::TDataType<NUdf::TJsonDocument>::Id) {
                    const auto fnType = FunctionType::get(Type::getVoidTy(context), {
                        PointerType::getUnqual(Type::getInt8Ty(context)),
                        PointerType::getUnqual(Type::getInt8Ty(context)),
                        sizeType,
                    }, false);
                    const auto func = module.getOrInsertFunction("YtCodecWriteJsonDocument", fnType);
                    CallInst::Create(func, { buf, bytePtr, size }, "", Block_);
                } else {
                    CallInst::Create(module.getFunction("Write32"), { buf, size }, "", Block_);
                    CallInst::Create(module.getFunction("WriteString"), { buf, bytePtr, size }, "", Block_);
                }
                BranchInst::Create(done, Block_);
            }

            {
                Block_ = str;

                const auto foffs = ExtractElementInst::Create(four, ConstantInt::get(type, 3), "foffs", Block_);
                const auto offs = BinaryOperator::CreateAnd(foffs, ConstantInt::get(foffs->getType(), 0xFFFFFF), "offs", Block_);
                const auto skip = BinaryOperator::CreateAdd(offs, ConstantInt::get(offs->getType(), 16), "skip", Block_);

                const auto half = CastInst::Create(Instruction::Trunc, elem, Type::getInt64Ty(context), "half", Block_);
                const auto ptr = CastInst::Create(Instruction::IntToPtr, half, PointerType::getUnqual(type), "ptr", Block_);

                const auto bytePtr = GetElementPtrInst::CreateInBounds(Type::getInt8Ty(context), ptr, { skip }, "bptr", Block_);

                if (schemeType == NUdf::TDataType<NUdf::TJsonDocument>::Id) {
                    const auto fnType = FunctionType::get(Type::getVoidTy(context), {
                        PointerType::getUnqual(Type::getInt8Ty(context)),
                        PointerType::getUnqual(Type::getInt8Ty(context)),
                        sizeType,
                    }, false);
                    const auto func = module.getOrInsertFunction("YtCodecWriteJsonDocument", fnType);
                    CallInst::Create(func, { buf, bytePtr, size }, "", Block_);
                } else {
                    CallInst::Create(module.getFunction("Write32"), { buf, size }, "", Block_);
                    CallInst::Create(module.getFunction("WriteString"), { buf, bytePtr, size }, "", Block_);
                }

                if constexpr (!Flat) {
                    TCodegenContext ctx(*Codegen_);
                    ctx.Func = Func_;
                    ValueUnRef(EValueRepresentation::String, elemPtr, ctx, Block_);
                }

                BranchInst::Create(done, Block_);
            }

            Block_ = done;
            break;
        }

        case NUdf::TDataType<NUdf::TTzDate>::Id: {
            const auto data = CastInst::Create(Instruction::Trunc, elem, Type::getInt16Ty(context), "data", Block_);
            const auto sizeType = Type::getInt16Ty(context);
            const auto strType = FixedVectorType::get(sizeType, 8);
            const auto eight = CastInst::Create(Instruction::BitCast, elem, strType, "eight", Block_);
            const auto type = Type::getInt8Ty(context);
            const auto tzId = ExtractElementInst::Create(eight, ConstantInt::get(type, 4), "id", Block_);
            CallInst::Create(module.getFunction("WriteTzDate"), { buf, data, tzId }, "", Block_);
            break;
        }

        case NUdf::TDataType<NUdf::TTzDatetime>::Id: {
            const auto data = CastInst::Create(Instruction::Trunc, elem, Type::getInt32Ty(context), "data", Block_);
            const auto sizeType = Type::getInt16Ty(context);
            const auto strType = FixedVectorType::get(sizeType, 8);
            const auto eight = CastInst::Create(Instruction::BitCast, elem, strType, "eight", Block_);
            const auto type = Type::getInt8Ty(context);
            const auto tzId = ExtractElementInst::Create(eight, ConstantInt::get(type, 4), "id", Block_);
            CallInst::Create(module.getFunction("WriteTzDatetime"), { buf, data, tzId }, "", Block_);
            break;
        }

        case NUdf::TDataType<NUdf::TTzTimestamp>::Id: {
            const auto data = CastInst::Create(Instruction::Trunc, elem, Type::getInt64Ty(context), "data", Block_);
            const auto sizeType = Type::getInt16Ty(context);
            const auto strType = FixedVectorType::get(sizeType, 8);
            const auto eight = CastInst::Create(Instruction::BitCast, elem, strType, "eight", Block_);
            const auto type = Type::getInt8Ty(context);
            const auto tzId = ExtractElementInst::Create(eight, ConstantInt::get(type, 4), "id", Block_);
            CallInst::Create(module.getFunction("WriteTzTimestamp"), { buf, data, tzId }, "", Block_);
            break;
        }

        case NUdf::TDataType<NUdf::TTzDate32>::Id: {
            const auto data = CastInst::Create(Instruction::Trunc, elem, Type::getInt32Ty(context), "data", Block_);
            const auto sizeType = Type::getInt16Ty(context);
            const auto strType = FixedVectorType::get(sizeType, 8);
            const auto eight = CastInst::Create(Instruction::BitCast, elem, strType, "eight", Block_);
            const auto type = Type::getInt8Ty(context);
            const auto tzId = ExtractElementInst::Create(eight, ConstantInt::get(type, 4), "id", Block_);
            CallInst::Create(module.getFunction("WriteTzDate32"), { buf, data, tzId }, "", Block_);
            break;
        }

        case NUdf::TDataType<NUdf::TTzDatetime64>::Id: {
            const auto data = CastInst::Create(Instruction::Trunc, elem, Type::getInt64Ty(context), "data", Block_);
            const auto sizeType = Type::getInt16Ty(context);
            const auto strType = FixedVectorType::get(sizeType, 8);
            const auto eight = CastInst::Create(Instruction::BitCast, elem, strType, "eight", Block_);
            const auto type = Type::getInt8Ty(context);
            const auto tzId = ExtractElementInst::Create(eight, ConstantInt::get(type, 4), "id", Block_);
            CallInst::Create(module.getFunction("WriteTzDatetime64"), { buf, data, tzId }, "", Block_);
            break;
        }

        case NUdf::TDataType<NUdf::TTzTimestamp64>::Id: {
            const auto data = CastInst::Create(Instruction::Trunc, elem, Type::getInt64Ty(context), "data", Block_);
            const auto sizeType = Type::getInt16Ty(context);
            const auto strType = FixedVectorType::get(sizeType, 8);
            const auto eight = CastInst::Create(Instruction::BitCast, elem, strType, "eight", Block_);
            const auto type = Type::getInt8Ty(context);
            const auto tzId = ExtractElementInst::Create(eight, ConstantInt::get(type, 4), "id", Block_);
            CallInst::Create(module.getFunction("WriteTzTimestamp64"), { buf, data, tzId }, "", Block_);
            break;
        }

        default:
            YQL_ENSURE(false, "Unsupported data type: " << schemeType);
        }
    }

    void GeneratePg(Value* elemPtr, Value* buf, NKikimr::NMiniKQL::TPgType* type) {
        auto& context = Codegen_->GetContext();
        const auto typeConst = ConstantInt::get(Type::getInt64Ty(context), (ui64)type);
        const auto valType = Type::getInt128Ty(context);
        const auto funcAddr = ConstantInt::get(Type::getInt64Ty(context), (ui64)&NYql::NCommon::WriteSkiffPgValue);
        const auto funType = FunctionType::get(Type::getVoidTy(context), {
            Type::getInt64Ty(context), PointerType::getUnqual(valType),
            PointerType::getUnqual(Type::getInt8Ty(context))
        }, false);

        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, funcAddr, PointerType::getUnqual(funType), "ptr", Block_);
        CallInst::Create(funType, funcPtr, { typeConst, elemPtr, buf }, "", Block_);
        if constexpr (!Flat) {
            TCodegenContext ctx(*Codegen_);
            ctx.Func = Func_;
            ValueUnRef(EValueRepresentation::Any, elemPtr, ctx, Block_);
        }
    }

private:
    const std::unique_ptr<NCodegen::ICodegen>& Codegen_;
    Function* Func_;
    BasicBlock* Block_;
    ui32 Index_ = 0;
};

class TYtCodecCgReader : public IYtCodecCgReader {
public:
    TYtCodecCgReader(const std::unique_ptr<NCodegen::ICodegen>& codegen,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const void* cookie)
        : Codegen_(codegen)
        , HolderFactory_(holderFactory)
    {
        auto& module = Codegen_->GetModule();
        auto& context = Codegen_->GetContext();
        // input - pointer to array of UnboxedValue as int128 and instance of buffer, output - void
        const auto funcType = FunctionType::get(Type::getVoidTy(context), {PointerType::getUnqual(Type::getInt128Ty(context)), PointerType::getUnqual(Type::getInt8Ty(context))}, false);
        Func_ = cast<Function>(module.getOrInsertFunction((TStringBuilder() << "YtCodecCgReader." << cookie).data(), funcType).getCallee());
        Block_ = BasicBlock::Create(context, "EntryBlock", Func_);
    }

    void AddField(NKikimr::NMiniKQL::TType* type, const NKikimr::NUdf::TUnboxedValuePod& defValue, ui64 nativeYtTypeFlags) override {
        auto& module = Codegen_->GetModule();
        auto& context = Codegen_->GetContext();
        auto args = Func_->arg_begin();
        const auto valuesPtr = &*args++;
        const auto buf = &*args++;
        const auto valueType = Type::getInt128Ty(context);
        const auto indexVal = ConstantInt::get(Type::getInt32Ty(context), Index_);
        const auto elemPtr = GetElementPtrInst::CreateInBounds(valueType, valuesPtr, { indexVal }, "elemPtr", Block_);
        const auto velemPtr = CastInst::Create(Instruction::BitCast, elemPtr, PointerType::getUnqual(Type::getInt8Ty(context)), "cast", Block_);

        bool isOptional;
        auto unwrappedType = UnpackOptional(type, isOptional);
        if (isOptional || defValue) {
            const auto just = BasicBlock::Create(context, "just", Func_);
            const auto nothing = BasicBlock::Create(context, "nothing", Func_);
            const auto done = BasicBlock::Create(context, "done", Func_);

            const auto optMarker = CallInst::Create(module.getFunction("ReadOptional"), { buf }, "optMarker", Block_);
            const auto zero = ConstantInt::get(Type::getInt8Ty(context), 0);
            const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, optMarker, zero, "exists", Block_);
            BranchInst::Create(just, nothing, check, Block_);

            {
                Block_ = just;
                if (unwrappedType->IsData()) {
                    GenerateData(velemPtr, buf, static_cast<TDataType*>(unwrappedType), nativeYtTypeFlags);
                } else {
                    GenerateContainer(velemPtr, buf, unwrappedType, true, nativeYtTypeFlags);
                }

                BranchInst::Create(done, Block_);
            }

            {
                Block_ = nothing;
                BranchInst::Create(done, Block_);
            }

            Block_ = done;
        } else {
            if (unwrappedType->IsData()) {
                GenerateData(velemPtr, buf, static_cast<TDataType*>(unwrappedType), nativeYtTypeFlags);
            } else if (unwrappedType->IsPg()) {
                GeneratePg(velemPtr, buf, static_cast<TPgType*>(unwrappedType));
            } else {
                GenerateContainer(velemPtr, buf, unwrappedType, false, nativeYtTypeFlags);
            }
        }

        if (defValue) {
            // load value from elemPtr and use default if that is empty
            const auto empty = BasicBlock::Create(context, "empty", Func_);
            const auto ok = BasicBlock::Create(context, "ok", Func_);
            const auto done = BasicBlock::Create(context, "done", Func_);

            auto value = new LoadInst(valueType, elemPtr, "elem", Block_);
            const auto zero = ConstantInt::get(valueType, 0ULL);
            const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, value, zero, "empty", Block_);
            BranchInst::Create(empty, ok, check, Block_);

            {
                Block_ = empty;
                // copy def value and ref it
                ArrayRef<uint64_t> bits((uint64_t*)&defValue, 2);
                APInt defInt(128, bits);
                const auto defValData = ConstantInt::get(valueType, defInt);
                new StoreInst(defValData, elemPtr, Block_);
                TCodegenContext ctx(*Codegen_);
                ctx.Func = Func_;
                ValueAddRef(EValueRepresentation::Any, elemPtr, ctx, Block_);
                BranchInst::Create(done, Block_);
            }

            {
                Block_ = ok;
                BranchInst::Create(done, Block_);
            }

            Block_ = done;
        }

        ++Index_;
    }

    void SkipField(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags) override {
        auto& module = Codegen_->GetModule();
        auto& context = Codegen_->GetContext();
        auto args = Func_->arg_begin();
        const auto valuesPtr = &*args++;
        Y_UNUSED(valuesPtr);
        const auto buf = &*args++;

        bool isOptional;
        auto unwrappedType = UnpackOptional(type, isOptional);
        if (isOptional) {
            const auto just = BasicBlock::Create(context, "just", Func_);
            const auto nothing = BasicBlock::Create(context, "nothing", Func_);
            const auto done = BasicBlock::Create(context, "done", Func_);

            const auto optMarker = CallInst::Create(module.getFunction("ReadOptional"), { buf }, "optMarker", Block_);
            const auto zero = ConstantInt::get(Type::getInt8Ty(context), 0);
            const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, optMarker, zero, "exists", Block_);
            BranchInst::Create(just, nothing, check, Block_);

            {
                Block_ = just;
                GenerateSkip(buf, unwrappedType, nativeYtTypeFlags);
                BranchInst::Create(done, Block_);
            }

            {
                Block_ = nothing;
                BranchInst::Create(done, Block_);
            }

            Block_ = done;
        } else {
            GenerateSkip(buf, unwrappedType, nativeYtTypeFlags);
        }

        ++Index_;
    }

    void SkipOther() override {
        ++Index_;
    }

    void SkipVirtual() override {
        auto& module = Codegen_->GetModule();
        auto& context = Codegen_->GetContext();
        auto args = Func_->arg_begin();
        const auto valuesPtr = &*args++;
        const auto indexVal = ConstantInt::get(Type::getInt32Ty(context), Index_);
        const auto elemPtr = GetElementPtrInst::CreateInBounds(Type::getInt128Ty(context), valuesPtr, { indexVal }, "elemPtr", Block_);
        const auto velemPtr = CastInst::Create(Instruction::BitCast, elemPtr, PointerType::getUnqual(Type::getInt8Ty(context)), "cast", Block_);

        CallInst::Create(module.getFunction("FillZero"), { velemPtr }, "", Block_);

        ++Index_;
    }

    llvm::Function* Build() override {
        ReturnInst::Create(Codegen_->GetContext(), Block_);
        Codegen_->ExportSymbol(Func_);
        return Func_;
    }

private:
    void GenerateData(Value* velemPtr, Value* buf, TDataType* dataType, ui64 nativeYtTypeFlags) {
        auto& module = Codegen_->GetModule();
        auto& context = Codegen_->GetContext();
        const auto schemeType = dataType->GetSchemeType();
        switch (schemeType) {
        case NUdf::TDataType<bool>::Id: {
            CallInst::Create(module.getFunction("ReadBool"), { buf, velemPtr }, "", Block_);
            break;
        }
        case NUdf::TDataType<i8>::Id: {
            CallInst::Create(module.getFunction("ReadInt8"), { buf, velemPtr }, "", Block_);
            break;
        }
        case NUdf::TDataType<ui8>::Id: {
            CallInst::Create(module.getFunction("ReadUint8"), { buf, velemPtr }, "", Block_);
            break;
        }
        case NUdf::TDataType<i16>::Id: {
            CallInst::Create(module.getFunction("ReadInt16"), { buf, velemPtr }, "", Block_);
            break;
        }
        case NUdf::TDataType<NUdf::TDate>::Id:
        case NUdf::TDataType<ui16>::Id: {
            CallInst::Create(module.getFunction("ReadUint16"), { buf, velemPtr }, "", Block_);
            break;
        }
        case NUdf::TDataType<NUdf::TDate32>::Id:
        case NUdf::TDataType<i32>::Id: {
            CallInst::Create(module.getFunction("ReadInt32"), { buf, velemPtr }, "", Block_);
            break;
        }
        case NUdf::TDataType<NUdf::TDatetime>::Id:
        case NUdf::TDataType<ui32>::Id: {
            CallInst::Create(module.getFunction("ReadUint32"), { buf, velemPtr }, "", Block_);
            break;
        }
        case NUdf::TDataType<NUdf::TInterval>::Id:
        case NUdf::TDataType<NUdf::TInterval64>::Id:
        case NUdf::TDataType<NUdf::TDatetime64>::Id:
        case NUdf::TDataType<NUdf::TTimestamp64>::Id:
        case NUdf::TDataType<i64>::Id: {
            CallInst::Create(module.getFunction("ReadInt64"), { buf, velemPtr }, "", Block_);
            break;
        }
        case NUdf::TDataType<NUdf::TTimestamp>::Id:
        case NUdf::TDataType<ui64>::Id: {
            CallInst::Create(module.getFunction("ReadUint64"), { buf, velemPtr }, "", Block_);
            break;
        }
        case NUdf::TDataType<float>::Id: {
            CallInst::Create(module.getFunction("ReadFloat"), { buf, velemPtr }, "", Block_);
            break;
        }
        case NUdf::TDataType<double>::Id: {
            CallInst::Create(module.getFunction("ReadDouble"), { buf, velemPtr }, "", Block_);
            break;
        }
        case NUdf::TDataType<NUdf::TDecimal>::Id: {
            if (nativeYtTypeFlags & NTCF_DECIMAL) {
                auto const params = static_cast<TDataDecimalType*>(dataType)->GetParams();
                if (params.first < 10) {
                    CallInst::Create(module.getFunction("ReadDecimal32"), { buf, velemPtr }, "", Block_);
                } else if (params.first < 19) {
                    CallInst::Create(module.getFunction("ReadDecimal64"), { buf, velemPtr }, "", Block_);
                } else {
                    CallInst::Create(module.getFunction("ReadDecimal128"), { buf, velemPtr }, "", Block_);
                }
            } else {
                CallInst::Create(module.getFunction("ReadInt120"), { buf, velemPtr }, "", Block_);
            }
            break;
        }

        case NUdf::TDataType<char*>::Id:
        case NUdf::TDataType<NUdf::TUtf8>::Id:
        case NUdf::TDataType<NUdf::TJson>::Id:
        case NUdf::TDataType<NUdf::TYson>::Id:
        case NUdf::TDataType<NUdf::TDyNumber>::Id:
        case NUdf::TDataType<NUdf::TUuid>::Id:
        case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
            const auto fnType = FunctionType::get(Type::getVoidTy(context), {
                PointerType::getUnqual(Type::getInt8Ty(context)),
                PointerType::getUnqual(Type::getInt8Ty(context))
            }, false);

            const char* funcName = "YtCodecReadString";
            if (schemeType == NUdf::TDataType<NUdf::TJsonDocument>::Id) {
                funcName = "YtCodecReadJsonDocument";
            }

            const auto func = module.getOrInsertFunction(funcName, fnType);
            CallInst::Create(func, { buf, velemPtr }, "", Block_);
            break;
        }

        case NUdf::TDataType<NUdf::TTzDate>::Id: {
            CallInst::Create(module.getFunction("ReadTzDate"), { buf, velemPtr }, "", Block_);
            break;
        }

        case NUdf::TDataType<NUdf::TTzDatetime>::Id: {
            CallInst::Create(module.getFunction("ReadTzDatetime"), { buf, velemPtr }, "", Block_);
            break;
        }

        case NUdf::TDataType<NUdf::TTzTimestamp>::Id: {
            CallInst::Create(module.getFunction("ReadTzTimestamp"), { buf, velemPtr }, "", Block_);
            break;
        }

        case NUdf::TDataType<NUdf::TTzDate32>::Id: {
            CallInst::Create(module.getFunction("ReadTzDate32"), { buf, velemPtr }, "", Block_);
            break;
        }

        case NUdf::TDataType<NUdf::TTzDatetime64>::Id: {
            CallInst::Create(module.getFunction("ReadTzDatetime64"), { buf, velemPtr }, "", Block_);
            break;
        }

        case NUdf::TDataType<NUdf::TTzTimestamp64>::Id: {
            CallInst::Create(module.getFunction("ReadTzTimestamp64"), { buf, velemPtr }, "", Block_);
            break;
        }        

        default:
            YQL_ENSURE(false, "Unknown data type: " << schemeType);
        }
    }

    void GeneratePg(Value* velemPtr, Value* buf, TPgType* type) {
        auto& context = Codegen_->GetContext();
        const auto funcAddr = ConstantInt::get(Type::getInt64Ty(context), (ui64)&NCommon::ReadSkiffPgValue);
        const auto typeConst = ConstantInt::get(Type::getInt64Ty(context), (ui64)type);

        const auto funType = FunctionType::get(Type::getVoidTy(context), {
            Type::getInt64Ty(context), PointerType::getUnqual(Type::getInt8Ty(context)),
            PointerType::getUnqual(Type::getInt8Ty(context))
            }, false);

        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, funcAddr, PointerType::getUnqual(funType), "ptr", Block_);
        CallInst::Create(funType, funcPtr, { typeConst, velemPtr, buf}, "", Block_);
    }

    void GenerateContainer(Value* velemPtr, Value* buf, TType* type, bool wrapOptional, ui64 nativeYtTypeFlags) {
        auto& context = Codegen_->GetContext();

        const auto funcAddr = ConstantInt::get(Type::getInt64Ty(context), nativeYtTypeFlags ? (ui64)&NCommon::ReadContainerNativeYtValue : (ui64)&NCommon::ReadYsonContainerValue);
        const auto typeConst = ConstantInt::get(Type::getInt64Ty(context), (ui64)type);
        const auto holderFactoryConst = ConstantInt::get(Type::getInt64Ty(context), (ui64)&HolderFactory_);
        const auto wrapConst = ConstantInt::get(Type::getInt1Ty(context), wrapOptional);
        const auto flagsConst = ConstantInt::get(Type::getInt64Ty(context), nativeYtTypeFlags);
        if (nativeYtTypeFlags) {
            const auto funType = FunctionType::get(Type::getVoidTy(context), {
                Type::getInt64Ty(context), Type::getInt64Ty(context), Type::getInt64Ty(context), PointerType::getUnqual(Type::getInt8Ty(context)),
                PointerType::getUnqual(Type::getInt8Ty(context)), Type::getInt1Ty(context)
            }, false);

            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, funcAddr, PointerType::getUnqual(funType), "ptr", Block_);
            CallInst::Create(funType, funcPtr, { typeConst, flagsConst, holderFactoryConst, velemPtr, buf, wrapConst }, "", Block_);
        } else {
            const auto funType = FunctionType::get(Type::getVoidTy(context), {
                Type::getInt64Ty(context), Type::getInt64Ty(context), Type::getInt64Ty(context), PointerType::getUnqual(Type::getInt8Ty(context)),
                PointerType::getUnqual(Type::getInt8Ty(context)), Type::getInt1Ty(context)
            }, false);

            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, funcAddr, PointerType::getUnqual(funType), "ptr", Block_);
            CallInst::Create(funType, funcPtr, { typeConst, flagsConst, holderFactoryConst, velemPtr, buf, wrapConst }, "", Block_);
        }
    }

    void GenerateSkip(Value* buf, TType* type, ui64 nativeYtTypeFlags) {
        auto& module = Codegen_->GetModule();
        auto& context = Codegen_->GetContext();

        if (type->IsData()) {
            auto schemeType = static_cast<TDataType*>(type)->GetSchemeType();
            switch (schemeType) {
            case NUdf::TDataType<bool>::Id: {
                const auto sizeConst = ConstantInt::get(Type::getInt64Ty(context), (ui64)sizeof(ui8));
                CallInst::Create(module.getFunction("SkipFixedData"), { buf, sizeConst }, "", Block_);
                break;
            }

            case NUdf::TDataType<ui8>::Id:
            case NUdf::TDataType<ui16>::Id:
            case NUdf::TDataType<ui32>::Id:
            case NUdf::TDataType<ui64>::Id:
            case NUdf::TDataType<NUdf::TDate>::Id:
            case NUdf::TDataType<NUdf::TDatetime>::Id:
            case NUdf::TDataType<NUdf::TTimestamp>::Id: {
                const auto sizeConst = ConstantInt::get(Type::getInt64Ty(context), (ui64)sizeof(ui64));
                CallInst::Create(module.getFunction("SkipFixedData"), { buf, sizeConst }, "", Block_);
                break;
            }
            case NUdf::TDataType<i8>::Id:
            case NUdf::TDataType<i16>::Id:
            case NUdf::TDataType<i32>::Id:
            case NUdf::TDataType<i64>::Id:
            case NUdf::TDataType<NUdf::TDate32>::Id:
            case NUdf::TDataType<NUdf::TDatetime64>::Id:
            case NUdf::TDataType<NUdf::TTimestamp64>::Id:
            case NUdf::TDataType<NUdf::TInterval64>::Id:
            case NUdf::TDataType<NUdf::TInterval>::Id: {
                const auto sizeConst = ConstantInt::get(Type::getInt64Ty(context), (ui64)sizeof(i64));
                CallInst::Create(module.getFunction("SkipFixedData"), { buf, sizeConst }, "", Block_);
                break;
            }
            case NUdf::TDataType<float>::Id:
            case NUdf::TDataType<double>::Id: {
                const auto sizeConst = ConstantInt::get(Type::getInt64Ty(context), (ui64)sizeof(double));
                CallInst::Create(module.getFunction("SkipFixedData"), { buf, sizeConst }, "", Block_);
                break;
            }
            case NUdf::TDataType<NUdf::TUtf8>::Id:
            case NUdf::TDataType<char*>::Id:
            case NUdf::TDataType<NUdf::TJson>::Id:
            case NUdf::TDataType<NUdf::TYson>::Id:
            case NUdf::TDataType<NUdf::TUuid>::Id:
            case NUdf::TDataType<NUdf::TJsonDocument>::Id:
            case NUdf::TDataType<NUdf::TTzDate>::Id:
            case NUdf::TDataType<NUdf::TTzDatetime>::Id:
            case NUdf::TDataType<NUdf::TTzTimestamp>::Id:
            case NUdf::TDataType<NUdf::TTzDate32>::Id:
            case NUdf::TDataType<NUdf::TTzDatetime64>::Id:
            case NUdf::TDataType<NUdf::TTzTimestamp64>::Id: {
                CallInst::Create(module.getFunction("SkipVarData"), { buf }, "", Block_);
                break;
            }
            case NUdf::TDataType<NUdf::TDecimal>::Id: {
                if (nativeYtTypeFlags & NTCF_DECIMAL) {
                    auto const params = static_cast<TDataDecimalType*>(type)->GetParams();
                    if (params.first < 10) {
                        const auto sizeConst = ConstantInt::get(Type::getInt64Ty(context), (ui64)sizeof(i32));
                        CallInst::Create(module.getFunction("SkipFixedData"), { buf, sizeConst }, "", Block_);
                    } else if (params.first < 19) {
                        const auto sizeConst = ConstantInt::get(Type::getInt64Ty(context), (ui64)sizeof(i64));
                        CallInst::Create(module.getFunction("SkipFixedData"), { buf, sizeConst }, "", Block_);
                    } else {
                        const auto sizeConst = ConstantInt::get(Type::getInt64Ty(context), (ui64)sizeof(NDecimal::TInt128));
                        CallInst::Create(module.getFunction("SkipFixedData"), { buf, sizeConst }, "", Block_);
                    }
                } else {
                    CallInst::Create(module.getFunction("SkipVarData"), { buf }, "", Block_);
                }
                break;
            }

            default:
                YQL_ENSURE(false, "Unknown data type: " << schemeType);
            }
            return;
        }

        if (type->IsStruct()) {
            auto structType = static_cast<TStructType*>(type);
            const std::vector<size_t>* reorder = nullptr;
            if (auto cookie = structType->GetCookie()) {
                reorder = ((const std::vector<size_t>*)cookie);
            }
            for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                GenerateSkip(buf, structType->GetMemberType(reorder ? reorder->at(i) : i), nativeYtTypeFlags);
            }
            return;
        }

        if (type->IsTuple()) {
            auto tupleType = static_cast<TTupleType*>(type);
            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                GenerateSkip(buf, tupleType->GetElementType(i), nativeYtTypeFlags);
            }
            return;
        }

        if (type->IsList()) {
            auto itemType = static_cast<TListType*>(type)->GetItemType();
            const auto done = BasicBlock::Create(context, "done", Func_);
            const auto listEndMarker = ConstantInt::get(Type::getInt8Ty(context), 0xFF);
            const auto innerSkip = BasicBlock::Create(context, "innerSkip", Func_);
            const auto listContinue = BasicBlock::Create(context, "listContinue", Func_);
            BranchInst::Create(listContinue, Block_);

            {
                Block_ = innerSkip;
                GenerateSkip(buf, itemType, nativeYtTypeFlags);
                BranchInst::Create(listContinue, Block_);
            }
            {
                Block_ = listContinue;
                const auto marker = CallInst::Create(module.getFunction("ReadOptional"), { buf }, "optMarker", Block_);
                const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, marker, listEndMarker, "exists", Block_);
                BranchInst::Create(done, innerSkip, check, Block_);
            }

            Block_ = done;
            return;
        }

        if (type->IsVariant()) {
            auto varType = static_cast<TVariantType*>(type);
            const auto isOneByte = ConstantInt::get(Type::getInt8Ty(context), varType->GetAlternativesCount() < 256);
            const auto data = CallInst::Create(module.getFunction("ReadVariantData"), { buf, isOneByte }, "data", Block_);

            std::function<TType*(size_t)> getType;
            std::function<void(size_t, size_t)> genLR = [&] (size_t l, size_t r){
                size_t m = (l + r) >> 1;
                if (l == r) {
                    GenerateSkip(buf, getType(m), nativeYtTypeFlags);
                    return;
                }
                auto fn = std::to_string(l) + "_" + std::to_string(r);
                const auto currIdx = ConstantInt::get(Type::getInt16Ty(context), m);
                const auto isCurrent = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULE, data, currIdx, "isUs" + fn, Block_);
                auto lessEq = BasicBlock::Create(context, "le" + fn, Func_);
                auto greater = BasicBlock::Create(context, "g" + fn, Func_);
                auto out = BasicBlock::Create(context, "o" + fn, Func_);
                BranchInst::Create(lessEq, greater, isCurrent, Block_);
                {
                    Block_ = lessEq;
                    genLR(l, m);
                    BranchInst::Create(out, Block_);
                }
                {
                    Block_ = greater;
                    genLR(m + 1, r);
                    BranchInst::Create(out, Block_);
                }
                Block_ = out;
            };

            size_t elemCount = 0;
            if (varType->GetUnderlyingType()->IsTuple()) {
                auto tupleType = static_cast<TTupleType*>(varType->GetUnderlyingType());
                elemCount = tupleType->GetElementsCount();
                getType = [tupleType=tupleType] (size_t i) {
                    return tupleType->GetElementType(i);
                };
            } else {
                auto structType = static_cast<TStructType*>(varType->GetUnderlyingType());

                const std::vector<size_t>* reorder = nullptr;
                if (auto cookie = structType->GetCookie()) {
                    reorder = ((const std::vector<size_t>*)cookie);
                }

                elemCount = structType->GetMembersCount();

                getType = [reorder = reorder, structType=structType] (size_t i) {
                    return structType->GetMemberType(reorder ? reorder->at(i) : i);
                };
            }
            genLR(0, elemCount - 1);
            return;
        }

        if (type->IsVoid()) {
            return;
        }

        if (type->IsNull()) {
            return;
        }

        if (type->IsEmptyList() || type->IsEmptyDict()) {
            return;
        }

        if (type->IsDict()) {
            auto dictType = static_cast<TDictType*>(type);
            auto keyType = dictType->GetKeyType();
            auto payloadType = dictType->GetPayloadType();
            const auto done = BasicBlock::Create(context, "done", Func_);

            const auto innerSkip = BasicBlock::Create(context, "innerSkip", Func_);

            const auto listContinue = BasicBlock::Create(context, "listContinue", Func_);

            const auto listEndMarker = ConstantInt::get(Type::getInt8Ty(context), 0xFF);

            BranchInst::Create(listContinue, Block_);

            {
                Block_ = innerSkip;
                GenerateSkip(buf, keyType, nativeYtTypeFlags);
                GenerateSkip(buf, payloadType, nativeYtTypeFlags);
                BranchInst::Create(listContinue, Block_);
            }
            {
                Block_ = listContinue;
                const auto marker = CallInst::Create(module.getFunction("ReadOptional"), { buf }, "optMarker", Block_);
                const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, marker, listEndMarker, "exists", Block_);
                BranchInst::Create(done, innerSkip, check, Block_);
            }

            Block_ = done;
            return;
        }

        YQL_ENSURE(false, "Unsupported type for skip: " << type->GetKindAsStr());
    }

private:
    const std::unique_ptr<NCodegen::ICodegen>& Codegen_;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory_;
    Function* Func_;
    BasicBlock* Block_;
    ui32 Index_ = 0;
};

TString GetYtCodecBitCode() {
    auto bitcode = NResource::Find("/llvm_bc/YtCodecFuncs");
    return bitcode;
}

void YtCodecAddMappings(NCodegen::ICodegen& codegen) {
    codegen.AddGlobalMapping("OutputBufFlushThunk", (const void*)&NCommon::OutputBufFlushThunk);
    codegen.AddGlobalMapping("OutputBufWriteManySlowThunk", (const void*)&NCommon::OutputBufWriteManySlowThunk);
    codegen.AddGlobalMapping("InputBufReadSlowThunk", (const void*)&NCommon::InputBufReadSlowThunk);
    codegen.AddGlobalMapping("InputBufReadManySlowThunk", (const void*)&NCommon::InputBufReadManySlowThunk);
    codegen.AddGlobalMapping("InputBufSkipManySlowThunk", (const void*)&NCommon::InputBufSkipManySlowThunk);
    codegen.AddGlobalMapping("YtCodecReadString", (const void*)&YtCodecReadString);
    codegen.AddGlobalMapping("YtCodecWriteJsonDocument", (const void*)&YtCodecWriteJsonDocument);
    codegen.AddGlobalMapping("YtCodecReadJsonDocument", (const void*)&YtCodecReadJsonDocument);
    codegen.AddGlobalMapping("ThrowBadDecimal", (const void*)&ThrowBadDecimal);
}

template<bool Flat>
THolder<IYtCodecCgWriter> MakeYtCodecCgWriter(const std::unique_ptr<NCodegen::ICodegen>& codegen, const void* cookie) {
    return MakeHolder<TYtCodecCgWriter<Flat>>(codegen, cookie);
}

template THolder<IYtCodecCgWriter> MakeYtCodecCgWriter<true>(const std::unique_ptr<NCodegen::ICodegen>& codegen, const void* cookie);
template THolder<IYtCodecCgWriter> MakeYtCodecCgWriter<false>(const std::unique_ptr<NCodegen::ICodegen>& codegen, const void* cookie);

THolder<IYtCodecCgReader> MakeYtCodecCgReader(const std::unique_ptr<NCodegen::ICodegen>& codegen,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory, const void* cookie) {
    return MakeHolder<TYtCodecCgReader>(codegen, holderFactory, cookie);
}

#else
TString GetYtCodecBitCode() {
    ythrow yexception() << "No Codegen";
}

void YtCodecAddMappings(NCodegen::ICodegen& codegen) {
    Y_UNUSED(codegen);
    ythrow yexception() << "No Codegen";
}

THolder<IYtCodecCgWriter> MakeYtCodecCgWriter(const std::unique_ptr<NCodegen::ICodegen>& codegen,
    const void* cookie) {
    Y_UNUSED(codegen);
    Y_UNUSED(cookie);
    ythrow yexception() << "No Codegen";
}

THolder<IYtCodecCgReader> MakeYtCodecCgReader(const std::unique_ptr<NCodegen::ICodegen>& codegen,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const void* cookie) {
    Y_UNUSED(codegen);
    Y_UNUSED(holderFactory);
    Y_UNUSED(cookie);
    ythrow yexception() << "No Codegen";
}

#endif

}
