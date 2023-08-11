#include "mkql_computation_node_pack.h"
#include "mkql_computation_node_pack_impl.h"
#include "mkql_computation_node_holders.h"
#include "presort.h"

#include <ydb/library/yql/parser/pg_wrapper/interface/pack.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <ydb/library/yql/public/decimal/yql_decimal_serialize.h>
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/pack_num.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <library/cpp/resource/resource.h>
#include <ydb/library/yql/utils/fp_bits.h>

#ifndef MKQL_DISABLE_CODEGEN
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#endif

#include <util/system/yassert.h>
#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NMiniKQL {

#ifndef MKQL_DISABLE_CODEGEN
using namespace llvm;
#endif

namespace {
#ifndef MKQL_DISABLE_CODEGEN
    TString MakeName(const TStringBuf& common, const TType* type) {
        TStringStream out;
        out << common << intptr_t(type);
        return out.Str();
    }

    BasicBlock* CreatePackBlock(const TType* type, bool useTopLength, const Module &module, LLVMContext &context, Function* pack, BasicBlock* block, Value* value, Value* buffer, Value* mask) {
        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);

        switch (type->GetKind()) {
            case TType::EKind::Data: {
                const auto dataType = static_cast<const TDataType*>(type);
                switch (*dataType->GetDataSlot()) {
                    case NUdf::EDataSlot::Bool:
                        CallInst::Create(module.getFunction("PackBool"), {value, buffer}, "", block);
                        break;
                    case NUdf::EDataSlot::Int8:
                        Y_FAIL("Not impl");
                        break;
                    case NUdf::EDataSlot::Uint8:
                        CallInst::Create(module.getFunction("PackByte"), {value, buffer}, "", block);
                        break;
                    case NUdf::EDataSlot::Int16:
                        Y_FAIL("Not impl");
                        break;
                    case NUdf::EDataSlot::Uint16:
                        Y_FAIL("Not impl");
                        break;
                    case NUdf::EDataSlot::Int32:
                        CallInst::Create(module.getFunction("PackInt32"), {value, buffer}, "", block);
                        break;
                    case NUdf::EDataSlot::Uint32:
                        CallInst::Create(module.getFunction("PackUInt32"), {value, buffer}, "", block);
                        break;
                    case NUdf::EDataSlot::Int64:
                        CallInst::Create(module.getFunction("PackInt64"), {value, buffer}, "", block);
                        break;
                    case NUdf::EDataSlot::Uint64:
                        CallInst::Create(module.getFunction("PackUInt64"), {value, buffer}, "", block);
                        break;
                    case NUdf::EDataSlot::Float:
                        CallInst::Create(module.getFunction("PackFloat"), { value, buffer }, "", block);
                        break;
                    case NUdf::EDataSlot::Double:
                        CallInst::Create(module.getFunction("PackDouble"), { value, buffer }, "", block);
                        break;
                    default:
                        CallInst::Create(module.getFunction(useTopLength ? "PackStringData" : "PackString"), {value, buffer}, "", block);
                        break;
                }

                return block;
            }
            case TType::EKind::Optional: {
                const auto optType = static_cast<const TOptionalType*>(type);

                const auto item = new AllocaInst(valueType, 0U, nullptr, llvm::Align(16), "item", block);
                const auto hasi = CallInst::Create(module.getFunction("GetOptionalValue"), {value, item, mask}, "has", block);

                const auto done = BasicBlock::Create(context, "done", pack);
                const auto fill = BasicBlock::Create(context, "fill", pack);

                const auto zero = ConstantInt::getFalse(context);
                const auto icmp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, hasi, zero, "cond", block);

                BranchInst::Create(done, fill, icmp, block);

                const auto next = CreatePackBlock(optType->GetItemType(), useTopLength, module, context, pack, fill, item, buffer, mask);
                BranchInst::Create(done, next);
                return done;
            }
            case TType::EKind::Struct: {
                const auto structType = static_cast<const TStructType*>(type);
                const auto getter = module.getFunction("GetElement");
                const auto member = new AllocaInst(valueType, 0U, nullptr, llvm::Align(16), "member", block);
                auto curr = block;
                for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                    const auto index = ConstantInt::get(Type::getInt32Ty(context), i);
                    CallInst::Create(getter, {value, index, member}, "", curr);
                    curr = CreatePackBlock(structType->GetMemberType(i), useTopLength, module, context, pack, curr, member, buffer, mask);
                }
                return curr;
            }

            case TType::EKind::Tuple: {
                const auto tupleType = static_cast<const TTupleType*>(type);
                const auto getter = module.getFunction("GetElement");
                const auto element = new AllocaInst(valueType, 0U, nullptr, llvm::Align(16), "item", block);
                auto curr = block;
                for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                    const auto index = ConstantInt::get(Type::getInt32Ty(context), i);
                    CallInst::Create(getter, {value, index, element}, "", curr);
                    curr = CreatePackBlock(tupleType->GetElementType(i), useTopLength, module, context, pack, curr, element, buffer, mask);
                }
                return curr;
            }
            case TType::EKind::Variant: {
                const auto variantType = static_cast<const TVariantType*>(type);
                const auto innerType = variantType->GetUnderlyingType();

                std::function<const TType* (ui32)> typeGetter;
                ui32 size = 0U;

                if (innerType->IsStruct()) {
                    const auto structType = static_cast<const TStructType*>(innerType);
                    typeGetter = std::bind(&TStructType::GetMemberType, structType, std::placeholders::_1);
                    size = structType->GetMembersCount();
                } else if (innerType->IsTuple()) {
                    const auto tupleType = static_cast<const TTupleType*>(innerType);
                    typeGetter = std::bind(&TTupleType::GetElementType, tupleType, std::placeholders::_1);
                    size = tupleType->GetElementsCount();
                } else {
                    THROW yexception() << "Unexpected underlying variant type: " << innerType->GetKindAsStr();
                }

                const auto variant = new AllocaInst(valueType, 0U, nullptr, llvm::Align(16), "variant", block);
                const auto index = CallInst::Create(module.getFunction("GetVariantItem"), {value, variant, buffer}, "index", block);

                const auto exit = BasicBlock::Create(context, "exit", pack);
                const auto choise = SwitchInst::Create(index, exit, size, block);

                for (ui32 i = 0; i < size; ++i) {
                    const auto var = BasicBlock::Create(context, (TString("case_") += ToString(i)).c_str(), pack);
                    choise->addCase(ConstantInt::get(Type::getInt32Ty(context), i), var);
                    const auto done = CreatePackBlock(typeGetter(i), useTopLength, module, context, pack, var, variant, buffer, mask);
                    BranchInst::Create(exit, done);
                }

                return exit;
            }

            case TType::EKind::List: {
                const auto listType = static_cast<const TListType*>(type);

                const auto iterType = Type::getInt64PtrTy(context);
                const auto zero = ConstantInt::getFalse(context);
                const auto iter = new AllocaInst(valueType, 0U, nullptr, llvm::Align(16), "iter", block);

                const auto begin = CallInst::Create(module.getFunction("GetListIterator"), {value, iter, buffer}, "iterator", block);
                const auto item = new AllocaInst(valueType, 0U, nullptr, llvm::Align(16), "item", block);

                const auto loop = BasicBlock::Create(context, "loop", pack);
                BranchInst::Create(loop, block);

                const auto next = CallInst::Create(module.getFunction("NextListItem"), {iter, item}, "next", loop);

                const auto icmp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, next, zero, "cond", loop);
                const auto exit = BasicBlock::Create(context, "exit", pack);
                const auto good = BasicBlock::Create(context, "good", pack);

                BranchInst::Create(exit, good, icmp, loop);

                const auto done = CreatePackBlock(listType->GetItemType(), useTopLength, module, context, pack, good, item, buffer, mask);
                BranchInst::Create(loop, done);
                return exit;
            }

            case TType::EKind::Dict: {
                const auto dictType = static_cast<const TDictType*>(type);

                const auto iterType = Type::getInt64PtrTy(context);
                const auto zero = ConstantInt::getFalse(context);
                const auto iter = new AllocaInst(valueType, 0U, nullptr, llvm::Align(16), "iter", block);

                const auto begin = CallInst::Create(module.getFunction("GetDictIterator"), {value, iter, buffer}, "iterator", block);
                const auto first = new AllocaInst(valueType, 0U, nullptr, llvm::Align(16), "first", block);
                const auto second = new AllocaInst(valueType, 0U, nullptr, llvm::Align(16), "second", block);

                const auto loop = BasicBlock::Create(context, "loop", pack);
                BranchInst::Create(loop, block);

                const auto next = CallInst::Create(module.getFunction("NextDictItem"), {iter, first, second}, "next", loop);

                const auto icmp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, next, zero, "cond", loop);
                const auto exit = BasicBlock::Create(context, "exit", pack);
                const auto good = BasicBlock::Create(context, "good", pack);

                BranchInst::Create(exit, good, icmp, loop);

                const auto one = CreatePackBlock(dictType->GetKeyType(), useTopLength, module, context, pack, good, first, buffer, mask);
                const auto two = CreatePackBlock(dictType->GetPayloadType(), useTopLength, module, context, pack, one, second, buffer, mask);

                BranchInst::Create(loop, two);
                return exit;
            }
        }
        Y_UNREACHABLE();
    }

    Function* CreatePackFunction(const TType* type, bool useTopLength, Module &module, LLVMContext &context) {
        const auto& name = MakeName("Pack:", type);
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto packFuncType = FunctionType::get(Type::getVoidTy(context), {ptrValueType, Type::getInt64PtrTy(context), Type::getInt64PtrTy(context)}, false);
        const auto pack = cast<Function>(module.getOrInsertFunction(name.c_str(), packFuncType).getCallee());

        auto argsIt = pack->arg_begin();
        const auto value = argsIt;
        const auto buffer = ++argsIt;
        const auto mask = ++argsIt;

        value->setName("Value");
        buffer->setName("Buffer");
        mask->setName("Mask");

        const auto main = BasicBlock::Create(context, "main", pack);
        const auto last = CreatePackBlock(type, useTopLength, module, context, pack, main, &*value, &*buffer, &*mask);
        ReturnInst::Create(context, last);

//        pack->addFnAttr("target-cpu", "x86-64");
//        pack->addFnAttr("target-features", "+sse,+sse2,+sse3");

        return pack;
    }
#endif
}

TValuePacker::TValuePacker(bool stable, const TType* type, bool tryUseCodegen)
#ifndef MKQL_DISABLE_CODEGEN
#ifdef __llvm__
    : Codegen(tryUseCodegen ? NYql::NCodegen::ICodegen::Make(NYql::NCodegen::ETarget::Native) : NYql::NCodegen::ICodegen::TPtr())
#else
    : Codegen()
#endif
    , Stable(stable)
#else
    : Stable(stable)
#endif
    , Type(type)
    , Properties(ScanTypeProperties(Type))
    , OptionalMaskReserve(Properties.Test(EProps::UseOptionalMask) ? 1 : 0)
    , PackFunc(MakePackFunction())
{
#ifndef MKQL_DISABLE_CODEGEN
    if (Codegen) {
        Codegen->Verify();
        Codegen->Compile();
    }
#else
    Y_UNUSED(tryUseCodegen);
#endif
}

TValuePacker::TValuePacker(const TValuePacker& other)
    : Stable(other.Stable)
    , Type(other.Type)
    , Properties(other.Properties)
    , OptionalMaskReserve(other.OptionalMaskReserve)
    , PackFunc(other.PackFunc)
{}

std::pair<ui32, bool> TValuePacker::SkipEmbeddedLength(TStringBuf& buf) {
    ui32 length = 0;
    bool emptySingleOptional = false;
    if (buf.size() > 8) {
        length = ReadUnaligned<ui32>(buf.data());
        MKQL_ENSURE(length + 4 == buf.size(), "Bad packed data. Invalid embedded size");
        buf.Skip(4);
    } else {
        length = *buf.data();
        MKQL_ENSURE(length & 1, "Bad packed data. Invalid embedded size");
        emptySingleOptional = 0 != (length & 0x10);
        length = (length & 0x0f) >> 1;
        MKQL_ENSURE(length + 1 == buf.size(), "Bad packed data. Invalid embedded size");
        buf.Skip(1);
    }
    return {length, emptySingleOptional};
}

NUdf::TUnboxedValue TValuePacker::Unpack(TStringBuf buf, const THolderFactory& holderFactory) const {
    auto pair = SkipEmbeddedLength(buf);
    ui32 length = pair.first;
    bool emptySingleOptional = pair.second;

    if (Properties.Test(EProps::UseOptionalMask)) {
        OptionalUsageMask.Reset(buf);
    }
    NUdf::TUnboxedValue res;
    if (Properties.Test(EProps::SingleOptional) && emptySingleOptional) {
        res = NUdf::TUnboxedValuePod();
    } else if (Type->IsStruct()) {
        auto structType = static_cast<const TStructType*>(Type);
        NUdf::TUnboxedValue * items = nullptr;
        res = TopStruct.NewArray(holderFactory, structType->GetMembersCount(), items);
        for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
            auto memberType = structType->GetMemberType(index);
            *items++ = UnpackImpl(memberType, buf, length, holderFactory);
        }
    } else {
        res = UnpackImpl(Type, buf, length, holderFactory);
    }

    MKQL_ENSURE(buf.empty(), "Bad packed data. Not fully data read");
    return res;
}

NUdf::TUnboxedValue TValuePacker::UnpackImpl(const TType* type, TStringBuf& buf, ui32 topLength,
    const THolderFactory& holderFactory) const
{
    switch (type->GetKind()) {
    case TType::EKind::Void:
        return NUdf::TUnboxedValuePod::Void();
    case TType::EKind::Null:
        return NUdf::TUnboxedValuePod();
    case TType::EKind::EmptyList:
        return holderFactory.GetEmptyContainer();
    case TType::EKind::EmptyDict:
        return holderFactory.GetEmptyContainer();

    case TType::EKind::Data: {
        auto dataType = static_cast<const TDataType*>(type);
        switch (*dataType->GetDataSlot()) {
        case NUdf::EDataSlot::Bool:
            return NUdf::TUnboxedValuePod(NDetails::GetRawData<bool>(buf));
        case NUdf::EDataSlot::Int8:
            return NUdf::TUnboxedValuePod(NDetails::GetRawData<i8>(buf));
        case NUdf::EDataSlot::Uint8:
            return NUdf::TUnboxedValuePod(NDetails::GetRawData<ui8>(buf));
        case NUdf::EDataSlot::Int16:
            return NUdf::TUnboxedValuePod(NDetails::UnpackInt16(buf));
        case NUdf::EDataSlot::Uint16:
            return NUdf::TUnboxedValuePod(NDetails::UnpackUInt16(buf));
        case NUdf::EDataSlot::Int32:
            return NUdf::TUnboxedValuePod(NDetails::UnpackInt32(buf));
        case NUdf::EDataSlot::Uint32:
            return NUdf::TUnboxedValuePod(NDetails::UnpackUInt32(buf));
        case NUdf::EDataSlot::Int64:
            return NUdf::TUnboxedValuePod(NDetails::UnpackInt64(buf));
        case NUdf::EDataSlot::Uint64:
            return NUdf::TUnboxedValuePod(NDetails::UnpackUInt64(buf));
        case NUdf::EDataSlot::Float:
            return NUdf::TUnboxedValuePod(NDetails::GetRawData<float>(buf));
        case NUdf::EDataSlot::Double:
            return NUdf::TUnboxedValuePod(NDetails::GetRawData<double>(buf));
        case NUdf::EDataSlot::Date:
            return NUdf::TUnboxedValuePod(NDetails::UnpackUInt16(buf));
        case NUdf::EDataSlot::Datetime:
            return NUdf::TUnboxedValuePod(NDetails::UnpackUInt32(buf));
        case NUdf::EDataSlot::Timestamp:
            return NUdf::TUnboxedValuePod(NDetails::UnpackUInt64(buf));
        case NUdf::EDataSlot::Interval:
            return NUdf::TUnboxedValuePod(NDetails::UnpackInt64(buf));
        case NUdf::EDataSlot::TzDate: {
            auto value = NDetails::UnpackUInt16(buf);
            auto tzId = NDetails::UnpackUInt16(buf);
            auto ret = NUdf::TUnboxedValuePod(value);
            ret.SetTimezoneId(tzId);
            return ret;
        }
        case NUdf::EDataSlot::TzDatetime: {
            auto value = NDetails::UnpackUInt32(buf);
            auto tzId = NDetails::UnpackUInt16(buf);
            auto ret = NUdf::TUnboxedValuePod(value);
            ret.SetTimezoneId(tzId);
            return ret;
        }
        case NUdf::EDataSlot::TzTimestamp: {
            auto value = NDetails::UnpackUInt64(buf);
            auto tzId = NDetails::UnpackUInt16(buf);
            auto ret = NUdf::TUnboxedValuePod(value);
            ret.SetTimezoneId(tzId);
            return ret;
        }
        case NUdf::EDataSlot::Uuid: {
            MKQL_ENSURE(16 <= buf.size(), "Bad packed data. Buffer too small");
            const char* ptr = buf.data();
            buf.Skip(16);
            return MakeString(NUdf::TStringRef(ptr, 16));
        }
        case NUdf::EDataSlot::Decimal: {
            const auto des = NYql::NDecimal::Deserialize(buf.data(), buf.size());
            MKQL_ENSURE(!NYql::NDecimal::IsError(des.first), "Bad packed data: invalid decimal.");
            buf.Skip(des.second);
            return NUdf::TUnboxedValuePod(des.first);
        }
        default:
            ui32 size = 0;
            if (Properties.Test(EProps::UseTopLength)) {
                size = topLength;
            } else {
                size = NDetails::UnpackUInt32(buf);
            }
            MKQL_ENSURE(size <= buf.size(), "Bad packed data. Buffer too small");
            const char* ptr = buf.data();
            buf.Skip(size);
            return MakeString(NUdf::TStringRef(ptr, size));
        }
        break;
    }

    case TType::EKind::Optional: {
        auto optionalType = static_cast<const TOptionalType*>(type);
        if (!OptionalUsageMask.IsNextEmptyOptional()) {
            return UnpackImpl(optionalType->GetItemType(), buf, topLength, holderFactory).Release().MakeOptional();
        }
        else {
            return NUdf::TUnboxedValuePod();
        }
    }

    case TType::EKind::Pg: {
        auto pgType = static_cast<const TPgType*>(type);
        if (!OptionalUsageMask.IsNextEmptyOptional()) {
            return PGUnpackImpl(pgType, buf);
        }
        else {
            return NUdf::TUnboxedValuePod();
        }
    }

    case TType::EKind::List: {
        auto listType = static_cast<const TListType*>(type);
        auto itemType = listType->GetItemType();
        const auto len = NDetails::UnpackUInt64(buf);
        if (!len) {
            return holderFactory.GetEmptyContainer();
        }

        TTemporaryUnboxedValueVector tmp;
        for (ui64 i = 0; i < len; ++i) {
            tmp.emplace_back(UnpackImpl(itemType, buf, topLength, holderFactory));
        }

        NUdf::TUnboxedValue *items = nullptr;
        auto list = holderFactory.CreateDirectArrayHolder(len, items);
        for (ui64 i = 0; i < len; ++i) {
            items[i] = std::move(tmp[i]);
        }

        return std::move(list);
    }

    case TType::EKind::Struct: {
        auto structType = static_cast<const TStructType*>(type);
        NUdf::TUnboxedValue* itemsPtr = nullptr;
        auto res = holderFactory.CreateDirectArrayHolder(structType->GetMembersCount(), itemsPtr);
        for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
            auto memberType = structType->GetMemberType(index);
            itemsPtr[index] = UnpackImpl(memberType, buf, topLength, holderFactory);
        }
        return std::move(res);
    }

    case TType::EKind::Tuple: {
        auto tupleType = static_cast<const TTupleType*>(type);
        NUdf::TUnboxedValue* itemsPtr = nullptr;
        auto res = holderFactory.CreateDirectArrayHolder(tupleType->GetElementsCount(), itemsPtr);
        for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
            auto elementType = tupleType->GetElementType(index);
            itemsPtr[index] = UnpackImpl(elementType, buf, topLength, holderFactory);
        }
        return std::move(res);
    }

    case TType::EKind::Dict: {
        auto dictType = static_cast<const TDictType*>(type);
        auto keyType = dictType->GetKeyType();
        auto payloadType = dictType->GetPayloadType();
        auto dictBuilder = holderFactory.NewDict(dictType, NUdf::TDictFlags::EDictKind::Hashed);

        ui64 len = NDetails::UnpackUInt64(buf);
        for (ui64 i = 0; i < len; ++i) {
            auto key = UnpackImpl(keyType, buf, topLength, holderFactory);
            auto payload = UnpackImpl(payloadType, buf, topLength, holderFactory);
            dictBuilder->Add(std::move(key), std::move(payload));
        }
        return dictBuilder->Build();
    }

    case TType::EKind::Variant: {
        auto variantType = static_cast<const TVariantType*>(type);
        ui32 variantIndex = NDetails::UnpackUInt32(buf);
        TType* innerType = variantType->GetUnderlyingType();
        if (innerType->IsStruct()) {
            MKQL_ENSURE(variantIndex < static_cast<TStructType*>(innerType)->GetMembersCount(), "Bad variant index: " << variantIndex);
            innerType = static_cast<TStructType*>(innerType)->GetMemberType(variantIndex);
        } else {
            MKQL_ENSURE(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
            MKQL_ENSURE(variantIndex < static_cast<TTupleType*>(innerType)->GetElementsCount(), "Bad variant index: " << variantIndex);
            innerType = static_cast<TTupleType*>(innerType)->GetElementType(variantIndex);
        }
        return holderFactory.CreateVariantHolder(UnpackImpl(innerType, buf, topLength, holderFactory).Release(), variantIndex);
    }

    case TType::EKind::Tagged: {
        auto taggedType = static_cast<const TTaggedType*>(type);
        return UnpackImpl(taggedType->GetBaseType(), buf, topLength, holderFactory);
    }

    default:
        THROW yexception() << "Unsupported type: " << type->GetKindAsStr();
    }
}

TStringBuf TValuePacker::Pack(const NUdf::TUnboxedValuePod& value) const {
    OptionalUsageMask.Reset();
    const size_t lengthReserve = sizeof(ui32);
    Buffer.Proceed(lengthReserve + OptionalMaskReserve);

    if (PackFunc)
        PackFunc(reinterpret_cast<const TRawUV*>(&value), reinterpret_cast<ui64*>(&Buffer), reinterpret_cast<ui64*>(&OptionalUsageMask));
    else
        PackImpl(Type, value);

    size_t delta = 0;
    size_t len = Buffer.Size();

    if (Properties.Test(EProps::UseOptionalMask)) {
        // Prepend optional mask
        const size_t actualOptionalMaskSize = OptionalUsageMask.CalcSerializedSize();

        if (actualOptionalMaskSize > OptionalMaskReserve) {
            TBuffer buf(Buffer.Size() + actualOptionalMaskSize - OptionalMaskReserve);
            buf.Proceed(actualOptionalMaskSize - OptionalMaskReserve);
            buf.Append(Buffer.Data(), Buffer.Size());
            Buffer.Swap(buf);
            OptionalMaskReserve = actualOptionalMaskSize;
            len = Buffer.Size();
        }

        delta = OptionalMaskReserve - actualOptionalMaskSize;
        Buffer.Proceed(lengthReserve + delta);
        OptionalUsageMask.Serialize(Buffer);
    }

    // Prepend length
    if (len - delta - lengthReserve > 7) {
        const ui32 length = len - delta - lengthReserve;
        Buffer.Proceed(delta);
        Buffer.Append((const char*)&length, sizeof(length));
        // Long length always singnals non-empty optional. So, don't check EProps::SingleOptional here
    } else {
        ui8 length = 1 | ((len - delta - lengthReserve) << 1);
        // Empty root optional always has short length. Embed empty flag into the length
        if (Properties.Test(EProps::SingleOptional) && !OptionalUsageMask.IsEmptyMask()) {
            length |= 0x10;
        }
        delta += 3;
        Buffer.Proceed(delta);
        Buffer.Append((const char*)&length, sizeof(length));
    }
    NSan::Unpoison(Buffer.Data() + delta, len - delta);
    return TStringBuf(Buffer.Data() + delta, len - delta);
}

void TValuePacker::PackImpl(const TType* type, const NUdf::TUnboxedValuePod& value) const {
    switch (type->GetKind()) {
    case TType::EKind::Void:
        break;
    case TType::EKind::Null:
        break;
    case TType::EKind::EmptyList:
        break;
    case TType::EKind::EmptyDict:
        break;

    case TType::EKind::Data: {
        auto dataType = static_cast<const TDataType*>(type);
        switch (*dataType->GetDataSlot()) {
        case NUdf::EDataSlot::Bool:
            NDetails::PutRawData(value.Get<bool>(), Buffer);
            break;
        case NUdf::EDataSlot::Int8:
            NDetails::PutRawData(value.Get<i8>(), Buffer);
            break;
        case NUdf::EDataSlot::Uint8:
            NDetails::PutRawData(value.Get<ui8>(), Buffer);
            break;
        case NUdf::EDataSlot::Int16:
            NDetails::PackInt16(value.Get<i16>(), Buffer);
            break;
        case NUdf::EDataSlot::Uint16:
            NDetails::PackUInt16(value.Get<ui16>(), Buffer);
            break;
        case NUdf::EDataSlot::Int32:
            NDetails::PackInt32(value.Get<i32>(), Buffer);
            break;
        case NUdf::EDataSlot::Uint32:
            NDetails::PackUInt32(value.Get<ui32>(), Buffer);
            break;
        case NUdf::EDataSlot::Int64:
            NDetails::PackInt64(value.Get<i64>(), Buffer);
            break;
        case NUdf::EDataSlot::Uint64:
            NDetails::PackUInt64(value.Get<ui64>(), Buffer);
            break;
        case NUdf::EDataSlot::Float: {
            float x = value.Get<float>();
            if (Stable) {
                NYql::CanonizeFpBits<float>(&x);
            }

            NDetails::PutRawData(x, Buffer);
            break;
        }
        case NUdf::EDataSlot::Double: {
            double x = value.Get<double>();
            if (Stable) {
                NYql::CanonizeFpBits<double>(&x);
            }

            NDetails::PutRawData(x, Buffer);
            break;
        }
        case NUdf::EDataSlot::Date:
            NDetails::PackUInt32(value.Get<ui16>(), Buffer);
            break;
        case NUdf::EDataSlot::Datetime:
            NDetails::PackUInt32(value.Get<ui32>(), Buffer);
            break;
        case NUdf::EDataSlot::Timestamp:
            NDetails::PackUInt64(value.Get<ui64>(), Buffer);
            break;
        case NUdf::EDataSlot::Interval:
            NDetails::PackInt64(value.Get<i64>(), Buffer);
            break;
        case NUdf::EDataSlot::Uuid: {
            auto ref = value.AsStringRef();
            Buffer.Append(ref.Data(), ref.Size());
            break;
        }
        case NUdf::EDataSlot::TzDate: {
            NDetails::PackUInt16(value.Get<ui16>(), Buffer);
            NDetails::PackUInt16(value.GetTimezoneId(), Buffer);
            break;
        }
        case NUdf::EDataSlot::TzDatetime: {
            NDetails::PackUInt32(value.Get<ui32>(), Buffer);
            NDetails::PackUInt16(value.GetTimezoneId(), Buffer);
            break;
        }
        case NUdf::EDataSlot::TzTimestamp: {
            NDetails::PackUInt64(value.Get<ui64>(), Buffer);
            NDetails::PackUInt16(value.GetTimezoneId(), Buffer);
            break;
        }
        case NUdf::EDataSlot::Decimal: {
            char buff[0x10U];
            Buffer.Append(buff, NYql::NDecimal::Serialize(value.GetInt128(), buff));
            break;
        }
        default: {
            auto stringRef = value.AsStringRef();
            if (!Properties.Test(EProps::UseTopLength)) {
                NDetails::PackUInt32(stringRef.Size(), Buffer);
            }
            Buffer.Append(stringRef.Data(), stringRef.Size());
        }
        }
        break;
    }

    case TType::EKind::Optional: {
        auto optionalType = static_cast<const TOptionalType*>(type);
        OptionalUsageMask.SetNextEmptyOptional(!value);
        if (value) {
            PackImpl(optionalType->GetItemType(), value.GetOptionalValue());
        }
        break;
    }

    case TType::EKind::Pg: {
        auto pgType = static_cast<const TPgType*>(type);
        OptionalUsageMask.SetNextEmptyOptional(!value);
        if (value) {
            PGPackImpl(Stable, pgType, value, Buffer);
        }
        break;
    }

    case TType::EKind::List: {
        auto listType = static_cast<const TListType*>(type);
        auto itemType = listType->GetItemType();
        if (value.HasFastListLength()) {
            auto len = value.GetListLength();
            NDetails::PackUInt64(len, Buffer);
            if (len) {
                if (auto p = value.GetElements()) {
                    value.GetListIterator();
                    do PackImpl(itemType, *p++);
                    while (--len);
                } else if (const auto iter = value.GetListIterator()) {
                    for (NUdf::TUnboxedValue item; iter.Next(item); PackImpl(itemType, item))
                        continue;
                }
            }
        } else {
            TUnboxedValueVector items;
            const auto iter = value.GetListIterator();
            for (NUdf::TUnboxedValue item; iter.Next(item);) {
                items.emplace_back(std::move(item));
            }

            NDetails::PackUInt64(items.size(), Buffer);
            for (const auto& item : items) {
                PackImpl(itemType, item);
            }
        }
        break;
    }

    case TType::EKind::Struct: {
        auto structType = static_cast<const TStructType*>(type);
        for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
            auto memberType = structType->GetMemberType(index);
            PackImpl(memberType, value.GetElement(index));
        }
        break;
    }

    case TType::EKind::Tuple: {
        auto tupleType = static_cast<const TTupleType*>(type);
        for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
            auto elementType = tupleType->GetElementType(index);
            PackImpl(elementType, value.GetElement(index));
        }
        break;
    }

    case TType::EKind::Dict:  {
        auto dictType = static_cast<const TDictType*>(type);
        auto keyType = dictType->GetKeyType();
        auto payloadType = dictType->GetPayloadType();

        auto length = value.GetDictLength();
        NDetails::PackUInt64(length, Buffer);
        const auto iter = value.GetDictIterator();
        if (Stable && !value.IsSortedDict()) {
            // no key duplicates here
            TKeyTypes types;
            bool isTuple;
            bool encoded;
            bool useIHash;
            GetDictionaryKeyTypes(keyType, types, isTuple, encoded, useIHash);
            if (encoded) {
                TGenericPresortEncoder packer(keyType);
                decltype(EncodedDictBuffers)::value_type dictBuffer;
                if (!EncodedDictBuffers.empty()) {
                    dictBuffer = std::move(EncodedDictBuffers.back());
                    EncodedDictBuffers.pop_back();
                    dictBuffer.clear();
                }
                dictBuffer.reserve(length);
                for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                    NUdf::TUnboxedValue encodedKey = MakeString(packer.Encode(key, false));
                    dictBuffer.emplace_back(std::move(encodedKey), std::move(key), std::move(payload));
                }

                Sort(dictBuffer.begin(), dictBuffer.end(), [&](const auto& left, const auto& right) {
                    return CompareKeys(std::get<0>(left), std::get<0>(right), types, isTuple) < 0;
                });

                for (const auto& x : dictBuffer) {
                    PackImpl(keyType, std::get<1>(x));
                    PackImpl(payloadType, std::get<2>(x));
                }
                dictBuffer.clear();
                EncodedDictBuffers.push_back(std::move(dictBuffer));
            } else {
                decltype(DictBuffers)::value_type dictBuffer;
                if (!DictBuffers.empty()) {
                    dictBuffer = std::move(DictBuffers.back());
                    DictBuffers.pop_back();
                    dictBuffer.clear();
                }
                dictBuffer.reserve(length);
                for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                    dictBuffer.emplace_back(std::move(key), std::move(payload));
                }

                NUdf::ICompare::TPtr cmp = useIHash ? MakeCompareImpl(keyType) : nullptr;
                Sort(dictBuffer.begin(), dictBuffer.end(), TKeyPayloadPairLess(types, isTuple, cmp.Get()));
                for (const auto& p: dictBuffer) {
                    PackImpl(keyType, p.first);
                    PackImpl(payloadType, p.second);
                }
                dictBuffer.clear();
                DictBuffers.push_back(std::move(dictBuffer));
            }
        } else {
            for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                PackImpl(keyType, key);
                PackImpl(payloadType, payload);
            }
        }
        break;
    }

    case TType::EKind::Variant: {
        auto variantType = static_cast<const TVariantType*>(type);
        ui32 variantIndex = value.GetVariantIndex();
        TType* innerType = variantType->GetUnderlyingType();
        if (innerType->IsStruct()) {
            innerType = static_cast<TStructType*>(innerType)->GetMemberType(variantIndex);
        } else {
            MKQL_ENSURE(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
            innerType = static_cast<TTupleType*>(innerType)->GetElementType(variantIndex);
        }
        NDetails::PackUInt32(variantIndex, Buffer);
        PackImpl(innerType, value.GetVariantItem());
        break;
    }

    default:
        THROW yexception() << "Unsupported type: " << type->GetKindAsStr();
    }
}



TValuePacker::TProperties TValuePacker::ScanTypeProperties(const TType* type) {
    TProperties props;
    if (HasOptionalFields(type)) {
        props.Set(EProps::UseOptionalMask);
    }
    if (type->GetKind() == TType::EKind::Optional) {
        type = static_cast<const TOptionalType*>(type)->GetItemType();
        if (!HasOptionalFields(type)) {
            props.Set(EProps::SingleOptional);
            props.Reset(EProps::UseOptionalMask);
        }
    }
    // Here and after the type is unwrapped!!

    if (type->GetKind() == TType::EKind::Data) {
        auto dataType = static_cast<const TDataType*>(type);
        switch (*dataType->GetDataSlot()) {
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Json:
        case NUdf::EDataSlot::Yson:
        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::JsonDocument:
            // Reuse entire packed value length for strings
            props.Set(EProps::UseTopLength);
            break;
        default:
            break;
        }
    }
    return props;
}

bool TValuePacker::HasOptionalFields(const TType* type) {
    switch (type->GetKind()) {
    case TType::EKind::Void:
    case TType::EKind::Null:
    case TType::EKind::EmptyList:
    case TType::EKind::EmptyDict:
    case TType::EKind::Data:
        return false;

    case TType::EKind::Optional:
        return true;

    case TType::EKind::Pg:
        return true;

    case TType::EKind::List:
        return HasOptionalFields(static_cast<const TListType*>(type)->GetItemType());

    case TType::EKind::Struct: {
        auto structType = static_cast<const TStructType*>(type);
        for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
            if (HasOptionalFields(structType->GetMemberType(index))) {
                return true;
            }
        }
        return false;
    }

    case TType::EKind::Tuple: {
        auto tupleType = static_cast<const TTupleType*>(type);
        for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
            if (HasOptionalFields(tupleType->GetElementType(index))) {
                return true;
            }
        }
        return false;
    }

    case TType::EKind::Dict:  {
        auto dictType = static_cast<const TDictType*>(type);
        return HasOptionalFields(dictType->GetKeyType()) || HasOptionalFields(dictType->GetPayloadType());
    }

    case TType::EKind::Variant:  {
        auto variantType = static_cast<const TVariantType*>(type);
        return HasOptionalFields(variantType->GetUnderlyingType());
    }

    case TType::EKind::Tagged:  {
        auto taggedType = static_cast<const TTaggedType*>(type);
        return HasOptionalFields(taggedType->GetBaseType());
    }

    default:
        THROW yexception() << "Unsupported type: " << type->GetKindAsStr();
    }
}

TValuePacker::TPackFunction
TValuePacker::MakePackFunction() {
#ifdef MKQL_DISABLE_CODEGEN
    return nullptr;
#else
    if (!Codegen)
        return nullptr;

    Codegen->LoadBitCode(NResource::Find("/llvm_bc/mkql_pack.bc"), "mkql_pack");
    return reinterpret_cast<TPackFunction>(Codegen->GetPointerToFunction(CreatePackFunction(Type, Properties.Test(EProps::UseTopLength), Codegen->GetModule(), Codegen->GetContext())));
#endif
}

TValuePackerBoxed::TValuePackerBoxed(TMemoryUsageInfo* memInfo, bool stable, const TType* type, bool tryUseCodegen)
    : TBase(memInfo)
    , TValuePacker(stable, type, tryUseCodegen)
{}

TValuePackerBoxed::TValuePackerBoxed(TMemoryUsageInfo* memInfo, const TValuePacker& other)
    : TBase(memInfo)
    , TValuePacker(other)
{}

} // NMiniKQL
} // NKikimr
