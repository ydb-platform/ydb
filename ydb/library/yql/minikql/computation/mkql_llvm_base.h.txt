#pragma once

#ifndef MKQL_DISABLE_CODEGEN
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Constant.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Constants.h>

namespace NKikimr::NMiniKQL {
template <class T>
class TLLVMFieldsStructure;

template <class T>
class TLLVMFieldsStructure<TComputationValue<T>> {
protected:
    llvm::LLVMContext& Context;
    ui32 GetFieldsCount() const {
        return FieldsCount;
    }
    const std::vector<llvm::Type*>& GetFields() const {
        return Fields;
    }
private:
    llvm::PointerType* StructPtrType;
    std::vector<llvm::Type*> Fields;
    const ui32 FieldsCount;
    std::vector<llvm::Type*> BuildFields() {
        std::vector<llvm::Type*> result;
        result.emplace_back(StructPtrType);             // vtbl
        result.emplace_back(llvm::Type::getInt32Ty(Context)); // ref
        result.emplace_back(llvm::Type::getInt16Ty(Context)); // abi
        result.emplace_back(llvm::Type::getInt16Ty(Context)); // reserved
#ifndef NDEBUG
        result.emplace_back(StructPtrType);             // meminfo
#endif
        return result;
    }

public:
    TLLVMFieldsStructure(llvm::LLVMContext& context)
        : Context(context)
        , StructPtrType(llvm::PointerType::getUnqual(llvm::StructType::get(Context)))
        , Fields(BuildFields())
        , FieldsCount(Fields.size()) {

    }

    llvm::Constant* This() const {
        return llvm::ConstantInt::get(llvm::Type::getInt32Ty(Context), 0);
    }

    const std::vector<llvm::Type*>& GetFieldsArray() const {
        return Fields;
    }
};

}
#endif
