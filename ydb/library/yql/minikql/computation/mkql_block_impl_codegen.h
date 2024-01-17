#pragma once

#include "mkql_block_item.h"
#include "mkql_llvm_base.h"

#include <ydb/library/yql/minikql/codegen/codegen.h>

namespace NKikimr::NMiniKQL {

#ifndef MKQL_DISABLE_CODEGEN
    class TLLVMFieldsStructureBlockState: public TLLVMFieldsStructure<TComputationValue<TBlockState>> {
    private:
        using TBase = TLLVMFieldsStructure<TComputationValue<TBlockState>>;
        llvm::IntegerType*const CountType;
        llvm::PointerType*const PointerType;
        llvm::ArrayType*const SkipSpaceType;
    protected:
        using TBase::Context;
        static constexpr auto BaseFields = 3U;
    public:
        std::vector<llvm::Type*> GetFieldsArray() {
            std::vector<llvm::Type*> result = TBase::GetFields();
            result.emplace_back(CountType);
            result.emplace_back(PointerType);
            result.emplace_back(SkipSpaceType);
            return result;
        }

        llvm::Constant* GetCount() {
            return llvm::ConstantInt::get(llvm::Type::getInt32Ty(Context), TBase::GetFieldsCount() + 0);
        }

        llvm::Constant* GetPointer() {
            return llvm::ConstantInt::get(llvm::Type::getInt32Ty(Context), TBase::GetFieldsCount() + 1);
        }

        TLLVMFieldsStructureBlockState(llvm::LLVMContext& context, size_t width)
            : TBase(context)
            , CountType(llvm::Type::getInt64Ty(Context))
            , PointerType(llvm::PointerType::getUnqual(llvm::ArrayType::get(llvm::Type::getInt128Ty(Context), width)))
            , SkipSpaceType(llvm::ArrayType::get(llvm::Type::getInt64Ty(Context), 9U)) // Skip std::vectors Values & Arrays
        {}
    };
#endif
} //namespace NKikimr::NMiniKQL
