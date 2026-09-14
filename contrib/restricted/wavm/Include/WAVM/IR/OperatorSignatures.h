#pragma once

#include <initializer_list>
#include "WAVM/IR/Module.h"
#include "WAVM/IR/Operators.h"
#include "WAVM/IR/Types.h"

namespace WAVM { namespace IR {

	struct OpTypeTuple
	{
		static constexpr U8 maxTypes = 5;

		constexpr OpTypeTuple(std::initializer_list<ValueType> inTypes)
		: types{inTypes.size() > 0 ? inTypes.begin()[0] : ValueType::none,
				inTypes.size() > 1 ? inTypes.begin()[1] : ValueType::none,
				inTypes.size() > 2 ? inTypes.begin()[2] : ValueType::none,
				inTypes.size() > 3 ? inTypes.begin()[3] : ValueType::none,
				inTypes.size() > 4 ? inTypes.begin()[4] : ValueType::none}
		, numTypes(U8(inTypes.size()))
		{
		}

		ValueType operator[](Uptr index) const
		{
			WAVM_ASSERT(index < numTypes);
			return types[index];
		}

		U8 size() const { return numTypes; }

		const ValueType* data() const { return types; }

		const ValueType* begin() const { return &types[0]; }
		const ValueType* end() const { return &types[numTypes]; }

	private:
		ValueType types[maxTypes];
		U8 numTypes;
	};

	struct OpSignature
	{
		OpTypeTuple params;
		OpTypeTuple results;

		constexpr OpSignature(std::initializer_list<ValueType> inResults,
							  std::initializer_list<ValueType> inParams)
		: params(inParams), results(inResults)
		{
		}
	};

	namespace OpSignatures {
		// Monomorphic signatures.
		constexpr OpSignature none_to_none{{}, {}};
		constexpr OpSignature none_to_i32{{ValueType::i32}, {}};
		constexpr OpSignature none_to_i64{{ValueType::i64}, {}};
		constexpr OpSignature none_to_f32{{ValueType::f32}, {}};
		constexpr OpSignature none_to_f64{{ValueType::f64}, {}};
		constexpr OpSignature none_to_v128{{ValueType::v128}, {}};
		constexpr OpSignature none_to_funcref{{ValueType::funcref}, {}};

		constexpr OpSignature i32_to_i32{{ValueType::i32}, {ValueType::i32}};
		constexpr OpSignature i64_to_i64{{ValueType::i64}, {ValueType::i64}};
		constexpr OpSignature f32_to_f32{{ValueType::f32}, {ValueType::f32}};
		constexpr OpSignature f64_to_f64{{ValueType::f64}, {ValueType::f64}};
		constexpr OpSignature v128_to_v128{{ValueType::v128}, {ValueType::v128}};

		constexpr OpSignature i32_to_i64{{ValueType::i64}, {ValueType::i32}};
		constexpr OpSignature i32_to_f32{{ValueType::f32}, {ValueType::i32}};
		constexpr OpSignature i32_to_f64{{ValueType::f64}, {ValueType::i32}};
		constexpr OpSignature i32_to_v128{{ValueType::v128}, {ValueType::i32}};

		constexpr OpSignature i64_to_i32{{ValueType::i32}, {ValueType::i64}};
		constexpr OpSignature i64_to_f32{{ValueType::f32}, {ValueType::i64}};
		constexpr OpSignature i64_to_f64{{ValueType::f64}, {ValueType::i64}};
		constexpr OpSignature i64_to_v128{{ValueType::v128}, {ValueType::i64}};

		constexpr OpSignature f32_to_f64{{ValueType::f64}, {ValueType::f32}};
		constexpr OpSignature f32_to_i32{{ValueType::i32}, {ValueType::f32}};
		constexpr OpSignature f32_to_i64{{ValueType::i64}, {ValueType::f32}};
		constexpr OpSignature f32_to_v128{{ValueType::v128}, {ValueType::f32}};

		constexpr OpSignature f64_to_f32{{ValueType::f32}, {ValueType::f64}};
		constexpr OpSignature f64_to_i32{{ValueType::i32}, {ValueType::f64}};
		constexpr OpSignature f64_to_i64{{ValueType::i64}, {ValueType::f64}};
		constexpr OpSignature f64_to_v128{{ValueType::v128}, {ValueType::f64}};

		constexpr OpSignature v128_to_i32{{ValueType::i32}, {ValueType::v128}};
		constexpr OpSignature v128_to_i64{{ValueType::i64}, {ValueType::v128}};
		constexpr OpSignature v128_to_f32{{ValueType::f32}, {ValueType::v128}};
		constexpr OpSignature v128_to_f64{{ValueType::f64}, {ValueType::v128}};

		constexpr OpSignature i32_i32_to_i32{{ValueType::i32}, {ValueType::i32, ValueType::i32}};
		constexpr OpSignature i64_i64_to_i64{{ValueType::i64}, {ValueType::i64, ValueType::i64}};
		constexpr OpSignature f32_f32_to_f32{{ValueType::f32}, {ValueType::f32, ValueType::f32}};
		constexpr OpSignature f64_f64_to_f64{{ValueType::f64}, {ValueType::f64, ValueType::f64}};
		constexpr OpSignature v128_v128_to_v128{{ValueType::v128},
												{ValueType::v128, ValueType::v128}};

		constexpr OpSignature i64_i64_to_i32{{ValueType::i32}, {ValueType::i64, ValueType::i64}};
		constexpr OpSignature f32_f32_to_i32{{ValueType::i32}, {ValueType::f32, ValueType::f32}};
		constexpr OpSignature f64_f64_to_i32{{ValueType::i32}, {ValueType::f64, ValueType::f64}};

		constexpr OpSignature v128_v128_v128_to_v128{
			{ValueType::v128},
			{ValueType::v128, ValueType::v128, ValueType::v128}};

		constexpr OpSignature v128_i32_to_v128{{ValueType::v128},
											   {ValueType::v128, ValueType::i32}};
		constexpr OpSignature v128_i64_to_v128{{ValueType::v128},
											   {ValueType::v128, ValueType::i64}};
		constexpr OpSignature v128_f32_to_v128{{ValueType::v128},
											   {ValueType::v128, ValueType::f32}};
		constexpr OpSignature v128_f64_to_v128{{ValueType::v128},
											   {ValueType::v128, ValueType::f64}};

		// Memory/table index polymorphic signatures.
		inline OpSignature load(const Module& module,
								const BaseLoadOrStoreImm& imm,
								ValueType resultType)
		{
			return OpSignature({resultType},
							   {asValueType(module.memories.getType(imm.memoryIndex).indexType)});
		}
		inline OpSignature load_i32(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			return load(module, imm, ValueType::i32);
		}
		inline OpSignature load_i64(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			return load(module, imm, ValueType::i64);
		}
		inline OpSignature load_f32(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			return load(module, imm, ValueType::f32);
		}
		inline OpSignature load_f64(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			return load(module, imm, ValueType::f64);
		}
		inline OpSignature load_v128(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			return load(module, imm, ValueType::v128);
		}
		template<Uptr naturalAlignmentLog2, Uptr numLanes>
		inline OpSignature load_v128_lane(
			const Module& module,
			const LoadOrStoreLaneImm<naturalAlignmentLog2, numLanes>& imm)
		{
			return OpSignature(
				{ValueType::v128},
				{asValueType(module.memories.getType(imm.memoryIndex).indexType), ValueType::v128});
		}

		inline OpSignature store(const Module& module,
								 const BaseLoadOrStoreImm& imm,
								 ValueType valueType)
		{
			return OpSignature(
				{}, {asValueType(module.memories.getType(imm.memoryIndex).indexType), valueType});
		}
		inline OpSignature store_i32(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			return store(module, imm, ValueType::i32);
		}
		inline OpSignature store_i64(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			return store(module, imm, ValueType::i64);
		}
		inline OpSignature store_f32(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			return store(module, imm, ValueType::f32);
		}
		inline OpSignature store_f64(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			return store(module, imm, ValueType::f64);
		}
		inline OpSignature store_v128(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			return store(module, imm, ValueType::v128);
		}
		template<Uptr naturalAlignmentLog2, Uptr numLanes>
		inline OpSignature store_v128_lane(
			const Module& module,
			const LoadOrStoreLaneImm<naturalAlignmentLog2, numLanes>& imm)
		{
			return store(module, imm, ValueType::v128);
		}

		template<Uptr numVectors>
		inline OpSignature load_v128xN(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			const ValueType indexType
				= asValueType(module.memories.getType(imm.memoryIndex).indexType);
			switch(numVectors)
			{
			case 2: return OpSignature({ValueType::v128, ValueType::v128}, {indexType});
			case 3:
				return OpSignature({ValueType::v128, ValueType::v128, ValueType::v128},
								   {indexType});
			case 4:
				return OpSignature(
					{ValueType::v128, ValueType::v128, ValueType::v128, ValueType::v128},
					{indexType});
			default: WAVM_UNREACHABLE();
			};
		}
		template<Uptr numVectors>
		inline OpSignature store_v128xN(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			const ValueType indexType
				= asValueType(module.memories.getType(imm.memoryIndex).indexType);
			switch(numVectors)
			{
			case 2: return OpSignature({}, {indexType, ValueType::v128, ValueType::v128});
			case 3:
				return OpSignature({},
								   {indexType, ValueType::v128, ValueType::v128, ValueType::v128});
			case 4:
				return OpSignature({},
								   {indexType,
									ValueType::v128,
									ValueType::v128,
									ValueType::v128,
									ValueType::v128});
			default: WAVM_UNREACHABLE();
			};
		}

		inline OpSignature size(const Module& module, const MemoryImm& imm)
		{
			const ValueType indexType
				= asValueType(module.memories.getType(imm.memoryIndex).indexType);
			return OpSignature({indexType}, {});
		}
		inline OpSignature size(const Module& module, const TableImm& imm)
		{
			const ValueType indexType
				= asValueType(module.tables.getType(imm.tableIndex).indexType);
			return OpSignature({indexType}, {});
		}

		inline OpSignature grow(const Module& module, const MemoryImm& imm)
		{
			const ValueType indexType
				= asValueType(module.memories.getType(imm.memoryIndex).indexType);
			return OpSignature({indexType}, {indexType});
		}

		inline OpSignature init(const Module& module, const DataSegmentAndMemImm& imm)
		{
			const ValueType indexType
				= asValueType(module.memories.getType(imm.memoryIndex).indexType);
			return OpSignature({}, {indexType, indexType, indexType});
		}
		inline OpSignature init(const Module& module, const ElemSegmentAndTableImm& imm)
		{
			const ValueType indexType
				= asValueType(module.tables.getType(imm.tableIndex).indexType);
			return OpSignature({}, {indexType, indexType, indexType});
		}

		inline OpSignature copy(const Module& module, const MemoryCopyImm& imm)
		{
			const ValueType destIndexType
				= asValueType(module.memories.getType(imm.destMemoryIndex).indexType);
			const ValueType sourceIndexType
				= asValueType(module.memories.getType(imm.sourceMemoryIndex).indexType);
			const ValueType numBytesType
				= destIndexType == ValueType::i64 && sourceIndexType == ValueType::i64
					  ? ValueType::i64
					  : ValueType::i32;
			return OpSignature({}, {destIndexType, sourceIndexType, numBytesType});
		}
		inline OpSignature copy(const Module& module, const TableCopyImm& imm)
		{
			const ValueType destIndexType
				= asValueType(module.tables.getType(imm.destTableIndex).indexType);
			const ValueType sourceIndexType
				= asValueType(module.tables.getType(imm.sourceTableIndex).indexType);
			const ValueType numBytesType
				= destIndexType == ValueType::i64 && sourceIndexType == ValueType::i64
					  ? ValueType::i64
					  : ValueType::i32;
			return OpSignature({}, {destIndexType, sourceIndexType, numBytesType});
		}

		inline OpSignature fill(const Module& module, const MemoryImm& imm)
		{
			const ValueType indexType
				= asValueType(module.memories.getType(imm.memoryIndex).indexType);
			return OpSignature({}, {indexType, ValueType::i32, indexType});
		}
		inline OpSignature fill(const Module& module, const TableImm& imm)
		{
			const TableType& tableType = module.tables.getType(imm.tableIndex);
			const ValueType indexType = asValueType(tableType.indexType);
			const ValueType elementType = asValueType(tableType.elementType);
			return OpSignature({}, {indexType, elementType, indexType});
		}

		inline OpSignature notify(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			const ValueType indexType
				= asValueType(module.memories.getType(imm.memoryIndex).indexType);
			return OpSignature({ValueType::i32}, {indexType, ValueType::i32});
		}
		inline OpSignature wait32(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			const ValueType indexType
				= asValueType(module.memories.getType(imm.memoryIndex).indexType);
			return OpSignature({ValueType::i32}, {indexType, ValueType::i32, ValueType::i64});
		}
		inline OpSignature wait64(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			const ValueType indexType
				= asValueType(module.memories.getType(imm.memoryIndex).indexType);
			return OpSignature({ValueType::i32}, {indexType, ValueType::i64, ValueType::i64});
		}

		inline OpSignature atomicrmw(const Module& module,
									 const BaseLoadOrStoreImm& imm,
									 ValueType valueType)
		{
			const ValueType indexType
				= asValueType(module.memories.getType(imm.memoryIndex).indexType);
			return OpSignature({valueType}, {indexType, valueType});
		}
		inline OpSignature atomicrmw_i32(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			return atomicrmw(module, imm, ValueType::i32);
		}
		inline OpSignature atomicrmw_i64(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			return atomicrmw(module, imm, ValueType::i64);
		}

		inline OpSignature atomiccmpxchg(const Module& module,
										 const BaseLoadOrStoreImm& imm,
										 ValueType valueType)
		{
			const ValueType indexType
				= asValueType(module.memories.getType(imm.memoryIndex).indexType);
			return OpSignature({valueType}, {indexType, valueType, valueType});
		}
		inline OpSignature atomiccmpxchg_i32(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			return atomiccmpxchg(module, imm, ValueType::i32);
		}
		inline OpSignature atomiccmpxchg_i64(const Module& module, const BaseLoadOrStoreImm& imm)
		{
			return atomiccmpxchg(module, imm, ValueType::i64);
		}
	};
}};
