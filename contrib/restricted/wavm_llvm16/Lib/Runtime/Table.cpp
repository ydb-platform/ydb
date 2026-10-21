#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <vector>
#include "RuntimePrivate.h"
#include "WAVM/IR/Types.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/LLVMJIT/LLVMJIT.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/Platform/Intrinsic.h"
#include "WAVM/Platform/Memory.h"
#include "WAVM/Platform/RWMutex.h"
#include "WAVM/Runtime/Runtime.h"
#include "WAVM/RuntimeABI/RuntimeABI.h"

using namespace WAVM;
using namespace WAVM::Runtime;

namespace WAVM { namespace Runtime {
	WAVM_DEFINE_INTRINSIC_MODULE(wavmIntrinsicsTable)
}}

// Global lists of tables; used to query whether an address is reserved by one of them.
static Platform::RWMutex tablesMutex;
static std::vector<Table*> tables;

static thread_local Table* CurrentTable = nullptr;
__attribute__((__noinline__)) void Table::setCurrentTable(Table* table)
{
	CurrentTable = table;
}
__attribute__((__noinline__)) Table* Table::getCurrentTable()
{
	return CurrentTable;
}

static constexpr Uptr numGuardPages = 1;
static constexpr U64 maxTable32Elems = U64(UINT32_MAX) + 1;
static constexpr U64 maxTable64Elems = U64(128) * 1024 * 1024 * 1024;

static Uptr getNumPlatformPages(Uptr numBytes)
{
	return (numBytes + Platform::getBytesPerPage() - 1) >> Platform::getBytesPerPageLog2();
}

static Function* makeDummyFunction(const char* debugName)
{
	FunctionMutableData* functionMutableData = new FunctionMutableData(debugName);
	Function* function
		= new Function(functionMutableData, UINTPTR_MAX, IR::FunctionType::Encoding{0});
	functionMutableData->function = function;
	return function;
}

Object* Runtime::getOutOfBoundsElement()
{
	static Function* function = makeDummyFunction("out-of-bounds table element");
	return asObject(function);
}

static Object* getUninitializedElement()
{
	static Function* function = makeDummyFunction("uninitialized table element");
	return asObject(function);
}

static Uptr objectToBiasedTableElementValue(Object* object)
{
	return reinterpret_cast<Uptr>(object) - reinterpret_cast<Uptr>(getOutOfBoundsElement());
}

static Object* biasedTableElementValueToObject(Uptr biasedValue)
{
	return reinterpret_cast<Object*>(biasedValue + reinterpret_cast<Uptr>(getOutOfBoundsElement()));
}

static Table* createTableImpl(Compartment* compartment,
							  IR::TableType type,
							  std::string&& debugName,
							  ResourceQuotaRefParam resourceQuota)
{
	Table* table = new Table(compartment, type, std::move(debugName), resourceQuota);

	const U64 tableMaxElements = std::min(
		type.size.max, type.indexType == IR::IndexType::i32 ? maxTable32Elems : maxTable64Elems);
	const U64 tableMaxBytes = sizeof(Table::Element) * tableMaxElements;
	const U64 tableMaxPages
		= (tableMaxBytes + Platform::getBytesPerPage() - 1) >> Platform::getBytesPerPageLog2();

	table->elements
		= (Table::Element*)Platform::allocateVirtualPages(tableMaxPages + numGuardPages);
	table->numReservedBytes = tableMaxBytes;
	table->numReservedElements = tableMaxElements;
	if(!table->elements)
	{
		delete table;
		return nullptr;
	}

	// Add the table to the global array.
	{
		Platform::RWMutex::ExclusiveLock tablesLock(tablesMutex);
		tables.push_back(table);
	}
	return table;
}

static GrowResult growTableImpl(Table* table,
								Uptr numElementsToGrow,
								Uptr* outOldNumElements,
								bool initializeNewElements,
								Runtime::Object* initializeToElement = getUninitializedElement())
{
	Uptr oldNumElements;
	if(!numElementsToGrow) { oldNumElements = table->numElements.load(std::memory_order_acquire); }
	else
	{
		// Check the table element quota.
		if(table->resourceQuota && !table->resourceQuota->tableElems.allocate(numElementsToGrow))
		{ return GrowResult::outOfQuota; }

		Platform::RWMutex::ExclusiveLock resizingLock(table->resizingMutex);

		oldNumElements = table->numElements.load(std::memory_order_acquire);

		// If the growth would cause the table's size to exceed its maximum, return
		// GrowResult::outOfMaxSize.
		const U64 maxTableElems
			= table->indexType == IR::IndexType::i32 ? IR::maxTable32Elems : ::maxTable64Elems;
		if(numElementsToGrow > table->maxElements
		   || oldNumElements > table->maxElements - numElementsToGrow
		   || numElementsToGrow > maxTableElems
		   || oldNumElements > maxTableElems - numElementsToGrow)
		{
			if(table->resourceQuota) { table->resourceQuota->tableElems.free(numElementsToGrow); }
			return GrowResult::outOfMaxSize;
		}

		// Try to commit pages for the new elements, and return GrowResult::outOfMemory if the
		// commit fails.
		const Uptr newNumElements = oldNumElements + numElementsToGrow;
		const Uptr previousNumPlatformPages
			= getNumPlatformPages(oldNumElements * sizeof(Table::Element));
		const Uptr newNumPlatformPages
			= getNumPlatformPages(newNumElements * sizeof(Table::Element));
		if(newNumPlatformPages != previousNumPlatformPages
		   && !Platform::commitVirtualPages(
			   (U8*)table->elements + (previousNumPlatformPages << Platform::getBytesPerPageLog2()),
			   newNumPlatformPages - previousNumPlatformPages))
		{
			if(table->resourceQuota) { table->resourceQuota->tableElems.free(numElementsToGrow); }
			return GrowResult::outOfMemory;
		}
		Platform::registerVirtualAllocation((newNumPlatformPages - previousNumPlatformPages)
											<< Platform::getBytesPerPageLog2());

		if(initializeNewElements)
		{
			// Write the uninitialized sentinel value to the new elements.
			const Uptr biasedTableInitElement
				= objectToBiasedTableElementValue(initializeToElement);
			for(Uptr elementIndex = oldNumElements; elementIndex < newNumElements; ++elementIndex)
			{
				table->elements[elementIndex].biasedValue.store(biasedTableInitElement,
																std::memory_order_release);
			}
		}

		table->numElements.store(newNumElements, std::memory_order_release);
	}

	if(outOldNumElements) { *outOldNumElements = oldNumElements; }
	return GrowResult::success;
}

Table* Runtime::createTable(Compartment* compartment,
							IR::TableType type,
							Object* element,
							std::string&& debugName,
							ResourceQuotaRefParam resourceQuota)
{
	WAVM_ASSERT(type.size.min <= UINTPTR_MAX);
	Table* table = createTableImpl(compartment, type, std::move(debugName), resourceQuota);
	if(!table) { return nullptr; }

	// If element is null, use the uninitialized element sentinel instead.
	if(!element) { element = getUninitializedElement(); }
	else
	{
		WAVM_ERROR_UNLESS(isSubtype(asReferenceType(getExternType(element)), type.elementType));
	}

	// Grow the table to the type's minimum size.
	if(growTableImpl(table, Uptr(type.size.min), nullptr, true, element) != GrowResult::success)
	{
		delete table;
		return nullptr;
	}

	// Add the table to the compartment's tables IndexMap.
	{
		Platform::RWMutex::ExclusiveLock compartmentLock(compartment->mutex);

		table->id = compartment->tables.add(UINTPTR_MAX, table);
		if(table->id == UINTPTR_MAX)
		{
			delete table;
			return nullptr;
		}
		compartment->runtimeData->tables[table->id].base = table->elements;
		compartment->runtimeData->tables[table->id].endIndex = table->numReservedElements;
	}

	return table;
}

Table* Runtime::cloneTable(Table* table, Compartment* newCompartment)
{
	Platform::RWMutex::ExclusiveLock resizingLock(table->resizingMutex);

	// Create the new table.
	const IR::TableType tableType = getTableType(table);
	std::string debugName = table->debugName;
	Table* newTable
		= createTableImpl(newCompartment, tableType, std::move(debugName), table->resourceQuota);
	if(!newTable) { return nullptr; }

	// Grow the table to the same size as the original, without initializing the new elements since
	// they will be written immediately following this.
	if(growTableImpl(newTable, tableType.size.min, nullptr, false) != GrowResult::success)
	{
		delete newTable;
		return nullptr;
	}

	// Copy the original table's elements to the new table.
	for(Uptr elementIndex = 0; elementIndex < tableType.size.min; ++elementIndex)
	{
		newTable->elements[elementIndex].biasedValue.store(
			table->elements[elementIndex].biasedValue.load(std::memory_order_acquire),
			std::memory_order_release);
	}

	resizingLock.unlock();

	// Insert the table in the new compartment's tables array with the same index as it had in the
	// original compartment's tables IndexMap.
	{
		Platform::RWMutex::ExclusiveLock compartmentLock(newCompartment->mutex);

		newTable->id = table->id;
		newCompartment->tables.insertOrFail(newTable->id, newTable);
		newCompartment->runtimeData->tables[newTable->id].base = newTable->elements;
		newCompartment->runtimeData->tables[newTable->id].endIndex = newTable->numReservedElements;
	}

	return newTable;
}

Table::~Table()
{
	if(id != UINTPTR_MAX)
	{
		WAVM_ASSERT_RWMUTEX_IS_EXCLUSIVELY_LOCKED_BY_CURRENT_THREAD(compartment->mutex);

		WAVM_ASSERT(compartment->tables[id] == this);
		compartment->tables.removeOrFail(id);

		WAVM_ASSERT(compartment->runtimeData->tables[id].base == elements);
		compartment->runtimeData->tables[id].base = nullptr;
		compartment->runtimeData->tables[id].endIndex = 0;
	}

	// Remove the table from the global array.
	{
		Platform::RWMutex::ExclusiveLock tablesLock(tablesMutex);
		for(Uptr tableIndex = 0; tableIndex < tables.size(); ++tableIndex)
		{
			if(tables[tableIndex] == this)
			{
				tables.erase(tables.begin() + tableIndex);
				break;
			}
		}
	}

	// Free the virtual address space.
	const Uptr pageBytesLog2 = Platform::getBytesPerPageLog2();
	if(elements && numReservedBytes > 0)
	{
		Platform::freeVirtualPages((U8*)elements,
								   (numReservedBytes >> pageBytesLog2) + numGuardPages);

		Platform::deregisterVirtualAllocation(
			getNumPlatformPages(numElements * sizeof(Table::Element))
			<< Platform::getBytesPerPageLog2());
	}

	// Free the allocated quota.
	if(resourceQuota) { resourceQuota->tableElems.free(numElements); }
}

bool Runtime::isAddressOwnedByTable(U8* address, Table*& outTable, Uptr& outTableIndex)
{
	// Iterate over all tables and check if the address is within the reserved address space for
	// each.
	Platform::RWMutex::ShareableLock tablesLock(tablesMutex);
	for(auto table : tables)
	{
		U8* startAddress = (U8*)table->elements;
		U8* endAddress = ((U8*)table->elements) + table->numReservedBytes;
		if(address >= startAddress && address < endAddress)
		{
			outTable = table;
			outTableIndex = (address - startAddress) / sizeof(Table::Element);
			return true;
		}
	}
	return false;
}

static Object* setTableElementNonNull(Table* table, Uptr index, Object* object)
{
	WAVM_ASSERT(object);

	// Verify the index is within the table's bounds.
	if(index >= table->numReservedElements)
	{ throwException(ExceptionTypes::outOfBoundsTableAccess, {table, U64(index)}); }

	// Use a saturated index to access the table data to ensure that it's harmless for the CPU to
	// speculate past the above bounds check.
	const Uptr saturatedIndex = branchlessMin(index, U64(table->numReservedElements) - 1);

	// Compute the biased value to store in the table.
	const Uptr biasedValue = objectToBiasedTableElementValue(object);

	// Atomically replace the table element, throwing an out-of-bounds exception before the write if
	// the element being replaced is an out-of-bounds sentinel value.
	Uptr oldBiasedValue = table->elements[saturatedIndex].biasedValue;
	while(true)
	{
		if(biasedTableElementValueToObject(oldBiasedValue) == getOutOfBoundsElement())
		{ throwException(ExceptionTypes::outOfBoundsTableAccess, {table, U64(index)}); }
		if(table->elements[saturatedIndex].biasedValue.compare_exchange_weak(
			   oldBiasedValue, biasedValue, std::memory_order_acq_rel))
		{ break; }
	};

	return biasedTableElementValueToObject(oldBiasedValue);
}

static Object* getTableElementNonNull(const Table* table, Uptr index)
{
	// Verify the index is within the table's bounds.
	if(index >= table->numReservedElements)
	{
		throwException(ExceptionTypes::outOfBoundsTableAccess,
					   {const_cast<Table*>(table), U64(index)});
	}

	// Use a saturated index to access the table data to ensure that it's harmless for the CPU to
	// speculate past the above bounds check.
	const Uptr saturatedIndex = branchlessMin(index, U64(table->numReservedElements) - 1);

	// Read the table element.
	const Uptr biasedValue
		= table->elements[saturatedIndex].biasedValue.load(std::memory_order_acquire);
	Object* object = biasedTableElementValueToObject(biasedValue);

	// If the element was an out-of-bounds sentinel value, throw an out-of-bounds exception.
	if(object == getOutOfBoundsElement())
	{
		throwException(ExceptionTypes::outOfBoundsTableAccess,
					   {const_cast<Table*>(table), U64(index)});
	}

	WAVM_ASSERT(object);
	return object;
}

Object* Runtime::setTableElement(Table* table, Uptr index, Object* newValue)
{
	WAVM_ASSERT(!newValue || isInCompartment(newValue, table->compartment));

	// If the new value is null, write the uninitialized sentinel value instead.
	if(!newValue) { newValue = getUninitializedElement(); }

	// Write the table element.
	Object* oldObject = nullptr;
	Runtime::unwindSignalsAsExceptions([table, index, newValue, &oldObject] {
		oldObject = setTableElementNonNull(table, index, newValue);
	});

	// If the old table element was the uninitialized sentinel value, return null.
	return oldObject == getUninitializedElement() ? nullptr : oldObject;
}

Object* Runtime::getTableElement(const Table* table, Uptr index)
{
	Object* object = nullptr;
	Runtime::unwindSignalsAsExceptions(
		[table, index, &object] { object = getTableElementNonNull(table, index); });

	// If the old table element was the uninitialized sentinel value, return null.
	return object == getUninitializedElement() ? nullptr : object;
}

Uptr Runtime::getTableNumElements(const Table* table)
{
	return table->numElements.load(std::memory_order_acquire);
}

IR::TableType Runtime::getTableType(const Table* table)
{
	return IR::TableType(table->elementType,
						 table->isShared,
						 table->indexType,
						 IR::SizeConstraints{getTableNumElements(table), table->maxElements});
}

GrowResult Runtime::growTable(Table* table,
							  Uptr numElementsToGrow,
							  Uptr* outOldNumElements,
							  Object* initialElement)
{
	// If the initial value is null, write the uninitialized sentinel value instead.
	if(!initialElement) { initialElement = getUninitializedElement(); }

	return growTableImpl(table, numElementsToGrow, outOldNumElements, true, initialElement);
}

void Runtime::initElemSegment(Instance* instance,
							  Uptr elemSegmentIndex,
							  const IR::ElemSegment::Contents* contents,
							  Table* table,
							  Uptr destOffset,
							  Uptr sourceOffset,
							  Uptr numElems)
{
	Uptr numSourceElems;
	switch(contents->encoding)
	{
	case IR::ElemSegment::Encoding::expr: numSourceElems = contents->elemExprs.size(); break;
	case IR::ElemSegment::Encoding::index: numSourceElems = contents->elemIndices.size(); break;
	default: WAVM_UNREACHABLE();
	};

	if(sourceOffset + numElems > numSourceElems || sourceOffset + numElems < sourceOffset)
	{
		throwException(ExceptionTypes::outOfBoundsElemSegmentAccess,
					   {instance,
						U64(elemSegmentIndex),
						U64(sourceOffset > numSourceElems ? sourceOffset : numSourceElems)});
	}
	else
	{
		const Uptr numTableElems = getTableNumElements(table);
		if(destOffset + numElems > numTableElems || destOffset + numElems < destOffset)
		{
			throwException(ExceptionTypes::outOfBoundsTableAccess,
						   {table, U64(destOffset > numTableElems ? destOffset : numTableElems)});
		}
	}

	// Assert that the segment's elems are the right type for the table.
	switch(contents->encoding)
	{
	case IR::ElemSegment::Encoding::expr:
		WAVM_ASSERT(isSubtype(contents->elemType, table->elementType));
		break;
	case IR::ElemSegment::Encoding::index:
		WAVM_ASSERT(isSubtype(asReferenceType(contents->externKind), table->elementType));
		break;

	default: WAVM_UNREACHABLE();
	};

	for(Uptr index = 0; index < numElems; ++index)
	{
		Uptr sourceIndex = sourceOffset + index;
		const Uptr destIndex = destOffset + index;

		if(sourceIndex >= numSourceElems || sourceIndex < sourceOffset)
		{
			throwException(ExceptionTypes::outOfBoundsElemSegmentAccess,
						   {asObject(instance), U64(elemSegmentIndex), sourceIndex});
		}
		sourceIndex = branchlessMin(sourceIndex, numSourceElems);

		// Decode the element value.
		Object* elemObject = nullptr;
		switch(contents->encoding)
		{
		case IR::ElemSegment::Encoding::expr: {
			const IR::ElemExpr& elemExpr = contents->elemExprs[sourceIndex];
			switch(elemExpr.type)
			{
			case IR::ElemExpr::Type::ref_null: elemObject = nullptr; break;
			case IR::ElemExpr::Type::ref_func:
				elemObject = asObject(instance->functions[elemExpr.index]);
				break;

			case IR::ElemExpr::Type::invalid:
			default: WAVM_UNREACHABLE();
			}
			break;
		}
		case IR::ElemSegment::Encoding::index: {
			const Uptr externIndex = contents->elemIndices[sourceIndex];
			switch(contents->externKind)
			{
			case IR::ExternKind::function:
				elemObject = asObject(instance->functions[externIndex]);
				break;
			case IR::ExternKind::table: elemObject = asObject(instance->tables[externIndex]); break;
			case IR::ExternKind::memory:
				elemObject = asObject(instance->memories[externIndex]);
				break;
			case IR::ExternKind::global:
				elemObject = asObject(instance->globals[externIndex]);
				break;
			case IR::ExternKind::exceptionType:
				elemObject = asObject(instance->exceptionTypes[externIndex]);
				break;

			case IR::ExternKind::invalid:
			default: WAVM_UNREACHABLE();
			}
			break;
		}
		default: WAVM_UNREACHABLE();
		};

		setTableElement(table, destIndex, elemObject);
	}
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsicsTable,
							   "table.grow",
							   Iptr,
							   table_grow,
							   Object* initialValue,
							   Uptr deltaNumElements,
							   Uptr tableId)
{
	Table* table = getTableFromRuntimeData(contextRuntimeData, tableId);
	Uptr oldNumElements = 0;
	if(growTable(table,
				 deltaNumElements,
				 &oldNumElements,
				 initialValue ? initialValue : getUninitializedElement())
	   != GrowResult::success)
	{ return -1; }
	WAVM_ASSERT(oldNumElements <= INTPTR_MAX);
	return Iptr(oldNumElements);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsicsTable, "table.size", Uptr, table_size, Uptr tableId)
{
	Table* table = getTableFromRuntimeData(contextRuntimeData, tableId);
	return getTableNumElements(table);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsicsTable,
							   "table.get",
							   Object*,
							   table_get,
							   Uptr index,
							   Uptr tableId)
{
	Table* table = getTableFromRuntimeData(contextRuntimeData, tableId);
	return getTableElement(table, index);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsicsTable,
							   "table.set",
							   void,
							   table_set,
							   Uptr index,
							   Object* value,
							   Uptr tableId)
{
	Table* table = getTableFromRuntimeData(contextRuntimeData, tableId);
	setTableElement(table, index, value);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsicsTable,
							   "table.init",
							   void,
							   table_init,
							   Uptr destIndex,
							   Uptr sourceIndex,
							   Uptr numElems,
							   Uptr instanceId,
							   Uptr tableId,
							   Uptr elemSegmentIndex)
{
	Instance* instance = getInstanceFromRuntimeData(contextRuntimeData, instanceId);
	Table* table = getTableFromRuntimeData(contextRuntimeData, tableId);

	Platform::RWMutex::ShareableLock elemSegmentsLock(instance->elemSegmentsMutex);
	if(!instance->elemSegments[elemSegmentIndex])
	{
		if(sourceIndex != 0 || numElems != 0)
		{
			throwException(ExceptionTypes::outOfBoundsElemSegmentAccess,
						   {instance, elemSegmentIndex, sourceIndex});
		}
	}
	else
	{
		// Make a copy of the shared_ptr to the segment contents and unlock the elem segments mutex.
		std::shared_ptr<IR::ElemSegment::Contents> contents
			= instance->elemSegments[elemSegmentIndex];
		elemSegmentsLock.unlock();

		initElemSegment(
			instance, elemSegmentIndex, contents.get(), table, destIndex, sourceIndex, numElems);
	}
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsicsTable,
							   "elem.drop",
							   void,
							   elem_drop,
							   Uptr instanceId,
							   Uptr elemSegmentIndex)
{
	Instance* instance = getInstanceFromRuntimeData(contextRuntimeData, instanceId);
	Platform::RWMutex::ExclusiveLock elemSegmentsLock(instance->elemSegmentsMutex);

	if(instance->elemSegments[elemSegmentIndex])
	{ instance->elemSegments[elemSegmentIndex].reset(); }
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsicsTable,
							   "table.copy",
							   void,
							   table_copy,
							   Uptr destOffset,
							   Uptr sourceOffset,
							   Uptr numElements,
							   Uptr destTableId,
							   Uptr sourceTableId)
{
	Runtime::unwindSignalsAsExceptions([=] {
		Table* destTable = getTableFromRuntimeData(contextRuntimeData, destTableId);
		Table* sourceTable = getTableFromRuntimeData(contextRuntimeData, sourceTableId);

		const Uptr destOffsetUptr = Uptr(destOffset);
		const Uptr sourceOffsetUptr = Uptr(sourceOffset);
		const Uptr numElementsUptr = Uptr(numElements);

		const Uptr sourceTableNumElements = getTableNumElements(sourceTable);
		if(sourceOffsetUptr + numElementsUptr > sourceTableNumElements
		   || sourceOffsetUptr + numElementsUptr < sourceOffsetUptr)
		{
			const Uptr outOfBoundsSourceOffset = sourceOffsetUptr > sourceTableNumElements
													 ? sourceOffsetUptr
													 : sourceTableNumElements;
			throwException(ExceptionTypes::outOfBoundsTableAccess,
						   {sourceTable, outOfBoundsSourceOffset});
		}

		const Uptr destTableNumElements = getTableNumElements(destTable);
		if(destOffsetUptr + numElementsUptr > destTableNumElements
		   || destOffsetUptr + numElementsUptr < destOffsetUptr)
		{
			const Uptr outOfBoundsDestOffset
				= destOffsetUptr > destTableNumElements ? destOffsetUptr : destTableNumElements;
			throwException(ExceptionTypes::outOfBoundsTableAccess,
						   {destTable, outOfBoundsDestOffset});
		}

		if(sourceOffset < destOffset)
		{
			// When copying to higher indices, copy the elements in descending order to ensure that
			// source elements may only be overwritten after they have been copied.
			for(Uptr index = 0; index < numElements; ++index)
			{
				setTableElementNonNull(
					destTable,
					U64(destOffset) + U64(numElements - index - 1),
					getTableElementNonNull(sourceTable,
										   U64(sourceOffset) + U64(numElements - index - 1)));
			}
		}
		else
		{
			for(Uptr index = 0; index < numElements; ++index)
			{
				setTableElementNonNull(
					destTable,
					U64(destOffset) + U64(index),
					getTableElementNonNull(sourceTable, U64(sourceOffset) + U64(index)));
			}
		}
	});
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsicsTable,
							   "table.fill",
							   void,
							   table_fill,
							   Uptr destOffset,
							   Runtime::Object* value,
							   Uptr numElements,
							   Uptr destTableId)
{
	// If the value is null, write the uninitialized sentinel value instead.
	if(!value) { value = getUninitializedElement(); }

	Runtime::unwindSignalsAsExceptions([=] {
		Table* destTable = getTableFromRuntimeData(contextRuntimeData, destTableId);

		const Uptr destOffsetUptr = Uptr(destOffset);
		const Uptr numElementsUptr = Uptr(numElements);

		const Uptr destTableNumElements = getTableNumElements(destTable);
		if(destOffsetUptr + numElementsUptr > destTableNumElements
		   || destOffsetUptr + numElementsUptr < destOffsetUptr)
		{
			const Uptr outOfBoundsDestOffset
				= destOffsetUptr > destTableNumElements ? destOffsetUptr : destTableNumElements;
			throwException(ExceptionTypes::outOfBoundsTableAccess,
						   {destTable, outOfBoundsDestOffset});
		}

		for(Uptr index = 0; index < numElements; ++index)
		{ setTableElementNonNull(destTable, U64(destOffset) + U64(index), value); }
	});
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsicsTable,
							   "callIndirectFail",
							   void,
							   callIndirectFail,
							   Uptr index,
							   Uptr tableId,
							   Function* function,
							   Uptr expectedTypeEncoding)
{
	Table* table = getTableFromRuntimeData(contextRuntimeData, tableId);
	if(asObject(function) == getOutOfBoundsElement())
	{ throwException(ExceptionTypes::outOfBoundsTableAccess, {table, U64(index)}); }
	else if(asObject(function) == getUninitializedElement())
	{
		throwException(ExceptionTypes::uninitializedTableElement, {table, U64(index)});
	}
	else
	{
		throwException(ExceptionTypes::indirectCallSignatureMismatch,
					   {function, U64(expectedTypeEncoding)});
	}
}
