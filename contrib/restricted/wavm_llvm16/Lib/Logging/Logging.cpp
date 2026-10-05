#include "WAVM/Logging/Logging.h"
#include <atomic>
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Config.h"
#include "WAVM/Platform/Defines.h"
#include "WAVM/Platform/File.h"
#include "WAVM/VFS/VFS.h"

using namespace WAVM;
using namespace WAVM::Log;

static std::atomic<bool> categoryEnabled[(Uptr)Category::num] = {
	{true},         // error
	{!!WAVM_DEBUG}, // debug
	{false},        // metrics
	{true},         // output
	{false},        // trace validation
	{false},        // trace compilation
};
static std::atomic<OutputFunction*> atomicOutputFunction{nullptr};

static VFS::VFD* getFileForCategory(Log::Category category)
{
	return category == Log::error ? Platform::getStdFD(Platform::StdDevice::err)
								  : Platform::getStdFD(Platform::StdDevice::out);
}

void Log::setCategoryEnabled(Category category, bool enable)
{
	WAVM_ASSERT(category < Category::num);
	categoryEnabled[(Uptr)category].store(enable);
}

bool Log::isCategoryEnabled(Category category)
{
	WAVM_ASSERT(category < Category::num);
	return categoryEnabled[(Uptr)category].load();
}

void Log::printf(Category category, const char* format, ...)
{
	if(categoryEnabled[(Uptr)category].load())
	{
		va_list argList;
		va_start(argList, format);
		vprintf(category, format, argList);
		va_end(argList);
	}
}

void Log::vprintf(Category category, const char* format, va_list argList)
{
	if(categoryEnabled[(Uptr)category].load())
	{
		va_list argListCopy;
		va_copy(argListCopy, argList);
		const I32 numChars = vsnprintf(nullptr, 0, format, argListCopy);
		WAVM_ASSERT(numChars >= 0);

		const Uptr numBufferBytes = numChars + 1;
		static constexpr Uptr maxAllocaBytes = 4096;
		char* buffer = (char*)(numBufferBytes > maxAllocaBytes ? malloc(numBufferBytes)
															   : alloca(numBufferBytes));
		vsnprintf(buffer, numBufferBytes, format, argList);

		// If an output function is set, call it with the message.
		OutputFunction* outputFunction = atomicOutputFunction.load(std::memory_order_acquire);
		if(outputFunction) { (*outputFunction)(category, buffer, Uptr(numChars)); }
		else
		{
			// Otherwise, write the message to the appropriate stdio device.
			VFS::VFD* fd = getFileForCategory(category);
			Uptr numBytesWritten = 0;
			WAVM_ERROR_UNLESS(fd->write(buffer, numChars, &numBytesWritten)
							  == VFS::Result::success);
			WAVM_ERROR_UNLESS(numBytesWritten == U32(numChars));
		}

		if(numBufferBytes > maxAllocaBytes) { free(buffer); }
	}
}

void Log::setOutputFunction(OutputFunction* newOutputFunction)
{
	atomicOutputFunction.store(newOutputFunction, std::memory_order_release);
}
