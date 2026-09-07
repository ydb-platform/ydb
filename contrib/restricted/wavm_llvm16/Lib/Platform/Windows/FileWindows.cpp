#include <algorithm>
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/I128.h"
#include "WAVM/Inline/Time.h"
#include "WAVM/Inline/Unicode.h"
#include "WAVM/Platform/Clock.h"
#include "WAVM/Platform/Event.h"
#include "WAVM/Platform/File.h"
#include "WAVM/Platform/RWMutex.h"
#include "WAVM/VFS/VFS.h"
#include "WindowsPrivate.h"

#define NOMINMAX
#include <Windows.h>

using namespace WAVM;
using namespace WAVM::Platform;
using namespace WAVM::VFS;

static Result asVFSResult(DWORD windowsError)
{
	switch(windowsError)
	{
	case ERROR_SUCCESS: return Result::success;

	case ERROR_READ_FAULT:
	case ERROR_WRITE_FAULT:
	case ERROR_NET_WRITE_FAULT:
	case ERROR_IO_DEVICE: return Result::ioDeviceError;

	case ERROR_PATH_NOT_FOUND:
	case ERROR_FILE_NOT_FOUND: return Result::doesNotExist;

	case ERROR_NEGATIVE_SEEK: return Result::invalidOffset;
	case ERROR_INVALID_FUNCTION: return Result::notPermitted;
	case ERROR_ACCESS_DENIED: return Result::notAccessible;
	case ERROR_IO_PENDING: return Result::ioPending;
	case ERROR_OPERATION_ABORTED: return Result::interruptedByCancellation;
	case ERROR_NOT_ENOUGH_MEMORY: return Result::outOfMemory;
	case ERROR_NOT_ENOUGH_QUOTA: return Result::outOfQuota;
	case ERROR_HANDLE_DISK_FULL: return Result::outOfFreeSpace;
	case ERROR_INVALID_USER_BUFFER: return Result::inaccessibleBuffer;
	case ERROR_INSUFFICIENT_BUFFER: return Result::notEnoughBufferBytes;
	case ERROR_BROKEN_PIPE: return Result::brokenPipe;
	case ERROR_ALREADY_EXISTS: return Result::alreadyExists;
	case ERROR_DIR_NOT_EMPTY: return Result::isNotEmpty;
	case ERROR_INVALID_ADDRESS: return Result::inaccessibleBuffer;
	case ERROR_DIRECTORY: return Result::isNotDirectory;

	case ERROR_INVALID_PARAMETER:
		// This probably needs to be handled differently for each API entry point.
		Errors::fatalfWithCallStack("ERROR_INVALID_PARAMETER");

	case ERROR_INVALID_HANDLE:
		// This must be a bug, so make it a fatal error.
		Errors::fatalfWithCallStack("ERROR_INVALID_HANDLE");

	default: Errors::fatalfWithCallStack("Unexpected windows error code: %u", GetLastError());
	};
}

static LARGE_INTEGER makeLargeInt(U64 u64)
{
	LARGE_INTEGER result;
	result.QuadPart = u64;
	return result;
}

static Result getFileType(HANDLE handle, FileType& outType)
{
	const DWORD windowsFileType = GetFileType(handle);
	if(windowsFileType == FILE_TYPE_UNKNOWN && GetLastError() != ERROR_SUCCESS)
	{ return asVFSResult(GetLastError()); }

	switch(windowsFileType)
	{
	case FILE_TYPE_CHAR: outType = FileType::characterDevice; break;
	case FILE_TYPE_PIPE: outType = FileType::pipe; break;
	case FILE_TYPE_DISK: {
		FILE_BASIC_INFO fileBasicInfo;
		if(!GetFileInformationByHandleEx(
			   handle, FileBasicInfo, &fileBasicInfo, sizeof(fileBasicInfo)))
		{ return asVFSResult(GetLastError()); }

		outType = fileBasicInfo.FileAttributes & FILE_ATTRIBUTE_DIRECTORY ? FileType::directory
																		  : FileType::file;
		break;
	}

	default: outType = FileType::unknown;
	};

	return Result::success;
}

static Result getFileInfoByHandle(HANDLE handle, FileInfo& outInfo)
{
	BY_HANDLE_FILE_INFORMATION windowsFileInfo;
	if(!GetFileInformationByHandle(handle, &windowsFileInfo))
	{ return asVFSResult(GetLastError()); }

	outInfo.deviceNumber = windowsFileInfo.dwVolumeSerialNumber;
	outInfo.fileNumber
		= windowsFileInfo.nFileIndexLow | (U64(windowsFileInfo.nFileIndexHigh) << 32);

	const Result getTypeResult = getFileType(handle, outInfo.type);
	if(getTypeResult != Result::success) { return getTypeResult; }

	outInfo.numLinks = windowsFileInfo.nNumberOfLinks;
	outInfo.numBytes = windowsFileInfo.nFileSizeLow | (U64(windowsFileInfo.nFileSizeHigh) << 32);

	outInfo.lastAccessTime = fileTimeToWAVMRealTime(windowsFileInfo.ftLastAccessTime);
	outInfo.lastWriteTime = fileTimeToWAVMRealTime(windowsFileInfo.ftLastWriteTime);
	outInfo.creationTime = fileTimeToWAVMRealTime(windowsFileInfo.ftCreationTime);

	return Result::success;
}

static bool getWindowsPath(const std::string& inString, std::wstring& outString)
{
	outString.clear();

	U32 codePoint;
	const U8* nextChar = (const U8*)inString.c_str();
	const U8* endChar = nextChar + inString.size();
	while(nextChar != endChar)
	{
		if(!Unicode::decodeUTF8CodePoint(nextChar, endChar, codePoint)) { return false; }
		if(codePoint == U32('/')) { codePoint = U32('\\'); }
		Unicode::encodeUTF16CodePoint(codePoint, outString);
	};
	return true;
}

static void getVFSPath(const wchar_t* inChars,
					   size_t numChars,
					   std::string& outString,
					   const char* context)
{
	outString.clear();

	U32 codePoint;
	const U16* nextChar = (const U16*)inChars;
	const U16* endChar = nextChar + numChars;
	while(nextChar != endChar)
	{
		if(!Unicode::decodeUTF16CodePoint(nextChar, endChar, codePoint))
		{ Errors::fatalf("Found an invalid UTF-16 code point (%u) in %s", *nextChar, context); }
		if(codePoint == U32('\\')) { codePoint = U32('/'); }
		Unicode::encodeUTF8CodePoint(codePoint, outString);
	};
}

static bool readDirEnts(HANDLE handle, bool startFromBeginning, std::vector<DirEnt>& outDirEnts)
{
	U8 buffer[2048];
	if(!GetFileInformationByHandleEx(
		   handle,
		   startFromBeginning ? FileIdBothDirectoryRestartInfo : FileIdBothDirectoryInfo,
		   buffer,
		   sizeof(buffer)))
	{ return false; }

	auto fileInfo = (FILE_ID_BOTH_DIR_INFO*)buffer;
	while(true)
	{
		DirEnt dirEnt;
		dirEnt.fileNumber = fileInfo->FileId.QuadPart;

		// Convert the Windows path (UTF-16 + \) to a VFS path (UTF-8 + /).
		getVFSPath(fileInfo->FileName,
				   fileInfo->FileNameLength / sizeof(wchar_t),
				   dirEnt.name,
				   "a filename returned by GetFileInformationByHandleEx");

		// Assume this is a FILE_TYPE_DISK.
		dirEnt.type = fileInfo->FileAttributes & FILE_ATTRIBUTE_DIRECTORY ? FileType::directory
																		  : FileType::file;

		outDirEnts.push_back(dirEnt);

		if(fileInfo->NextEntryOffset == 0) { break; }
		else
		{
			fileInfo = (FILE_ID_BOTH_DIR_INFO*)(((U8*)fileInfo) + fileInfo->NextEntryOffset);
		}
	};

	return true;
}

struct WindowsDirEntStream : DirEntStream
{
	WindowsDirEntStream(HANDLE inHandle, std::vector<DirEnt>&& inDirEnts)
	: handle(inHandle), dirEnts(std::move(inDirEnts))
	{
	}

	virtual void close() override
	{
		if(!CloseHandle(handle))
		{ Errors::fatalf("CloseHandle failed: GetLastError()=%u", GetLastError()); }
		delete this;
	}

	virtual bool getNext(DirEnt& outEntry) override
	{
		RWMutex::ExclusiveLock lock(mutex);
		while(nextReadIndex >= dirEnts.size())
		{
			if(!readDirEnts(handle, nextReadIndex == 0, dirEnts))
			{
				if(GetLastError() == ERROR_NO_MORE_FILES) { return false; }
				else
				{
					Errors::fatalfWithCallStack("Unexpected windows error code: %u",
												GetLastError());
				}
			}
		};

		outEntry = dirEnts[nextReadIndex++];

		return true;
	}

	virtual void restart() override
	{
		RWMutex::ExclusiveLock lock(mutex);
		dirEnts.clear();
		nextReadIndex = 0;
	}

	virtual U64 tell() override
	{
		WAVM_ERROR_UNLESS(nextReadIndex < UINT64_MAX);
		return U64(nextReadIndex);
	}

	virtual bool seek(U64 offset) override
	{
		RWMutex::ShareableLock lock(mutex);

		// Don't allow seeking forward past the last buffered dirent.
		if(offset > UINTPTR_MAX || offset > dirEnts.size()) { return false; }

		nextReadIndex = Uptr(offset);
		return true;
	}

private:
	HANDLE handle;

	Platform::RWMutex mutex;
	std::vector<DirEnt> dirEnts;
	std::atomic<Uptr> nextReadIndex{0};
};

struct WindowsFD : VFD
{
	WindowsFD(HANDLE inHandle,
			  DWORD inDesiredAccess,
			  DWORD inShareMode,
			  DWORD inFlagsAndAttributes,
			  bool inNonBlocking,
			  VFDSync inImplicitSync)
	: handle(inHandle)
	, desiredAccess(inDesiredAccess)
	, shareMode(inShareMode)
	, flagsAndAttributes(inFlagsAndAttributes)
	, nonBlocking(inNonBlocking)
	, syncLevel(inImplicitSync)
	{
		WAVM_ASSERT(handle != INVALID_HANDLE_VALUE);
	}

	virtual Result close() override
	{
		Result result = Result::success;
		if(!CloseHandle(handle)) { result = asVFSResult(GetLastError()); }
		delete this;
		return result;
	}

	virtual Result seek(I64 offset, SeekOrigin origin, U64* outAbsoluteOffset = nullptr) override
	{
		RWMutex::ShareableLock lock(mutex);

		DWORD windowsOrigin;
		switch(origin)
		{
		case SeekOrigin::begin: windowsOrigin = FILE_BEGIN; break;
		case SeekOrigin::cur: windowsOrigin = FILE_CURRENT; break;
		case SeekOrigin::end: windowsOrigin = FILE_END; break;
		default: WAVM_UNREACHABLE();
		}

		LONG offsetHigh = LONG((offset >> 32) & 0xffffffff);
		LONG result = SetFilePointer(handle, LONG(offset & 0xffffffff), &offsetHigh, windowsOrigin);
		if(result == LONG(INVALID_SET_FILE_POINTER))
		{
			// "If an application calls SetFilePointer with distance to move values that result
			// in a position not sector-aligned and a handle that is opened with
			// FILE_FLAG_NO_BUFFERING, the function fails, and GetLastError returns
			// ERROR_INVALID_PARAMETER."
			return GetLastError() == ERROR_INVALID_PARAMETER ? Result::invalidOffset
															 : asVFSResult(GetLastError());
		}

		if(outAbsoluteOffset) { *outAbsoluteOffset = (U64(offsetHigh) << 32) | result; }
		return Result::success;
	}
	virtual Result readv(const IOReadBuffer* buffers,
						 Uptr numBuffers,
						 Uptr* outNumBytesRead,
						 const U64* offset) override
	{
		RWMutex::ShareableLock lock(mutex);

		if(outNumBytesRead) { *outNumBytesRead = 0; }
		if(numBuffers == 0) { return Result::success; }

		// If there's an offset specified, translate it to an OVERLAPPED struct.
		OVERLAPPED* overlapped = nullptr;
		if(offset)
		{
			overlapped = (OVERLAPPED*)alloca(sizeof(OVERLAPPED));
			overlapped->Offset = DWORD(*offset);
			overlapped->OffsetHigh = DWORD(*offset >> 32);
			overlapped->hEvent = nullptr;
		}

		// Count the number of bytes in all the buffers.
		Uptr numBufferBytes = 0;
		for(Uptr bufferIndex = 0; bufferIndex < numBuffers; ++bufferIndex)
		{
			const IOReadBuffer& buffer = buffers[bufferIndex];
			if(numBufferBytes + buffer.numBytes < numBufferBytes)
			{ return Result::tooManyBufferBytes; }
			numBufferBytes += buffer.numBytes;
		}
		if(numBufferBytes > UINT32_MAX) { return Result::tooManyBufferBytes; }
		const U32 numBufferBytesU32 = U32(numBufferBytes);

		// If there's a single buffer, just use it directly. Otherwise, allocate a combined buffer.
		if(numBuffers == 1)
		{ return readImpl(buffers[0].data, numBufferBytesU32, overlapped, outNumBytesRead); }
		else
		{
			U8* combinedBuffer = (U8*)malloc(numBufferBytes);
			if(!combinedBuffer) { return Result::outOfMemory; }

			Uptr numBytesRead = 0;
			const Result result
				= readImpl(combinedBuffer, numBufferBytesU32, overlapped, &numBytesRead);

			if(result == Result::success)
			{
				// If there was a combined buffer, copy the contents of it to the individual
				// buffers.
				Uptr numBytesCopied = 0;
				for(Uptr bufferIndex = 0; bufferIndex < numBuffers && numBytesCopied < numBytesRead;
					++bufferIndex)
				{
					const IOReadBuffer& buffer = buffers[bufferIndex];
					const Uptr numBytesToCopy
						= std::min(buffer.numBytes, numBytesRead - numBytesCopied);
					if(numBytesToCopy)
					{ memcpy(buffer.data, combinedBuffer + numBytesCopied, numBytesToCopy); }
					numBytesCopied += numBytesToCopy;
				}

				// Write the total number of bytes read.
				if(outNumBytesRead) { *outNumBytesRead = numBytesRead; }
			}

			// Free the combined buffer.
			free(combinedBuffer);

			return result;
		}
	}
	virtual Result writev(const IOWriteBuffer* buffers,
						  Uptr numBuffers,
						  Uptr* outNumBytesWritten,
						  const U64* offset) override
	{
		RWMutex::ShareableLock lock(mutex);

		if(outNumBytesWritten) { *outNumBytesWritten = 0; }
		if(numBuffers == 0) { return Result::success; }

		// If there's an offset specified, translate it to an OVERLAPPED struct.
		OVERLAPPED* overlapped = nullptr;
		if(offset)
		{
			overlapped = (OVERLAPPED*)alloca(sizeof(OVERLAPPED));
			overlapped->Offset = DWORD(*offset);
			overlapped->OffsetHigh = DWORD(*offset >> 32);
			overlapped->hEvent = nullptr;
		}

		// Count the number of bytes in all the buffers.
		Uptr numBufferBytes = 0;
		for(Uptr bufferIndex = 0; bufferIndex < numBuffers; ++bufferIndex)
		{
			const IOWriteBuffer& buffer = buffers[bufferIndex];
			if(numBufferBytes + buffer.numBytes < numBufferBytes)
			{ return Result::tooManyBufferBytes; }
			numBufferBytes += buffer.numBytes;
		}
		if(numBufferBytes > Uptr(UINT32_MAX)) { return Result::tooManyBufferBytes; }
		const U32 numBufferBytesU32 = U32(numBufferBytes);

		// If there's a single buffer, just use it directly. Otherwise, allocate a combined buffer.
		if(numBuffers == 1)
		{ return writeImpl(buffers[0].data, numBufferBytesU32, overlapped, outNumBytesWritten); }
		else
		{
			U8* combinedBuffer = (U8*)malloc(numBufferBytes);
			if(!combinedBuffer) { return Result::outOfMemory; }

			// Copy all the input buffers into the combined buffer.
			Uptr numBytesCopied = 0;
			for(Uptr bufferIndex = 0; bufferIndex < numBuffers; ++bufferIndex)
			{
				const IOWriteBuffer& buffer = buffers[bufferIndex];
				if(buffer.numBytes)
				{ memcpy(combinedBuffer + numBytesCopied, buffer.data, buffer.numBytes); }
				numBytesCopied += buffer.numBytes;
			}

			// Do the write.
			const Result result
				= writeImpl(combinedBuffer, numBufferBytesU32, overlapped, outNumBytesWritten);

			// Free the combined buffer.
			free(combinedBuffer);

			return result;
		}
	}

	virtual Result sync(SyncType syncType) override
	{
		RWMutex::ShareableLock lock(mutex);

		return FlushFileBuffers(handle) ? Result::success : asVFSResult(GetLastError());
	}

	virtual Result getVFDInfo(VFDInfo& outInfo) override
	{
		RWMutex::ShareableLock lock(mutex);

		const Result getTypeResult = getFileType(handle, outInfo.type);
		if(getTypeResult != Result::success) { return getTypeResult; }

		outInfo.flags.append = desiredAccess & FILE_APPEND_DATA;
		outInfo.flags.nonBlocking = nonBlocking;
		outInfo.flags.syncLevel = syncLevel;
		return Result::success;
	}
	virtual Result getFileInfo(FileInfo& outInfo) override
	{
		RWMutex::ShareableLock lock(mutex);

		return getFileInfoByHandle(handle, outInfo);
	}

	virtual Result setVFDFlags(const VFDFlags& flags) override
	{
		RWMutex::ExclusiveLock lock(mutex);

		const DWORD originalDesiredAccess = desiredAccess;
		if(originalDesiredAccess & (GENERIC_WRITE | FILE_APPEND_DATA))
		{
			desiredAccess &= ~(GENERIC_WRITE | FILE_APPEND_DATA);
			desiredAccess |= flags.append ? FILE_APPEND_DATA : GENERIC_WRITE;
		}

		if(desiredAccess != originalDesiredAccess)
		{
			HANDLE reopenedHandle
				= ReOpenFile(handle, desiredAccess, shareMode, flagsAndAttributes);
			if(reopenedHandle != INVALID_HANDLE_VALUE)
			{
				if(!CloseHandle(handle))
				{ Errors::fatalf("CloseHandle failed: GetLastError()=%u", GetLastError()); }
				handle = reopenedHandle;
			}
			else
			{
				desiredAccess = originalDesiredAccess;
				return asVFSResult(GetLastError());
			}
		}

		nonBlocking = flags.nonBlocking;
		syncLevel = flags.syncLevel;

		return Result::success;
	}
	virtual Result setFileSize(U64 numBytes) override
	{
		RWMutex::ShareableLock lock(mutex);

		FILE_END_OF_FILE_INFO endOfFileInfo;
		endOfFileInfo.EndOfFile = makeLargeInt(numBytes);
		return SetFileInformationByHandle(
				   handle, FileEndOfFileInfo, &endOfFileInfo, sizeof(endOfFileInfo))
				   ? Result::success
				   : asVFSResult(GetLastError());
	}
	virtual Result setFileTimes(bool setLastAccessTime,
								Time lastAccessTime,
								bool setLastWriteTime,
								Time lastWriteTime) override
	{
		RWMutex::ShareableLock lock(mutex);

		// Translate the times to Windows file times.
		FILETIME lastAccessFileTime;
		if(setLastAccessTime) { lastAccessFileTime = wavmRealTimeToFileTime(lastAccessTime); }
		FILETIME lastWriteFileTime;
		if(setLastWriteTime) { lastWriteFileTime = wavmRealTimeToFileTime(lastWriteTime); }

		return SetFileTime(handle,
						   nullptr,
						   setLastAccessTime ? &lastAccessFileTime : nullptr,
						   setLastWriteTime ? &lastWriteFileTime : nullptr)
				   ? Result::success
				   : asVFSResult(GetLastError());
	}

	virtual Result openDir(DirEntStream*& outStream) override
	{
		RWMutex::ShareableLock lock(mutex);

		// Make a copy of the handle, so the FD and the DirEntStream can be closed independently.
		HANDLE duplicatedHandle = INVALID_HANDLE_VALUE;
		WAVM_ERROR_UNLESS(DuplicateHandle(GetCurrentProcess(),
										  handle,
										  GetCurrentProcess(),
										  &duplicatedHandle,
										  0,
										  TRUE,
										  DUPLICATE_SAME_ACCESS));

		// Try to read the initial buffer-full of dirents to determine whether this is a directory.
		std::vector<DirEnt> initialDirEnts;
		if(readDirEnts(duplicatedHandle, true, initialDirEnts)
		   || GetLastError() == ERROR_NO_MORE_FILES)
		{
			outStream = new WindowsDirEntStream(duplicatedHandle, std::move(initialDirEnts));
			return Result::success;
		}
		else
		{
			const Result result = GetLastError() == ERROR_INVALID_PARAMETER
									  ? Result::isNotDirectory
									  : asVFSResult(GetLastError());

			// Close the duplicated handle.
			if(!CloseHandle(duplicatedHandle))
			{ Errors::fatalf("CloseHandle failed: GetLastError()=%u", GetLastError()); }

			return result;
		}
	}

private:
	Platform::RWMutex mutex;
	HANDLE handle;
	DWORD desiredAccess;
	const DWORD shareMode;
	const DWORD flagsAndAttributes;
	bool nonBlocking;
	VFDSync syncLevel;

	Result readImpl(void* buffer,
					U32 numBufferBytesU32,
					OVERLAPPED* overlapped,
					Uptr* outNumBytesRead)
	{
		// If the FD has implicit syncing before reads, do it.
		if(syncLevel == VFDSync::contentsAfterWriteAndBeforeRead
		   || syncLevel == VFDSync::contentsAndMetadataAfterWriteAndBeforeRead)
		{
			if(!FlushFileBuffers(handle)) { return asVFSResult(GetLastError()); }
		}

		// Do the read.
		DWORD numBytesRead = 0;
		if(!ReadFile(handle, buffer, numBufferBytesU32, &numBytesRead, overlapped))
		{
			// "The ReadFile function may fail with ERROR_NOT_ENOUGH_QUOTA, which means the calling
			// process's buffer could not be page-locked."
			return GetLastError() == ERROR_NOT_ENOUGH_QUOTA ? Result::outOfMemory
															: asVFSResult(GetLastError());
		}

		// Write the total number of bytes read.
		if(outNumBytesRead) { *outNumBytesRead = Uptr(numBytesRead); }

		return Result::success;
	}

	Result writeImpl(const void* buffer,
					 U32 numBufferBytesU32,
					 OVERLAPPED* overlapped,
					 Uptr* outNumBytesWritten)
	{
		// Do the write.
		DWORD numBytesWritten = 0;
		if(!WriteFile(handle, buffer, numBufferBytesU32, &numBytesWritten, overlapped))
		{
			// "The WriteFile function may fail with ERROR_NOT_ENOUGH_QUOTA, which means the calling
			// process's buffer could not be page-locked."
			return GetLastError() == ERROR_NOT_ENOUGH_QUOTA ? Result::outOfMemory
															: asVFSResult(GetLastError());
		}

		// If the FD has implicit syncing after writes, do it.
		if(syncLevel != VFDSync::none)
		{
			if(!FlushFileBuffers(handle)) { return asVFSResult(GetLastError()); }
		}

		// Write the total number of bytes written.
		if(outNumBytesWritten) { *outNumBytesWritten = Uptr(numBytesWritten); }

		return Result::success;
	}
};

struct WindowsStdFD : WindowsFD
{
	WindowsStdFD(HANDLE inHandle, DWORD inDesiredAccess, VFDSync inImplicitSync)
	: WindowsFD(inHandle,
				inDesiredAccess,
				FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
				0,
				false,
				inImplicitSync)
	{
	}

	virtual Result close()
	{
		// The stdio FDs are shared, so don't close them.
		return Result::success;
	}
};

VFD* Platform::getStdFD(StdDevice device)
{
	static WindowsStdFD* stdinVFD
		= new WindowsStdFD(GetStdHandle(STD_INPUT_HANDLE), GENERIC_READ, VFDSync::none);
	static WindowsStdFD* stdoutVFD
		= new WindowsStdFD(GetStdHandle(STD_OUTPUT_HANDLE), FILE_APPEND_DATA, VFDSync::none);
	static WindowsStdFD* stderrVFD
		= new WindowsStdFD(GetStdHandle(STD_ERROR_HANDLE), FILE_APPEND_DATA, VFDSync::none);

	switch(device)
	{
	case StdDevice::in: return stdinVFD;
	case StdDevice::out: return stdoutVFD;
	case StdDevice::err: return stderrVFD;
	default: WAVM_UNREACHABLE();
	};
}

struct WindowsFS : HostFS
{
	virtual Result open(const std::string& path,
						FileAccessMode accessMode,
						FileCreateMode createMode,
						VFD*& outFD,
						const VFDFlags& flags = VFDFlags{}) override;

	virtual Result getFileInfo(const std::string& path, FileInfo& outInfo) override;
	virtual Result setFileTimes(const std::string& path,
								bool setLastAccessTime,
								Time lastAccessTime,
								bool setLastWriteTime,
								Time lastWriteTime) override;

	virtual Result openDir(const std::string& path, DirEntStream*& outStream) override;

	virtual Result renameFile(const std::string& oldPath, const std::string& newPath) override;
	virtual Result unlinkFile(const std::string& path) override;
	virtual Result removeDir(const std::string& path) override;
	virtual Result createDir(const std::string& path) override;

	static WindowsFS& get()
	{
		static WindowsFS windowsFS;
		return windowsFS;
	}

protected:
	WindowsFS() {}
};

HostFS& Platform::getHostFS() { return WindowsFS::get(); }

Result WindowsFS::open(const std::string& path,
					   FileAccessMode accessMode,
					   FileCreateMode createMode,
					   VFD*& outFD,
					   const VFDFlags& flags)
{
	// Convert the path from a UTF-8 VFS path (with /) to a UTF-16 Windows path (with \).
	std::wstring windowsPath;
	if(!getWindowsPath(path, windowsPath)) { return Result::invalidNameCharacter; }

	// Map the VFS accessMode/createMode to the appropriate Windows CreateFile arguments.
	DWORD desiredAccess = 0;
	DWORD shareMode = FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE;
	DWORD creationDisposition = 0;
	DWORD flagsAndAttributes = 0;

	// From https://docs.microsoft.com/en-us/windows/desktop/api/fileapi/nf-fileapi-createfilew:
	// The file is being opened or created for a backup or restore operation. The system
	// ensures that the calling process overrides file security checks when the process has
	// SE_BACKUP_NAME and SE_RESTORE_NAME privileges. For more information, see Changing
	// Privileges in a Token.
	// You must set this flag to obtain a handle to a directory. A directory handle can be
	// passed to some functions instead of a file handle.For more information, see the Remarks
	// section.
	flagsAndAttributes |= FILE_FLAG_BACKUP_SEMANTICS;

	const DWORD writeOrAppend = flags.append ? FILE_APPEND_DATA : GENERIC_WRITE;
	switch(accessMode)
	{
	case FileAccessMode::none: desiredAccess = 0; break;
	case FileAccessMode::readOnly: desiredAccess = GENERIC_READ; break;
	case FileAccessMode::writeOnly: desiredAccess = writeOrAppend; break;
	case FileAccessMode::readWrite: desiredAccess = GENERIC_READ | writeOrAppend; break;
	default: WAVM_UNREACHABLE();
	};

	switch(createMode)
	{
	case FileCreateMode::createAlways: creationDisposition = CREATE_ALWAYS; break;
	case FileCreateMode::createNew: creationDisposition = CREATE_NEW; break;
	case FileCreateMode::openAlways: creationDisposition = OPEN_ALWAYS; break;
	case FileCreateMode::openExisting: creationDisposition = OPEN_EXISTING; break;
	case FileCreateMode::truncateExisting: creationDisposition = TRUNCATE_EXISTING; break;
	default: WAVM_UNREACHABLE();
	};

	// Try to open the file.
	HANDLE handle = CreateFileW(windowsPath.c_str(),
								desiredAccess,
								shareMode,
								nullptr,
								creationDisposition,
								flagsAndAttributes,
								nullptr);
	if(handle == INVALID_HANDLE_VALUE) { return asVFSResult(GetLastError()); }

	outFD = new WindowsFD(
		handle, desiredAccess, shareMode, flagsAndAttributes, flags.nonBlocking, flags.syncLevel);
	return Result::success;
}

Result WindowsFS::getFileInfo(const std::string& path, FileInfo& outInfo)
{
	// Convert the path from a UTF-8 VFS path (with /) to a UTF-16 Windows path (with \).
	std::wstring windowsPath;
	if(!getWindowsPath(path, windowsPath)) { return Result::invalidNameCharacter; }

	// Try to open the file with no access requested.
	HANDLE handle = CreateFileW(windowsPath.c_str(),
								0,
								FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
								nullptr,
								OPEN_EXISTING,
								FILE_FLAG_BACKUP_SEMANTICS,
								nullptr);
	if(handle == INVALID_HANDLE_VALUE) { return asVFSResult(GetLastError()); }

	const Result result = getFileInfoByHandle(handle, outInfo);

	if(!CloseHandle(handle))
	{ Errors::fatalf("CloseHandle failed: GetLastError()=%u", GetLastError()); }

	return result;
}

Result WindowsFS::setFileTimes(const std::string& path,
							   bool setLastAccessTime,
							   Time lastAccessTime,
							   bool setLastWriteTime,
							   Time lastWriteTime)
{
	// Convert the path from a UTF-8 VFS path (with /) to a UTF-16 Windows path (with \).
	std::wstring windowsPath;
	if(!getWindowsPath(path, windowsPath)) { return Result::invalidNameCharacter; }

	// Try to open the file with no access requested.
	HANDLE handle = CreateFileW(windowsPath.c_str(),
								FILE_WRITE_ATTRIBUTES,
								FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
								nullptr,
								OPEN_EXISTING,
								FILE_FLAG_BACKUP_SEMANTICS,
								nullptr);
	if(handle == INVALID_HANDLE_VALUE) { return asVFSResult(GetLastError()); }

	// Translate the times to Windows file times.
	FILETIME lastAccessFileTime;
	if(setLastAccessTime) { lastAccessFileTime = wavmRealTimeToFileTime(lastAccessTime); }
	FILETIME lastWriteFileTime;
	if(setLastWriteTime) { lastWriteFileTime = wavmRealTimeToFileTime(lastWriteTime); }

	const VFS::Result result = SetFileTime(handle,
										   nullptr,
										   setLastAccessTime ? &lastAccessFileTime : nullptr,
										   setLastWriteTime ? &lastWriteFileTime : nullptr)
								   ? Result::success
								   : asVFSResult(GetLastError());

	if(!CloseHandle(handle))
	{ Errors::fatalf("CloseHandle failed: GetLastError()=%u", GetLastError()); }

	return result;
}

Result WindowsFS::renameFile(const std::string& oldPath, const std::string& newPath)
{
	// Convert the paths from UTF-8 VFS paths (with /) to UTF-16 Windows paths (with \).
	std::wstring oldWindowsPath;
	if(!getWindowsPath(oldPath, oldWindowsPath)) { return Result::invalidNameCharacter; }
	std::wstring newWindowsPath;
	if(!getWindowsPath(newPath, newWindowsPath)) { return Result::invalidNameCharacter; }

	return MoveFileW(oldWindowsPath.c_str(), newWindowsPath.c_str()) ? Result::success
																	 : asVFSResult(GetLastError());
}

Result WindowsFS::unlinkFile(const std::string& path)
{
	// Convert the path from a UTF-8 VFS path (with /) to a UTF-16 Windows path (with \).
	std::wstring windowsPath;
	if(!getWindowsPath(path, windowsPath)) { return Result::invalidNameCharacter; }

	return DeleteFileW(windowsPath.c_str()) ? Result::success : asVFSResult(GetLastError());
}

Result WindowsFS::removeDir(const std::string& path)
{
	// Convert the path from a UTF-8 VFS path (with /) to a UTF-16 Windows path (with \).
	std::wstring windowsPath;
	if(!getWindowsPath(path, windowsPath)) { return Result::invalidNameCharacter; }

	return RemoveDirectoryW(windowsPath.c_str()) ? Result::success : asVFSResult(GetLastError());
}

Result WindowsFS::createDir(const std::string& path)
{
	// Convert the path from a UTF-8 VFS path (with /) to a UTF-16 Windows path (with \).
	std::wstring windowsPath;
	if(!getWindowsPath(path, windowsPath)) { return Result::invalidNameCharacter; }

	return CreateDirectoryW(windowsPath.c_str(), nullptr) ? Result::success
														  : asVFSResult(GetLastError());
}

Result WindowsFS::openDir(const std::string& path, DirEntStream*& outStream)
{
	// Convert the path from a UTF-8 VFS path (with /) to a UTF-16 Windows path (with \).
	std::wstring windowsPath;
	if(!getWindowsPath(path, windowsPath)) { return Result::invalidNameCharacter; }

	// Try to open the file.
	HANDLE handle = CreateFileW(windowsPath.c_str(),
								FILE_LIST_DIRECTORY,
								FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE,
								nullptr,
								OPEN_EXISTING,
								FILE_FLAG_BACKUP_SEMANTICS,
								nullptr);
	if(handle == INVALID_HANDLE_VALUE) { return asVFSResult(GetLastError()); }

	// Try to read the initial buffer-full of dirents to determine whether this is a directory.
	std::vector<DirEnt> initialDirEnts;
	if(readDirEnts(handle, true, initialDirEnts) || GetLastError() == ERROR_NO_MORE_FILES)
	{
		outStream = new WindowsDirEntStream(handle, std::move(initialDirEnts));
		return Result::success;
	}
	else
	{
		const VFS::Result result = GetLastError() == ERROR_INVALID_PARAMETER
									   ? Result::isNotDirectory
									   : asVFSResult(GetLastError());

		// Close the file handle we just opened if there was an error reading dirents from it.
		if(!CloseHandle(handle))
		{ Errors::fatalf("CloseHandle failed: GetLastError()=%u", GetLastError()); }

		return result;
	}
}

std::string Platform::getCurrentWorkingDirectory()
{
	wchar_t buffer[MAX_PATH];
	const DWORD numChars = GetCurrentDirectoryW(MAX_PATH, buffer);
	WAVM_ERROR_UNLESS(numChars);

	// Convert the Windows path (UTF-16 + \) to a VFS path (UTF-8 + /).
	std::string result;
	getVFSPath(buffer, numChars, result, "the result of GetCurrentDirectoryW");

	return result;
}
