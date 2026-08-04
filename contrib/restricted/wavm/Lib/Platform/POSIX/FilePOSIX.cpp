#ifdef _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#endif

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/I128.h"
#include "WAVM/Inline/Time.h"
#include "WAVM/Platform/File.h"
#include "WAVM/Platform/Mutex.h"
#include "WAVM/VFS/VFS.h"

#define FILE_OFFSET_IS_64BIT (sizeof(off_t) == 8)

using namespace WAVM;
using namespace WAVM::Platform;
using namespace WAVM::VFS;

static_assert(offsetof(struct iovec, iov_base) == offsetof(IOReadBuffer, data)
				  && offsetof(struct iovec, iov_len) == offsetof(IOReadBuffer, numBytes)
				  && sizeof(((struct iovec*)nullptr)->iov_base) == sizeof(IOReadBuffer::data)
				  && sizeof(((struct iovec*)nullptr)->iov_len) == sizeof(IOReadBuffer::numBytes)
				  && sizeof(struct iovec) == sizeof(IOReadBuffer),
			  "IOReadBuffer must match iovec");

static_assert(offsetof(struct iovec, iov_base) == offsetof(IOWriteBuffer, data)
				  && offsetof(struct iovec, iov_len) == offsetof(IOWriteBuffer, numBytes)
				  && sizeof(((struct iovec*)nullptr)->iov_base) == sizeof(IOWriteBuffer::data)
				  && sizeof(((struct iovec*)nullptr)->iov_len) == sizeof(IOWriteBuffer::numBytes)
				  && sizeof(struct iovec) == sizeof(IOWriteBuffer),
			  "IOWriteBuffer must match iovec");

static Result asVFSResult(int error)
{
	switch(error)
	{
	case ESPIPE: return Result::notSeekable;
	case EIO: return Result::ioDeviceError;
	case EINTR: return Result::interruptedBySignal;
	case EISDIR: return Result::isDirectory;
	case EFAULT: return Result::inaccessibleBuffer;
	case EFBIG: return Result::exceededFileSizeLimit;
	case EPERM: return Result::notPermitted;
	case EOVERFLOW: return Result::notEnoughBits;
	case EMFILE: return Result::outOfProcessFDs;
	case ENOTDIR: return Result::isNotDirectory;
	case EACCES: return Result::notAccessible;
	case EEXIST: return Result::alreadyExists;
	case ENAMETOOLONG: return Result::nameTooLong;
	case ENFILE: return Result::outOfSystemFDs;
	case ENOENT: return Result::doesNotExist;
	case ENOSPC: return Result::outOfFreeSpace;
	case EROFS: return Result::notPermitted;
	case ENOMEM: return Result::outOfMemory;
	case EDQUOT: return Result::outOfQuota;
	case ELOOP: return Result::tooManyLinksInPath;
	case EAGAIN: return Result::wouldBlock;
	case EINPROGRESS: return Result::ioPending;
	case ENOSR: return Result::outOfMemory;
	case ENXIO: return Result::missingDevice;
	case ETXTBSY: return Result::notAccessible;
	case EBUSY: return Result::busy;
	case ENOTEMPTY: return Result::isNotEmpty;
	case EMLINK: return Result::outOfLinksToParentDir;
	case ENOTSUP: return Result::notSupported;

	case EINVAL:
		// This probably needs to be handled differently for each API entry point.
		Errors::fatalfWithCallStack("ERROR_INVALID_PARAMETER");
	case EBADF: Errors::fatalfWithCallStack("EBADF");
	default: Errors::fatalfWithCallStack("Unexpected error code: %i (%s)", error, strerror(error));
	};
}

static FileType getFileTypeFromMode(mode_t mode)
{
	switch(mode & S_IFMT)
	{
	case S_IFBLK: return FileType::blockDevice;
	case S_IFCHR: return FileType::characterDevice;
	case S_IFIFO: return FileType::pipe;
	case S_IFREG: return FileType::file;
	case S_IFDIR: return FileType::directory;
	case S_IFLNK: return FileType::symbolicLink;

	case S_IFSOCK:
	default: return FileType::unknown;
	};
}

#ifdef _DIRENT_HAVE_D_TYPE
static FileType getFileTypeFromDirEntType(U8 type)
{
	switch(type)
	{
	case DT_BLK: return FileType::blockDevice;
	case DT_CHR: return FileType::characterDevice;
	case DT_DIR: return FileType::directory;
	case DT_FIFO: return FileType::pipe;
	case DT_LNK: return FileType::symbolicLink;
	case DT_REG: return FileType::file;

	case DT_SOCK:
	case DT_UNKNOWN:
	default: return FileType::unknown;
	};
}
#endif

static I128 timeToNS(time_t time)
{
	// time_t might be a long, and I128 doesn't have a long constructor, so coerce it to an integer
	// type first.
	const U64 timeInt = U64(time);
	return I128(timeInt) * 1000000000;
}

static void getFileInfoFromStatus(const struct stat& status, FileInfo& outInfo)
{
	outInfo.deviceNumber = status.st_dev;
	outInfo.fileNumber = status.st_ino;
	outInfo.type = getFileTypeFromMode(status.st_mode);
	outInfo.numLinks = status.st_nlink;
	outInfo.numBytes = status.st_size;
	outInfo.lastAccessTime.ns = timeToNS(status.st_atime);
	outInfo.lastWriteTime.ns = timeToNS(status.st_mtime);
	outInfo.creationTime.ns = timeToNS(status.st_ctime);
}

static I32 translateVFDFlags(const VFDFlags& vfsFlags)
{
	I32 flags = 0;

	if(vfsFlags.append) { flags |= O_APPEND; }
	if(vfsFlags.nonBlocking) { flags |= O_NONBLOCK; }

	switch(vfsFlags.syncLevel)
	{
	case VFDSync::none: break;
	case VFDSync::contentsAfterWrite: flags |= O_DSYNC; break;
	case VFDSync::contentsAndMetadataAfterWrite: flags |= O_SYNC; break;

#ifdef __APPLE__
		// Apple doesn't support O_RSYNC.
	case VFDSync::contentsAfterWriteAndBeforeRead:
		Errors::fatal(
			"VFDSync::contentsAfterWriteAndBeforeRead is not yet implemented on Apple "
			"platforms.");
	case VFDSync::contentsAndMetadataAfterWriteAndBeforeRead:
		Errors::fatal(
			"VFDSync::contentsAndMetadataAfterWriteAndBeforeRead is not yet implemented "
			"on Apple platforms.");
#else
	case VFDSync::contentsAfterWriteAndBeforeRead: flags |= O_DSYNC | O_RSYNC; break;
	case VFDSync::contentsAndMetadataAfterWriteAndBeforeRead: flags |= O_SYNC | O_RSYNC; break;
#endif

	default: WAVM_UNREACHABLE();
	}

	return flags;
}

struct POSIXDirEntStream : DirEntStream
{
	POSIXDirEntStream(DIR* inDir) : dir(inDir) {}

	virtual void close() override
	{
		closedir(dir);
		delete this;
	}

	virtual bool getNext(DirEnt& outEntry) override
	{
		Platform::Mutex::Lock lock(mutex);
		errno = 0;
		struct dirent* dirent = readdir(dir);
		if(dirent)
		{
			WAVM_ASSERT(dirent);
			outEntry.fileNumber = dirent->d_ino;
			outEntry.name = dirent->d_name;
#ifdef _DIRENT_HAVE_D_TYPE
			outEntry.type = getFileTypeFromDirEntType(dirent->d_type);
#else
			outEntry.type = FileType::unknown;
#endif
			return true;
		}
		else if(errno == 0)
		{
			// Reached the end of the directory.
			return false;
		}
		else if(errno == ENOENT || errno == EOVERFLOW)
		{
			return false;
		}
		else
		{
			Errors::fatalfWithCallStack("readdir returned unexpected error: %s", strerror(errno));
		}
	}

	virtual void restart() override
	{
		Platform::Mutex::Lock lock(mutex);
		rewinddir(dir);
		maxValidOffset = 0;
	}

	virtual U64 tell() override
	{
		Platform::Mutex::Lock lock(mutex);
		const long offset = telldir(dir);
		WAVM_ERROR_UNLESS(offset >= 0
						  && (LONG_MAX <= UINT64_MAX || (unsigned long)offset <= UINT64_MAX));
		if(U64(offset) > maxValidOffset) { maxValidOffset = U64(offset); }
		return U64(offset);
	}

	virtual bool seek(U64 offset) override
	{
		Platform::Mutex::Lock lock(mutex);
		// Don't allow seeking to higher offsets than have been returned by tell since the last
		// rewind.
		if(offset > maxValidOffset) { return false; };

		WAVM_ERROR_UNLESS(offset <= LONG_MAX);
		seekdir(dir, long(offset));
		return true;
	}

private:
	Platform::Mutex mutex;
	DIR* dir;
	U64 maxValidOffset{0};
};

struct POSIXFD : VFD
{
	const I32 fd;

	POSIXFD(I32 inFD) : fd(inFD) {}

	virtual Result close() override
	{
		WAVM_ASSERT(fd >= 0);
		VFS::Result result = VFS::Result::success;
		if(::close(fd))
		{
			// close is specified by POSIX to leave the file descriptor in an unspecified state when
			// an error occurs: https://pubs.opengroup.org/onlinepubs/009695399/functions/close.html
			// The Linux man page for close also says that close should not be retried after an
			// error: http://man7.org/linux/man-pages/man2/close.2.html#NOTES
			// Assume that even though an error was returned, that the file descriptor was closed.
			// Report the error to caller, but delete the VFD.
		}
		delete this;
		return result;
	}

	virtual Result seek(I64 offset, SeekOrigin origin, U64* outAbsoluteOffset = nullptr) override
	{
		I32 whence = 0;
		switch(origin)
		{
		case SeekOrigin::begin: whence = SEEK_SET; break;
		case SeekOrigin::cur: whence = SEEK_CUR; break;
		case SeekOrigin::end: whence = SEEK_END; break;
		default: WAVM_UNREACHABLE();
		};

		if(!FILE_OFFSET_IS_64BIT && (offset < INT32_MIN || offset > INT32_MAX))
		{ return Result::invalidOffset; }
		const I64 result = lseek(fd, off_t(offset), whence);
		if(result == -1) { return errno == EINVAL ? Result::invalidOffset : asVFSResult(errno); }

		if(outAbsoluteOffset) { *outAbsoluteOffset = U64(result); }
		return Result::success;
	}
	virtual Result readv(const IOReadBuffer* buffers,
						 Uptr numBuffers,
						 Uptr* outNumBytesRead = nullptr,
						 const U64* offset = nullptr) override
	{
		if(outNumBytesRead) { *outNumBytesRead = 0; }

		if(numBuffers == 0) { return Result::success; }
		else if(numBuffers > IOV_MAX)
		{
			return Result::tooManyBuffers;
		}

		if(offset == nullptr)
		{
			// Do the read.
			ssize_t result = ::readv(fd, (const struct iovec*)buffers, numBuffers);
			if(result == -1) { return asVFSResult(errno); }

			if(outNumBytesRead) { *outNumBytesRead = result; }
			return Result::success;
		}
		else
		{
			if(!FILE_OFFSET_IS_64BIT && *offset > INT32_MAX) { return Result::invalidOffset; }

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

			// Allocate a combined buffer.
			U8* combinedBuffer = (U8*)malloc(numBufferBytes);
			if(!combinedBuffer) { return Result::outOfMemory; }

			// Do the read.
			Result vfsResult = Result::success;
			const ssize_t result = pread(fd, combinedBuffer, numBufferBytes, off_t(*offset));
			if(result < 0) { vfsResult = asVFSResult(errno); }
			else
			{
				const Uptr numBytesRead = Uptr(result);

				// Copy the contents of the combined buffer to the individual buffers.
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

			return vfsResult;
		}
	}
	virtual Result writev(const IOWriteBuffer* buffers,
						  Uptr numBuffers,
						  Uptr* outNumBytesWritten = nullptr,
						  const U64* offset = nullptr) override
	{
		if(outNumBytesWritten) { *outNumBytesWritten = 0; }

		if(numBuffers == 0) { return Result::success; }
		else if(numBuffers > IOV_MAX)
		{
			return Result::tooManyBuffers;
		}

		if(offset == nullptr)
		{
			ssize_t result = ::writev(fd, (const struct iovec*)buffers, numBuffers);
			if(result == -1) { return asVFSResult(errno); }

			if(outNumBytesWritten) { *outNumBytesWritten = result; }
			return Result::success;
		}
		else
		{
			if(!FILE_OFFSET_IS_64BIT && *offset > INT32_MAX) { return Result::invalidOffset; }

			// Count the number of bytes in all the buffers.
			Uptr numBufferBytes = 0;
			for(Uptr bufferIndex = 0; bufferIndex < numBuffers; ++bufferIndex)
			{
				const IOWriteBuffer& buffer = buffers[bufferIndex];
				if(numBufferBytes + buffer.numBytes < numBufferBytes)
				{ return Result::tooManyBufferBytes; }
				numBufferBytes += buffer.numBytes;
			}
			if(numBufferBytes > UINT32_MAX) { return Result::tooManyBufferBytes; }

			// Allocate a combined buffer.
			U8* combinedBuffer = (U8*)malloc(numBufferBytes);
			if(!combinedBuffer) { return Result::outOfMemory; }

			// Copy the individual buffers into the combined buffer.
			Uptr numBytesCopied = 0;
			for(Uptr bufferIndex = 0; bufferIndex < numBuffers; ++bufferIndex)
			{
				const IOWriteBuffer& buffer = buffers[bufferIndex];
				const Uptr numBytesToCopy
					= std::min(buffer.numBytes, numBufferBytes - numBytesCopied);
				if(numBytesToCopy)
				{ memcpy(combinedBuffer + numBytesCopied, buffer.data, numBytesToCopy); }
				numBytesCopied += numBytesToCopy;
			}

			// Do the write.
			Result vfsResult = Result::success;
			ssize_t result = pwrite(fd, combinedBuffer, numBufferBytes, off_t(*offset));
			if(result < 0) { vfsResult = asVFSResult(errno); }

			// Write the total number of bytes writte.
			if(outNumBytesWritten) { *outNumBytesWritten = Uptr(result); }

			// Free the combined buffer.
			free(combinedBuffer);

			return vfsResult;
		}
	}
	virtual Result sync(SyncType syncType) override
	{
#ifdef __APPLE__
		I32 result = fsync(fd);
#else
		I32 result;
		switch(syncType)
		{
		case SyncType::contents: result = fdatasync(fd); break;
		case SyncType::contentsAndMetadata: result = fsync(fd); break;
		default: Errors::fatalfWithCallStack("Unexpected errno: %s", strerror(errno));
		}
#endif
		if(result) { return errno == EINVAL ? Result::notSynchronizable : asVFSResult(errno); }

		return Result::success;
	}
	virtual Result getVFDInfo(VFDInfo& outInfo) override
	{
		struct stat fdStatus;
		if(fstat(fd, &fdStatus) != 0) { return asVFSResult(errno); }

		outInfo.type = getFileTypeFromMode(fdStatus.st_mode);

		I32 fdFlags = fcntl(fd, F_GETFL);
		if(fdFlags < 0) { return asVFSResult(errno); }

		outInfo.flags.append = fdFlags & O_APPEND;
		outInfo.flags.nonBlocking = fdFlags & O_NONBLOCK;

		if(fdFlags & O_SYNC)
		{
#ifdef O_RSYNC
			outInfo.flags.syncLevel = fdFlags & O_RSYNC
										  ? VFDSync::contentsAndMetadataAfterWriteAndBeforeRead
										  : VFDSync::contentsAndMetadataAfterWrite;
#else
			outInfo.flags.syncLevel = VFDSync::contentsAndMetadataAfterWrite;
#endif
		}
		else if(fdFlags & O_DSYNC)
		{
#ifdef O_RSYNC
			outInfo.flags.syncLevel = fdFlags & O_RSYNC ? VFDSync::contentsAfterWriteAndBeforeRead
														: VFDSync::contentsAfterWrite;
#else
			outInfo.flags.syncLevel = VFDSync::contentsAfterWrite;
#endif
		}
		else
		{
			outInfo.flags.syncLevel = VFDSync::none;
		}

		return Result::success;
	}

	virtual Result setVFDFlags(const VFDFlags& vfsFlags) override
	{
		const I32 flags = translateVFDFlags(vfsFlags);
		return fcntl(fd, F_SETFL, flags) == 0 ? Result::success : asVFSResult(errno);
	}

	virtual Result setFileSize(U64 numBytes) override
	{
		if(!FILE_OFFSET_IS_64BIT && numBytes > INT32_MAX) { return Result::exceededFileSizeLimit; }

		int result = ftruncate(fd, off_t(numBytes));
		return result == 0 ? Result::success : asVFSResult(errno);
	}
	virtual Result setFileTimes(bool setLastAccessTime,
								Time lastAccessTime,
								bool setLastWriteTime,
								Time lastWriteTime) override
	{
#ifdef HAS_FUTIMENS
		struct timespec timespecs[2];

		if(!setLastAccessTime) { timespecs[0].tv_nsec = UTIME_OMIT; }
		else
		{
			timespecs[0].tv_sec = U64(lastAccessTime.ns / 1000000000);
			timespecs[0].tv_nsec = U32(lastAccessTime.ns % 1000000000);
		}

		if(!setLastWriteTime) { timespecs[1].tv_nsec = UTIME_OMIT; }
		else
		{
			timespecs[1].tv_sec = U64(lastWriteTime.ns / 1000000000);
			timespecs[1].tv_nsec = U32(lastWriteTime.ns % 1000000000);
		}

		return futimens(fd, timespecs) == 0 ? Result::success : asVFSResult(errno);
#else
		// MacOS pre-10.13 does not have futimens, so fall back to utimes, which only has
		// microsecond precision, and no equivalent of UTIME_OMIT.
		// If !setLastAccessTime or !setLastWriteTime, use fstat to read the current times.
		// This isn't atomic, but seems like the best we can do without futimens.
		if(!setLastAccessTime || !setLastWriteTime)
		{
			struct stat fdStatus;
			if(fstat(fd, &fdStatus)) { return asVFSResult(errno); }

			if(!setLastAccessTime) { lastAccessTime.ns = timeToNS(fdStatus.st_atime); }
			if(!setLastWriteTime) { lastWriteTime.ns = timeToNS(fdStatus.st_mtime); }
		}

		struct timeval timevals[2];
		timevals[0].tv_sec = U64(lastAccessTime.ns / 1000000000);
		timevals[0].tv_usec = U64(lastAccessTime.ns / 1000 % 1000000);
		timevals[1].tv_sec = U64(lastWriteTime.ns / 1000000000);
		timevals[1].tv_usec = U64(lastWriteTime.ns / 1000 % 1000000);

		// utimes takes a path instead of a fd, so use the BSD fcntl(F_GETPATH) to get the path to
		// the fd's file.
		char fdPath[PATH_MAX + 1];
		if(fcntl(fd, F_GETPATH, fdPath)) { return asVFSResult(errno); }
		fdPath[PATH_MAX] = 0;

		return utimes(fdPath, timevals) == 0 ? Result::success : asVFSResult(errno);
#endif
	}

	virtual Result getFileInfo(FileInfo& outInfo) override
	{
		struct stat fdStatus;

		if(fstat(fd, &fdStatus)) { return asVFSResult(errno); }

		getFileInfoFromStatus(fdStatus, outInfo);
		return Result::success;
	}

	virtual Result openDir(DirEntStream*& outStream) override
	{
		const I32 duplicateFD = dup(fd);
		if(duplicateFD < 0) { return asVFSResult(errno); }

		DIR* dir = fdopendir(duplicateFD);
		if(!dir) { return asVFSResult(errno); }

		// Rewind the dir to the beginning to ensure previous seeks on the FD don't affect the
		// dirent stream.
		rewinddir(dir);

		outStream = new POSIXDirEntStream(dir);
		return Result::success;
	}
};

struct POSIXStdFD : POSIXFD
{
	POSIXStdFD(I32 inFD) : POSIXFD(inFD) {}

	virtual Result close() override
	{
		// The stdio FDs are shared, so don't close them.
		return Result::success;
	}
};

VFD* Platform::getStdFD(StdDevice device)
{
	static POSIXStdFD* stdinVFD = new POSIXStdFD(0);
	static POSIXStdFD* stdoutVFD = new POSIXStdFD(1);
	static POSIXStdFD* stderrVFD = new POSIXStdFD(2);
	switch(device)
	{
	case StdDevice::in: return stdinVFD; break;
	case StdDevice::out: return stdoutVFD; break;
	case StdDevice::err: return stderrVFD; break;
	default: WAVM_UNREACHABLE();
	};
}

struct POSIXFS : HostFS
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

	static POSIXFS& get()
	{
		static POSIXFS posixFS;
		return posixFS;
	}

protected:
	POSIXFS() {}
};

HostFS& Platform::getHostFS() { return POSIXFS::get(); }

Result POSIXFS::open(const std::string& path,
					 FileAccessMode accessMode,
					 FileCreateMode createMode,
					 VFD*& outFD,
					 const VFDFlags& vfsFlags)
{
	U32 flags = 0;
	switch(accessMode)
	{
	case FileAccessMode::none: flags = O_RDONLY; break;
	case FileAccessMode::readOnly: flags = O_RDONLY; break;
	case FileAccessMode::writeOnly: flags = O_WRONLY; break;
	case FileAccessMode::readWrite: flags = O_RDWR; break;
	default: WAVM_UNREACHABLE();
	};

	switch(createMode)
	{
	case FileCreateMode::createAlways: flags |= O_CREAT | O_TRUNC; break;
	case FileCreateMode::createNew: flags |= O_CREAT | O_EXCL; break;
	case FileCreateMode::openAlways: flags |= O_CREAT; break;
	case FileCreateMode::openExisting: break;
	case FileCreateMode::truncateExisting: flags |= O_TRUNC; break;
	default: WAVM_UNREACHABLE();
	};

	mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;

	flags |= translateVFDFlags(vfsFlags);

	const I32 fd = ::open(path.c_str(), flags, mode);
	if(fd == -1) { return asVFSResult(errno); }

	outFD = new POSIXFD(fd);
	return Result::success;
}

Result POSIXFS::getFileInfo(const std::string& path, VFS::FileInfo& outInfo)
{
	struct stat fileStatus;

	if(stat(path.c_str(), &fileStatus)) { return asVFSResult(errno); }

	getFileInfoFromStatus(fileStatus, outInfo);
	return Result::success;
}

Result POSIXFS::setFileTimes(const std::string& path,
							 bool setLastAccessTime,
							 Time lastAccessTime,
							 bool setLastWriteTime,
							 Time lastWriteTime)
{
#ifdef HAS_UTIMENSAT
	struct timespec timespecs[2];

	if(!setLastAccessTime) { timespecs[0].tv_nsec = UTIME_OMIT; }
	else
	{
		timespecs[0].tv_sec = U64(lastAccessTime.ns / 1000000000);
		timespecs[0].tv_nsec = U32(lastAccessTime.ns % 1000000000);
	}

	if(!setLastWriteTime) { timespecs[1].tv_nsec = UTIME_OMIT; }
	else
	{
		timespecs[1].tv_sec = U64(lastWriteTime.ns / 1000000000);
		timespecs[1].tv_nsec = U32(lastWriteTime.ns % 1000000000);
	}

	return utimensat(AT_FDCWD, path.c_str(), timespecs, 0) == 0 ? Result::success
																: asVFSResult(errno);
#else
	// MacOS pre-10.13 does not have utimensat, so fall back to utimes, which only has microsecond
	// precision, and no equivalent of UTIME_OMIT.
	// If !setLastAccessTime or !setLastWriteTime, use stat to read the current times.
	// This isn't atomic, but seems like the best we can do without futimens.
	if(!setLastAccessTime || !setLastWriteTime)
	{
		struct stat fileStatus;
		if(stat(path.c_str(), &fileStatus)) { return asVFSResult(errno); }

		if(!setLastAccessTime) { lastAccessTime.ns = timeToNS(fileStatus.st_atime); }
		if(!setLastWriteTime) { lastWriteTime.ns = timeToNS(fileStatus.st_mtime); }
	}

	struct timeval timevals[2];
	timevals[0].tv_sec = U64(lastAccessTime.ns / 1000000000);
	timevals[0].tv_usec = U64(lastAccessTime.ns / 1000 % 1000000);
	timevals[1].tv_sec = U64(lastWriteTime.ns / 1000000000);
	timevals[1].tv_usec = U64(lastWriteTime.ns / 1000 % 1000000);

	return utimes(path.c_str(), timevals) == 0 ? Result::success : asVFSResult(errno);
#endif
}

Result POSIXFS::openDir(const std::string& path, DirEntStream*& outStream)
{
	DIR* dir = opendir(path.c_str());
	if(!dir) { return asVFSResult(errno); }

	outStream = new POSIXDirEntStream(dir);
	return Result::success;
}

Result POSIXFS::renameFile(const std::string& oldPath, const std::string& newPath)
{
	return !rename(oldPath.c_str(), newPath.c_str()) ? VFS::Result::success : asVFSResult(errno);
}

Result POSIXFS::unlinkFile(const std::string& path)
{
	return !unlink(path.c_str()) ? VFS::Result::success : asVFSResult(errno);
}

Result POSIXFS::removeDir(const std::string& path)
{
	return !unlinkat(AT_FDCWD, path.c_str(), AT_REMOVEDIR) ? Result::success : asVFSResult(errno);
}

Result POSIXFS::createDir(const std::string& path)
{
	return !mkdir(path.c_str(), 0666) ? Result::success : asVFSResult(errno);
}

std::string Platform::getCurrentWorkingDirectory()
{
	const Uptr maxPathBytes = pathconf(".", _PC_PATH_MAX);
	char* buffer = (char*)alloca(maxPathBytes);
	WAVM_ERROR_UNLESS(getcwd(buffer, maxPathBytes) == buffer);
	return std::string(buffer);
}
