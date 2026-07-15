#pragma once

#include <string>
#include <vector>
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/Platform/File.h"
#include "WAVM/VFS/VFS.h"

namespace WAVM {
	inline bool loadFile(const char* filename, std::vector<U8>& outFileContents)
	{
		VFS::Result result;
		VFS::VFD* vfd = nullptr;
		{
			result = Platform::getHostFS().open(
				filename, VFS::FileAccessMode::readOnly, VFS::FileCreateMode::openExisting, vfd);
			if(result != VFS::Result::success) { goto printAndReturnError; }

			// Ensure that we didn't open a directory.
			VFS::FileInfo fileInfo;
			result = vfd->getFileInfo(fileInfo);
			if(result != VFS::Result::success) { goto printAndReturnError; }
			else if(fileInfo.type == VFS::FileType::directory)
			{
				result = VFS::Result::isDirectory;
				goto printAndReturnError;
			}

			U64 numFileBytes64 = 0;
			result = vfd->seek(0, VFS::SeekOrigin::end, &numFileBytes64);
			if(result != VFS::Result::success) { goto printAndReturnError; }
			else if(numFileBytes64 > UINTPTR_MAX)
			{
				result = VFS::Result::outOfMemory;
				goto printAndReturnError;
			}

			const Uptr numFileBytes = Uptr(numFileBytes64);
			outFileContents.resize(numFileBytes);

			result = vfd->seek(0, VFS::SeekOrigin::begin);
			if(result != VFS::Result::success) { goto printAndReturnError; }

			result = vfd->read(const_cast<U8*>(outFileContents.data()), numFileBytes);
			if(result != VFS::Result::success) { goto printAndReturnError; }

			result = vfd->close();
			vfd = nullptr;
			if(result != VFS::Result::success) { goto printAndReturnError; }

			return true;
		}
	printAndReturnError:
		Log::printf(Log::error, "Error loading '%s': %s\n", filename, VFS::describeResult(result));
		if(vfd) { WAVM_ERROR_UNLESS(vfd->close() == VFS::Result::success); }
		return false;
	}

	inline bool saveFile(const char* filename, const void* fileBytes, Uptr numFileBytes)
	{
		VFS::Result result;
		VFS::VFD* vfd = nullptr;
		{
			result = Platform::getHostFS().open(
				filename, VFS::FileAccessMode::writeOnly, VFS::FileCreateMode::createAlways, vfd);
			if(result != VFS::Result::success) { goto printAndReturnError; }

			result = vfd->write(fileBytes, numFileBytes);
			if(result != VFS::Result::success) { goto printAndReturnError; }

			result = vfd->close();
			if(result != VFS::Result::success)
			{
				vfd = nullptr;
				goto printAndReturnError;
			}

			return true;
		}
	printAndReturnError:
		Log::printf(Log::error, "Error saving '%s': %s\n", filename, VFS::describeResult(result));
		if(vfd) { WAVM_ERROR_UNLESS(vfd->close() == VFS::Result::success); }
		return false;
	}

	inline const char* getEnvironmentVariableHelpText()
	{
		return "Environment variables:\n"
			   "  WAVM_OUTPUT=<category>(,<category>)*\n"
			   "    Enables printing of different categories of information to stdout.\n"
			   "    Categories:\n"
			   "      metrics            Performance and memory use metrics\n"
			   "      debug              Debug information\n"
			   "      trace-validation   Trace instructions as they are validated\n"
			   "      trace-compilation  Trace instructions as they are compiled\n"
			   "\n"
			   "  WAVM_OBJECT_CACHE_DIR=<directory>\n"
			   "    Specifies a directory that WAVM will use to cache compiled object\n"
			   "    code for WebAssembly modules.\n"
			   "\n"
			   "  WAVM_OBJECT_CACHE_MAX_MB=<n>\n"
			   "    Specifies the maximum amount of disk space that WAVM will use to\n"
			   "    cache compiled object code.\n";
	}

	inline bool initLogFromEnvironment()
	{
		const char* wavmOutputEnv = WAVM_SCOPED_DISABLE_SECURE_CRT_WARNINGS(getenv("WAVM_OUTPUT"));
		if(wavmOutputEnv && *wavmOutputEnv)
		{
			std::string category;
			for(Uptr charIndex = 0;; ++charIndex)
			{
				const char c = wavmOutputEnv[charIndex];
				if(c && c != ',') { category += c; }
				else
				{
					if(category == "metrics") { Log::setCategoryEnabled(Log::metrics, true); }
					else if(category == "debug")
					{
						Log::setCategoryEnabled(Log::debug, true);
					}
					else if(category == "trace-validation")
					{
						Log::setCategoryEnabled(Log::traceValidation, true);
					}
					else if(category == "trace-compilation")
					{
						Log::setCategoryEnabled(Log::traceCompilation, true);
					}
					else
					{
						Log::printf(Log::error,
									"Invalid category in WAVM_OUTPUT environment: %s\n",
									category.c_str());
						return false;
					}

					if(!c) { break; }
					else
					{
						category.clear();
					}
				}
			}
		}

		return true;
	}
}
