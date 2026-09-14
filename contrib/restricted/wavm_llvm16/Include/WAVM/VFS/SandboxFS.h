#pragma once

#include <memory>
#include <string>

namespace WAVM { namespace VFS {
	struct FileSystem;
	WAVM_API std::shared_ptr<FileSystem> makeSandboxFS(FileSystem* innerFS,
													   const std::string& innerRootPath);
}}
