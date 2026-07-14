#pragma once

#include <string>
#include <vector>
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Logging/Logging.h"

namespace WAVM { namespace IR {
	struct Module;
}}

namespace WAVM { namespace WAST {
	// A location in a text file.
	struct TextFileLocus
	{
		Uptr lineStartOffset;
		Uptr lineEndOffset;

		U32 newlines;
		U32 tabs;
		U32 characters;

		TextFileLocus() : newlines(0), tabs(0), characters(0) {}

		U32 lineNumber() const { return newlines + 1; }
		U32 column(U32 spacesPerTab = 4) const { return tabs * spacesPerTab + characters + 1; }

		std::string describe(U32 spacesPerTab = 4) const
		{
			return std::to_string(lineNumber()) + ":" + std::to_string(column(spacesPerTab));
		}

		friend bool operator==(const TextFileLocus& a, const TextFileLocus& b)
		{
			return a.lineStartOffset == b.lineStartOffset && a.lineEndOffset == b.lineEndOffset
				   && a.newlines == b.newlines && a.tabs == b.tabs && a.characters == b.characters;
		}

		friend bool operator!=(const TextFileLocus& a, const TextFileLocus& b) { return !(a == b); }
	};

	// A WAST parse error.
	struct Error
	{
		TextFileLocus locus;
		std::string message;

		friend bool operator==(const Error& a, const Error& b)
		{
			return a.locus == b.locus && a.message == b.message;
		}

		friend bool operator!=(const Error& a, const Error& b) { return !(a == b); }
	};

	// Parse a module from a string. Returns true if it succeeds, and writes the module to
	// outModule. If it fails, returns false and appends a list of errors to outErrors.
	WAVM_API bool parseModule(const char* string,
							  Uptr stringLength,
							  IR::Module& outModule,
							  std::vector<Error>& outErrors);

	WAVM_API void reportParseErrors(const char* filename,
									const char* source,
									const std::vector<Error>& parseErrors,
									Log::Category outputCategory = Log::Category::error);
}}
