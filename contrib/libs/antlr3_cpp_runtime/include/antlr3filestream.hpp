#ifndef	_ANTLR3_FILESTREAM_HPP
#define	_ANTLR3_FILESTREAM_HPP

// [The "BSD licence"]
// Copyright (c) 2005-2009 Gokulakannan Somasundaram, ElectronDB

//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. The name of the author may not be used to endorse or promote products
//    derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
// OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
// INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
// NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

namespace antlr3 {

template<class ImplTraits>
class FileUtils
{
public:
	/** \brief Open an operating system file and return the descriptor
	 * We just use the common open() and related functions here. 
	 * Later we might find better ways on systems
	 * such as Windows and OpenVMS for instance. But the idea is to read the 
	 * while file at once anyway, so it may be irrelevant.
	 */
	static ANTLR_FDSC	AntlrFopen(const ANTLR_UINT8* filename, const char * mode);

	/** \brief Close an operating system file and free any handles
	 *  etc.
	 */
	static void		AntlrFclose	(ANTLR_FDSC fd);

	static ANTLR_UINT32	AntlrFsize(const ANTLR_UINT8* filename);
	template<typename InputStreamType>
	static ANTLR_UINT32	AntlrRead8Bit(InputStreamType* input, const ANTLR_UINT8* fileName);
	static ANTLR_UINT32	AntlrFread(ANTLR_FDSC fdsc, ANTLR_UINT32 count,  void* data);

};

class ParseFileAbsentException : public std::exception
{
	virtual const char* what() const noexcept
	{
		return " Parse File not Present";
	}
};

}

#include "antlr3filestream.inl"

#endif
