/** \file
 * Defines the basic structures used to manipulate character
 * streams from any input source. Any character size and encoding
 * can in theory be used, so long as a set of functinos is provided that
 * can return a 32 bit Integer representation of their characters amd efficiently mark and revert
 * to specific offsets into their input streams.
 */
#ifndef	_ANTLR_INPUT_HPP
#define	_ANTLR_INPUT_HPP

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

/// Master context structure for an ANTLR3 C runtime based input stream.
/// \ingroup apistructures. Calling LT on this doesn't seem right. You would
/// call it only with parser / TreeParser, and their respective input streams 
/// has that function. calling it from lexer will throw a compile time error
///

template<class ImplTraits>
class	InputStream :   public ImplTraits::template IntStreamType< typename ImplTraits::InputStreamType >
{
public:
	typedef typename ImplTraits::AllocPolicyType AllocPolicyType;
	typedef typename ImplTraits::LexStateType LexStateType;
	typedef typename ImplTraits::template IntStreamType< typename ImplTraits::InputStreamType > IntStreamType;
	typedef IntStreamType BaseType;
	typedef typename ImplTraits::StreamDataType UnitType;
	typedef UnitType DataType;
	typedef UnitType TokenType;
	typedef typename AllocPolicyType::template VectorType<LexStateType> MarkersType;
	typedef typename ImplTraits::StringType StringType;

private:
    /** Pointer the start of the input string, characters may be
     *  taken as offsets from here and in original input format encoding.
     */
    const DataType*		m_data;

    /** Pointer to the next character to be consumed from the input data
     *  This is cast to point at the encoding of the original file that
     *  was read by the functions installed as pointer in this input stream
     *  context instance at file/string/whatever load time.
     */
    const DataType*		m_nextChar;

    /** Number of characters that can be consumed at this point in time.
     *  Mostly this is just what is left in the pre-read buffer, but if the
     *  input source is a stream such as a socket or something then we may
     *  call special read code to wait for more input.
     */
    ANTLR_UINT32	m_sizeBuf;

    /** The line number we are traversing in the input file. This gets incremented
     *  by a newline() call in the lexer grammar actions.
     */
    ANTLR_UINT32	m_line;

    /** Pointer into the input buffer where the current line
     *  started.
     */
    const DataType*		m_currentLine;

    /** The offset within the current line of the current character
     */
    ANTLR_INT32		m_charPositionInLine;

    /** Tracks how deep mark() calls are nested
     */
    ANTLR_UINT32	m_markDepth;

    /** List of mark() points in the input stream
     */
    MarkersType		m_markers;

    /** File name string, set to pointer to memory if
     * you set it manually as it will be free()d
     */
    StringType		m_fileName;

    /** File number, needs to be set manually to some file index of your devising.
     */
    ANTLR_UINT32	m_fileNo;

	/// Character that automatically causes an internal line count
    ///  increment.
    ///
    ANTLR_UCHAR		m_newlineChar;

    /// Indicates the size, in 8 bit units, of a single character. Note that
    /// the C runtime does not deal with surrogates as this would be
    /// slow and complicated. If this is a UTF-8 stream then this field
    /// will be set to 0. Generally you are best working internally with 32 bit characters
    /// as this is the most efficient.
    ///
    ANTLR_UINT8		m_charByteSize;

   /** Indicates if the data pointer was allocated by us, and so should be freed
     *  when the stream dies.
     */
    bool			m_isAllocated;

    /// Indicates the encoding scheme used in this input stream
    ///
    ANTLR_UINT32    m_encoding;

    /* API */
public:
	InputStream(const ANTLR_UINT8* fileName, ANTLR_UINT32 encoding);
	InputStream(const ANTLR_UINT8* data, ANTLR_UINT32 encoding, ANTLR_UINT32 size, ANTLR_UINT8* name);
	~InputStream();
	const DataType* get_data() const;
	bool get_isAllocated() const;
	const DataType* get_nextChar() const;
	ANTLR_UINT32 get_sizeBuf() const;
	ANTLR_UINT32 get_line() const;
	const DataType* get_currentLine() const;
	ANTLR_INT32 get_charPositionInLine() const;
	ANTLR_UINT32 get_markDepth() const;
	MarkersType& get_markers();
	const StringType& get_fileName() const;
	ANTLR_UINT32 get_fileNo() const;
	ANTLR_UCHAR get_newlineChar() const;
	ANTLR_UINT8 get_charByteSize() const;
	ANTLR_UINT32 get_encoding() const;

	void  set_data( DataType* data );
	void  set_isAllocated( bool isAllocated );
	void  set_nextChar( const DataType* nextChar );
	void  set_sizeBuf( ANTLR_UINT32 sizeBuf );
	void  set_line( ANTLR_UINT32 line );
	void  set_currentLine( const DataType* currentLine );
	void  set_charPositionInLine( ANTLR_INT32 charPositionInLine );
	void  set_markDepth( ANTLR_UINT32 markDepth );
	void  set_markers( const MarkersType& markers );
	void  set_fileName( const StringType& fileName );
	void  set_fileNo( ANTLR_UINT32 fileNo );
	void  set_newlineChar( ANTLR_UCHAR newlineChar );
	void  set_charByteSize( ANTLR_UINT8 charByteSize );
	void  set_encoding( ANTLR_UINT32 encoding );

	void inc_charPositionInLine();
	void inc_line();	
	void inc_markDepth();

	IntStreamType*	get_istream();

    /** Function that resets the input stream
     */
    void	reset();

    /** Pointer to a function that reuses and resets an input stream by
     *  supplying a new 'source'
     */
    void    reuse(ANTLR_UINT8* inString, ANTLR_UINT32 size, ANTLR_UINT8* name);

	
    /** Function to return the total size of the input buffer. For streams
     *  this may be just the total we have available so far. This means of course that
     *  the input stream must be careful to accumulate enough input so that any backtracking
     *  can be satisfied.
     */
    ANTLR_UINT32	size();

    /** Function to return a substring of the input stream. String is returned in allocated
     *  memory and is in same encoding as the input stream itself, NOT internal ANTLR_UCHAR form.
     */
    StringType	substr(ANTLR_MARKER start, ANTLR_MARKER stop);

    /** Function to return the current line number in the input stream
     */
    ANTLR_UINT32	get_line();

    /** Function to return the current line buffer in the input stream
     *  The pointer returned is directly into the input stream so you must copy
     *  it if you wish to manipulate it without damaging the input stream. Encoding
     *  is obviously in the same form as the input stream.
     *  \remark
     *    - Note taht this function wil lbe inaccurate if setLine is called as there
     *      is no way at the moment to position the input stream at a particular line 
     *	    number offset.
     */
    const DataType*	getLineBuf();

    /** Function to return the current offset in the current input stream line
     */
    ANTLR_UINT32	get_charPositionInLine();

    /** Function to set the current position in the current line.
     */
    void	set_charPositionInLine(ANTLR_UINT32 position);

    /** Function to override the default newline character that the input stream
     *  looks for to trigger the line/offset and line buffer recording information.
     *  \remark
     *   - By default the chracter '\n' will be installed as the newline trigger character. When this
     *     character is seen by the consume() function then the current line number is incremented and the
     *     current line offset is reset to 0. The Pointer for the line of input we are consuming
     *     is updated to point to the next character after this one in the input stream (which means it
     *     may become invalid if the last newline character in the file is seen (so watch out).
     *   - If for some reason you do not want the counters and pointers to be restee, you can set the 
     *     chracter to some impossible character such as '\0' or whatever.
     *   - This is a single character only, so choose the last character in a sequence of two or more.
     *   - This is only a simple aid to error reporting - if you have a complicated binary input structure
     *     it may not be adequate, but you can always override every function in the input stream with your
     *     own of course, and can even write your own complete input stream set if you like.
     *   - It is your responsiblity to set a valid character for the input stream type. There is no point 
     *     setting this to 0xFFFFFFFF if the input stream is 8 bit ASCII, as this will just be truncated and never
     *	   trigger as the comparison will be (INT32)0xFF == (INT32)0xFFFFFFFF
     */
    void	set_newLineChar(ANTLR_UINT32 newlineChar);
	
	ANTLR_MARKER index_impl();

private:
	/** \brief Use the contents of an operating system file as the input
	 *         for an input stream.
	 *
	 * \param fileName Name of operating system file to read.
	 * \return
	 *	- Pointer to new input stream context upon success
	 *	- One of the ANTLR3_ERR_ defines on error.
	 */
	void createFileStream(const ANTLR_UINT8* fileName);

	/** \brief Use the supplied 'string' as input to the stream
	 *
	 * \param data Pointer to the input data
	 * \return
	 *	- Pointer to new input stream context upon success
	 *	- NULL defines on error.
	 */
	void createStringStream(const ANTLR_UINT8* data);
	void genericSetupStream();

	/// Determine endianess of the input stream and install the
	/// API required for the encoding in that format.
	///
	void setupInputStream();

};

/** \brief Structure for track lex input states as part of mark()
 *  and rewind() of lexer.
 */
template<class ImplTraits>
class	LexState : public ImplTraits::AllocPolicyType
{
public:
	typedef typename ImplTraits::StreamDataType DataType;

private:
        /** Pointer to the next character to be consumed from the input data
     *  This is cast to point at the encoding of the original file that
     *  was read by the functions installed as pointer in this input stream
     *  context instance at file/string/whatever load time.
     */
    const DataType*			m_nextChar;

    /** The line number we are traversing in the input file. This gets incremented
     *  by a newline() call in the lexer grammer actions.
     */
    ANTLR_UINT32	m_line;

    /** Pointer into the input buffer where the current line
     *  started.
     */
    const DataType*			m_currentLine;

    /** The offset within the current line of the current character
     */
    ANTLR_INT32		m_charPositionInLine;

public:
	LexState();
	const DataType* get_nextChar() const;
	ANTLR_UINT32 get_line() const;
	const DataType* get_currentLine() const;
	ANTLR_INT32 get_charPositionInLine() const;
	void  set_nextChar( const DataType* nextChar );
	void  set_line( ANTLR_UINT32 line );
	void  set_currentLine( const DataType* currentLine );
	void  set_charPositionInLine( ANTLR_INT32 charPositionInLine );
};

class ParseNullStringException : public std::exception
{
	virtual const char* what() const noexcept
	{
		return "Null String";
	}
};

}

#include "antlr3input.inl"

#endif	/* _ANTLR_INPUT_H  */
