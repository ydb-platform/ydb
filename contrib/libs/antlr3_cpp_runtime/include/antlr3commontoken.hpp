/** \file
 * \brief Defines the interface for a common token.
 *
 * All token streams should provide their tokens using an instance
 * of this common token. A custom pointer is provided, wher you may attach
 * a further structure to enhance the common token if you feel the need
 * to do so. The C runtime will assume that a token provides implementations
 * of the interface functions, but all of them may be rplaced by your own
 * implementation if you require it.
 */
#ifndef	_ANTLR3_COMMON_TOKEN_HPP
#define	_ANTLR3_COMMON_TOKEN_HPP

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

/** The definition of an ANTLR3 common token structure, which all implementations
 * of a token stream should provide, installing any further structures in the
 * custom pointer element of this structure.
 *
 * \remark
 * Token streams are in essence provided by lexers or other programs that serve
 * as lexers.
 */

template<class ImplTraits>
class CommonToken : public ImplTraits::AllocPolicyType
{
public:
	/* Base token types, which all lexer/parser tokens come after in sequence.
	*/
	enum TOKEN_TYPE : ANTLR_UINT32
	{
		/** Indicator of an invalid token
		 */
		TOKEN_INVALID =	0
		, EOR_TOKEN_TYPE	
		/** Imaginary token type to cause a traversal of child nodes in a tree parser
		 */
		, TOKEN_DOWN		
		/** Imaginary token type to signal the end of a stream of child nodes.
		 */
		, TOKEN_UP	
		/** First token that can be used by users/generated code
		 */
		, MIN_TOKEN_TYPE =	TOKEN_UP + 1

		/** End of file token
		 */
#ifndef  _MSC_VER
		, TOKEN_EOF = std::numeric_limits<ANTLR_UINT32>::max()
#else
		, TOKEN_EOF = 0xFFFFFFFF
#endif
	};

	typedef typename ImplTraits::TokenIntStreamType TokenIntStreamType;
	typedef typename ImplTraits::StringType StringType;
	typedef typename ImplTraits::InputStreamType InputStreamType;
	typedef typename ImplTraits::StreamDataType StreamDataType;
	typedef typename ImplTraits::TokenUserDataType UserDataType;

private:
    /** The actual type of this token
     */
    ANTLR_UINT32   m_type;

	/** The virtual channel that this token exists in.
     */
    ANTLR_UINT32	m_channel;
	
	mutable StringType		m_tokText;

    /** The offset into the input stream that the line in which this
     *  token resides starts.
     */
	const StreamDataType*	m_lineStart;

	/** The line number in the input stream where this token was derived from
     */
    ANTLR_UINT32	m_line;

    /** The character position in the line that this token was derived from
     */
    ANTLR_INT32		m_charPositionInLine;

    /** Pointer to the input stream that this token originated in.
     */
    InputStreamType*    m_input;

    /** What the index of this token is, 0, 1, .., n-2, n-1 tokens
     */
    ANTLR_MARKER		m_index;

    /** The character offset in the input stream where the text for this token
     *  starts.
     */
    ANTLR_MARKER		m_startIndex;

    /** The character offset in the input stream where the text for this token
     *  stops.
     */
    ANTLR_MARKER		m_stopIndex;

public:
	CommonToken();
	CommonToken(ANTLR_UINT32 type);
	CommonToken(TOKEN_TYPE type);
	CommonToken( const CommonToken& ctoken );

	~CommonToken() {}

	CommonToken& operator=( const CommonToken& ctoken );
	bool operator==( const CommonToken& ctoken ) const;
	bool operator<( const CommonToken& ctoken ) const;

	InputStreamType* get_input() const;
	ANTLR_MARKER get_index() const;
	void set_index( ANTLR_MARKER index );
	void set_input( InputStreamType* input );

    /* ==============================
     * API 
     */

    /** Function that returns the text pointer of a token, use
     *  toString() if you want a pANTLR3_STRING version of the token.
     */
    StringType const & getText() const;
	
    /** Pointer to a function that 'might' be able to set the text associated
     *  with a token. Imaginary tokens such as an ANTLR3_CLASSIC_TOKEN may actually
     *  do this, however many tokens such as ANTLR3_COMMON_TOKEN do not actaully have
     *  strings associated with them but just point into the current input stream. These
     *  tokens will implement this function with a function that errors out (probably
     *  drastically.
     */
    void set_tokText( const StringType& text );

    /** Pointer to a function that 'might' be able to set the text associated
     *  with a token. Imaginary tokens such as an ANTLR3_CLASSIC_TOKEN may actually
     *  do this, however many tokens such as ANTLR3_COMMON_TOKEN do not actully have
     *  strings associated with them but just point into the current input stream. These
     *  tokens will implement this function with a function that errors out (probably
     *  drastically.
     */
    void	setText(ANTLR_UINT8* text);
	void	setText(const char* text);

    /** Pointer to a function that returns the token type of this token
     */
    ANTLR_UINT32  get_type() const;
	ANTLR_UINT32  getType() const;

    /** Pointer to a function that sets the type of this token
     */
    void	set_type(ANTLR_UINT32 ttype);

    /** Pointer to a function that gets the 'line' number where this token resides
     */
    ANTLR_UINT32   get_line() const;

    /** Pointer to a function that sets the 'line' number where this token reside
     */
    void set_line(ANTLR_UINT32 line);

    /** Pointer to a function that gets the offset in the line where this token exists
     */ 
    ANTLR_INT32  get_charPositionInLine() const;
	ANTLR_INT32  getCharPositionInLine() const;

    /** Pointer to a function that sets the offset in the line where this token exists
     */
    void	set_charPositionInLine(ANTLR_INT32 pos);

    /** Pointer to a function that gets the channel that this token was placed in (parsers
     *  can 'tune' to these channels.
     */
    ANTLR_UINT32   get_channel() const;

    /** Pointer to a function that sets the channel that this token should belong to
     */
    void set_channel(ANTLR_UINT32 channel);

    /** Pointer to a function that returns an index 0...n-1 of the token in the token
     *  input stream.
     */
    ANTLR_MARKER  get_tokenIndex() const;

    /** Pointer to a function that can set the token index of this token in the token
     *  input stream.
     */
    void	set_tokenIndex(ANTLR_MARKER tokenIndex);

    /** Pointer to a function that gets the start index in the input stream for this token.
     */
    ANTLR_MARKER   get_startIndex() const;

    /** Pointer to a function that sets the start index in the input stream for this token.
     */
    void	set_startIndex(ANTLR_MARKER index);
    
    /** Pointer to a function that gets the stop index in the input stream for this token.
     */
    ANTLR_MARKER  get_stopIndex() const;

    /** Pointer to a function that sets the stop index in the input stream for this token.
     */
    void	set_stopIndex(ANTLR_MARKER index);
	const StreamDataType* get_lineStart() const;
	void	set_lineStart( const StreamDataType* lineStart );

    /** Pointer to a function that returns this token as a text representation that can be 
     *  printed with embedded control codes such as \n replaced with the printable sequence "\\n"
     *  This also yields a string structure that can be used more easily than the pointer to 
     *  the input stream in certain situations.
     */
    StringType  toString() const;

	UserDataType UserData;	
};

}

#include "antlr3commontoken.inl"

#endif
