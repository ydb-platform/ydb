/** \file
 * Defines the the class interface for an antlr3 INTSTREAM.
 * 
 * Certain functionality (such as DFAs for instance) abstract the stream of tokens
 * or characters in to a steam of integers. Hence this structure should be included
 * in any stream that is able to provide the output as a stream of integers (which is anything
 * basically.
 *
 * There are no specific implementations of the methods in this interface in general. Though
 * for purposes of casting and so on, it may be necesssary to implement a function with
 * the signature in this interface which abstracts the base immplementation. In essence though
 * the base stream provides a pointer to this interface, within which it installs its
 * normal match() functions and so on. Interaces such as DFA are then passed the pANTLR3_INT_STREAM
 * and can treat any input as an int stream. 
 *
 * For instance, a lexer implements a pANTLR3_BASE_RECOGNIZER, within which there is a pANTLR3_INT_STREAM.
 * However, a pANTLR3_INPUT_STREAM also provides a pANTLR3_INT_STREAM, which it has constructed from
 * it's normal interface when it was created. This is then pointed at by the pANTLR_BASE_RECOGNIZER
 * when it is intialized with a pANTLR3_INPUT_STREAM.
 *
 * Similarly if a pANTLR3_BASE_RECOGNIZER is initialized with a pANTLR3_TOKEN_STREAM, then the 
 * pANTLR3_INT_STREAM is taken from the pANTLR3_TOKEN_STREAM. 
 *
 * If a pANTLR3_BASE_RECOGNIZER is initialized with a pANTLR3_TREENODE_STREAM, then guess where
 * the pANTLR3_INT_STREAM comes from?
 *
 * Note that because the context pointer points to the actual interface structure that is providing
 * the ANTLR3_INT_STREAM it is defined as a (void *) in this interface. There is no direct implementation
 * of an ANTLR3_INT_STREAM (unless someone did not understand what I was doing here =;?P
 */
#ifndef	_ANTLR3_INTSTREAM_HPP
#define	_ANTLR3_INTSTREAM_HPP

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

enum STREAM_TYPE
{
	/** Type indicator for a character stream
	 * \remark if a custom stream is created but it can be treated as
	 * a char stream, then you may OR in this value to your type indicator
	 */
	CHARSTREAM	= 0x0001

	/** Type indicator for a Token stream
	 * \remark if a custom stream is created but it can be treated as
	 * a token stream, then you may OR in this value to your type indicator
	 */
	, TOKENSTREAM = 0x0002

	/** Type indicator for a common tree node stream
	 * \remark if a custom stream is created but it can be treated as
	 * a common tree node stream, then you may OR in this value to your type indicator
	 */
	, COMMONTREENODE = 0x0004

	/** Type mask for input stream so we can switch in the above types
	*  \remark DO NOT USE 0x0000 as a stream type!
	*/
	, INPUT_MASK =	0x0007
};

class RESOLVE_ENDIAN_AT_RUNTIME {};
class BYTE_AGNOSTIC {};
class ANTLR_LITTLE_ENDIAN {};
class ANTLR_BIG_ENDIAN {};

template<class ImplTraits, class SuperType>
class IntStream : public ImplTraits::AllocPolicyType
{
public:
	typedef typename ImplTraits::StringType StringType;
	
protected:
    /** Potentially useful in error reporting and so on, this string is
     *  an identification of the input source. It may be NULL, so anything
     *  attempting to access it needs to check this and substitute a sensible
     *  default.
     */
    StringType		m_streamName;

    /** Last marker position allocated
     */
    ANTLR_MARKER	m_lastMarker;
	
    bool		m_upper_case; //if set, values should be returbed in upper case	

    /// Indicates whether we should implement endian-specific logic
    /// 0 - Undefined 1 - Default(machine and input are both same), 2 - Little Endian, 3 - Big Endian
    ANTLR_UINT8		m_endian_spec;	

public:
	IntStream();
	
	// Return a string that identifies the input source
	//
	StringType		getSourceName();
	StringType& 	get_streamName();
	const StringType& 	get_streamName() const;
	ANTLR_MARKER get_lastMarker() const;

	SuperType* get_super();
	/**
     * Function that installs a version of LA that always
     * returns upper case. Only valid for character streams and creates a case
     * insensitive lexer if the lexer tokens are described in upper case. The
     * tokens will preserve case in the token text.
     */
    void	setUcaseLA(bool flag);

    /** Consume the next 'ANTR3_UINT32' in the stream
     */
    void		    consume();

    /** Get ANTLR3_UINT32 at current input pointer + i ahead where i=1 is next ANTLR3_UINT32 
     */
    ANTLR_UINT32	LA( ANTLR_INT32 i);

    /** Tell the stream to start buffering if it hasn't already.  Return
     *  current input position, index(), or some other marker so that
     *  when passed to rewind() you get back to the same spot.
     *  rewind(mark()) should not affect the input cursor.
     */
    ANTLR_MARKER	    mark();
    
    /** Return the current input symbol index 0..n where n indicates the
     *  last symbol has been read.
     */
    ANTLR_MARKER	    index();

    /** Reset the stream so that next call to index would return marker.
     *  The marker will usually be index() but it doesn't have to be.  It's
     *  just a marker to indicate what state the stream was in.  This is
     *  essentially calling release() and seek().  If there are markers
     *  created after this marker argument, this routine must unroll them
     *  like a stack.  Assume the state the stream was in when this marker
     *  was created.
     */
    void	rewind(ANTLR_MARKER marker);

    /** Reset the stream to the last marker position, witouh destryoing the
     *  last marker position.
     */
    void	rewindLast();

    /** You may want to commit to a backtrack but don't want to force the
     *  stream to keep bookkeeping objects around for a marker that is
     *  no longer necessary.  This will have the same behavior as
     *  rewind() except it releases resources without the backward seek.
     */
    void	release(ANTLR_MARKER mark);

    /** Set the input cursor to the position indicated by index.  This is
     *  normally used to seek ahead in the input stream.  No buffering is
     *  required to do this unless you know your stream will use seek to
     *  move backwards such as when backtracking.
     *
     *  This is different from rewind in its multi-directional
     *  requirement and in that its argument is strictly an input cursor (index).
     *
     *  For char streams, seeking forward must update the stream state such
     *  as line number.  For seeking backwards, you will be presumably
     *  backtracking using the mark/rewind mechanism that restores state and
     *  so this method does not need to update state when seeking backwards.
     *
     *  Currently, this method is only used for efficient backtracking, but
     *  in the future it may be used for incremental parsing.
     */
    void	seek(ANTLR_MARKER index);

	/// Debug only method to flag consumption of initial off-channel
	/// tokens in the input stream
	///
	void consumeInitialHiddenTokens();

	void  rewindMark(ANTLR_MARKER marker);
	ANTLR_MARKER tindex();

    /** Frees any resources that were allocated for the implementation of this
     *  interface. Usually this is just releasing the memory allocated
     *  for the structure itself, but it may of course do anything it need to
     *  so long as it does not stamp on anything else.
     */
	~IntStream();

protected:
	void setupIntStream(bool machineBigEndian, bool inputBigEndian);
	void findout_endian_spec(bool machineBigEndian, bool inputBigEndian);

	//If the user chooses this option, then we will be resolving stuffs at run-time
	ANTLR_UINT32	LA( ANTLR_INT32 i, ClassForwarder<RESOLVE_ENDIAN_AT_RUNTIME> );

	//resolve into one of the three categories below at runtime
	void	consume( ClassForwarder<RESOLVE_ENDIAN_AT_RUNTIME> );
};

template<class ImplTraits, class SuperType>
class EBCDIC_IntStream : public IntStream<ImplTraits, SuperType>
{
public:
	ANTLR_UINT32	LA( ANTLR_INT32 i);

protected:
	void setupIntStream();
};

template<class ImplTraits, class SuperType>
class UTF8_IntStream : public IntStream<ImplTraits, SuperType>
{
public:
	ANTLR_UINT32	LA( ANTLR_INT32 i);
	void consume();

protected:
	void setupIntStream(bool machineBigEndian, bool inputBigEndian);

private:
	static const ANTLR_UINT32* TrailingBytesForUTF8();
	static const UTF32* OffsetsFromUTF8();
};

template<class ImplTraits, class SuperType>
class UTF16_IntStream : public IntStream<ImplTraits, SuperType>
{
public:
	ANTLR_UINT32	LA( ANTLR_INT32 i);
	void		    consume();
	ANTLR_MARKER	index();
	void seek(ANTLR_MARKER seekPoint);

protected:
	void setupIntStream(bool machineBigEndian, bool inputBigEndian);

	/// \brief Return the input element assuming an 8 bit ascii input
	///
	/// \param[in] input Input stream context pointer
	/// \param[in] la 1 based offset of next input stream element
	///
	/// \return Next input character in internal ANTLR3 encoding (UTF32)
	///
	ANTLR_UINT32	LA( ANTLR_INT32 i, ClassForwarder<BYTE_AGNOSTIC> );

	/// \brief Return the input element assuming a UTF16 input when the input is Little Endian and the machine is not
	///
	/// \param[in] input Input stream context pointer
	/// \param[in] la 1 based offset of next input stream element
	///
	/// \return Next input character in internal ANTLR3 encoding (UTF32)
	///
	ANTLR_UINT32	LA( ANTLR_INT32 i, ClassForwarder<ANTLR_LITTLE_ENDIAN> );
	
	/// \brief Return the input element assuming a UTF16 input when the input is Little Endian and the machine is not
	///
	/// \param[in] input Input stream context pointer
	/// \param[in] la 1 based offset of next input stream element
	///
	/// \return Next input character in internal ANTLR3 encoding (UTF32)
	///
	ANTLR_UINT32	LA( ANTLR_INT32 i, ClassForwarder<ANTLR_BIG_ENDIAN> );

	/// \brief Consume the next character in a UTF16 input stream
	///
	/// \param input Input stream context pointer
	///
	void	consume( ClassForwarder<BYTE_AGNOSTIC> );

	/// \brief Consume the next character in a UTF16 input stream when the input is Little Endian and the machine is not
	/// Note that the UTF16 routines do not do any substantial verification of the input stream as for performance
	/// sake, we assume it is validly encoded. So if a low surrogate is found at the curent input position then we
	/// just consume it. Surrogate pairs should be seen as Hi, Lo. So if we have a Lo first, then the input stream
	/// is fubar but we just ignore that.
	///
	/// \param input Input stream context pointer
	///
	void	consume( ClassForwarder<ANTLR_LITTLE_ENDIAN> );

	/// \brief Consume the next character in a UTF16 input stream when the input is Big Endian and the machine is not
	///
	/// \param input Input stream context pointer
	///
	void	consume( ClassForwarder<ANTLR_BIG_ENDIAN> );
};



template<class ImplTraits, class SuperType>
class UTF32_IntStream : public IntStream<ImplTraits, SuperType>
{
public:
	ANTLR_UINT32	LA( ANTLR_INT32 i);
	void		    consume();
	
	/// \brief Calculate the current index in the output stream.
	/// \param[in] input Input stream context pointer
	///
	ANTLR_MARKER	index();
	void seek(ANTLR_MARKER seekPoint);

protected:
	void setupIntStream(bool machineBigEndian, bool inputBigEndian);
	ANTLR_UINT32	LA( ANTLR_INT32 i, ClassForwarder<RESOLVE_ENDIAN_AT_RUNTIME> );
	ANTLR_UINT32	LA( ANTLR_INT32 i, ClassForwarder<BYTE_AGNOSTIC> );
	ANTLR_UINT32	LA( ANTLR_INT32 i, ClassForwarder<ANTLR_LITTLE_ENDIAN> );
	ANTLR_UINT32	LA( ANTLR_INT32 i, ClassForwarder<ANTLR_BIG_ENDIAN> );

	void	consume( ClassForwarder<RESOLVE_ENDIAN_AT_RUNTIME> );
	void	consume( ClassForwarder<BYTE_AGNOSTIC> );
	void	consume( ClassForwarder<ANTLR_LITTLE_ENDIAN> );
	void	consume( ClassForwarder<ANTLR_BIG_ENDIAN> );
};

template<class ImplTraits>
class TokenIntStream : public IntStream<ImplTraits, typename ImplTraits::TokenStreamType >
{
public:
	typedef typename ImplTraits::CommonTokenType CommonTokenType;
	typedef typename ImplTraits::StringType StringType;
	typedef typename ImplTraits::TokenStreamType TokenStreamType;
	typedef IntStream<ImplTraits, TokenStreamType > BaseType;

private:
	/** Because the indirect call, though small in individual cases can
     *  mount up if there are thousands of tokens (very large input streams), callers
     *  of size can optionally use this cached size field.
     */
    ANTLR_UINT32	    m_cachedSize;

public:
	TokenIntStream();
	ANTLR_UINT32 get_cachedSize() const;
	void set_cachedSize( ANTLR_UINT32 cachedSize );

	void consume();
	void  consumeInitialHiddenTokens();
	ANTLR_UINT32  LA( ANTLR_INT32 i );
	ANTLR_MARKER  mark();
	ANTLR_UINT32  size();
	void release();
	ANTLR_MARKER  tindex();
	void rewindLast();
	void rewind(ANTLR_MARKER marker);
	void seek(ANTLR_MARKER index);
	StringType getSourceName();

};

template<class ImplTraits>
class TreeNodeIntStream : public IntStream<ImplTraits, typename ImplTraits::TreeNodeStreamType>
{
public:
	typedef typename ImplTraits::TreeNodeStreamType TreeNodeStreamType;
	typedef IntStream<ImplTraits, TreeNodeStreamType > BaseType;
	typedef typename ImplTraits::TreeType TreeType;
	typedef typename ImplTraits::TreeTypePtr TreeTypePtr;
	typedef typename ImplTraits::CommonTokenType CommonTokenType;

public:
	void				consume();
	ANTLR_MARKER		tindex();
	ANTLR_UINT32		LA(ANTLR_INT32 i);
	ANTLR_MARKER		mark();
	void				release(ANTLR_MARKER marker);
	void				rewindMark(ANTLR_MARKER marker);
	void				rewindLast();
	void				seek(ANTLR_MARKER index);
	ANTLR_UINT32		size();
};

}

#include "antlr3intstream.inl"

#endif

