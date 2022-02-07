/** \file
 *  Contains the definition of a basic ANTLR3 exception structure created
 *  by a recognizer when errors are found/predicted.

 * Two things to be noted for C++ Target:
   a) This is not the C++ Exception. Consider this just as yet another class. This
   has to be like this because there is a inbuilt recovery and hence there is a try..catch
   block for every new token. This is not how C++ Exceptions work.Still there is exception support, as we are handling things like OutofMemory by
   throwing exceptions

   b) There is no use in implementing templates here, as all the exceptions are grouped in
   one container and hence needs virtual functions. But this would occur only when there is
   a exception/ while deleting base recognizer. So shouldn't incur the overhead in normal operation
 */
#ifndef	_ANTLR3_EXCEPTION_HPP
#define	_ANTLR3_EXCEPTION_HPP

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

/** Base structure for an ANTLR3 exception tracker
 */

template<class ImplTraits, class StreamType>
class ANTLR_ExceptionBase
{
public:
	typedef typename StreamType::UnitType TokenType;
	typedef typename StreamType::IntStreamType IntStreamType;
	typedef typename ImplTraits::AllocPolicyType AllocPolicyType;
	typedef typename ImplTraits::StringType StringType;
	typedef typename ImplTraits::StringStreamType StringStreamType;
	typedef typename ImplTraits::BitsetType BitsetType;
	typedef typename ImplTraits::BitsetListType BitsetListType;
	typedef typename ImplTraits::template ExceptionBaseType<StreamType> ExceptionBaseType;

protected:
    /** The printable message that goes with this exception, in your preferred
     *  encoding format. ANTLR just uses ASCII by default but you can ignore these
     *  messages or convert them to another format or whatever of course. They are
     *  really internal messages that you then decide how to print out in a form that
     *  the users of your product will understand, as they are unlikely to know what
     *  to do with "Recognition exception at: [[TOK_GERUND..... " ;-)
     */
    StringType		m_message;

    /** Name of the file/input source for reporting. Note that this may be empty!!
     */
    StringType		m_streamName;

    /** Indicates the index of the 'token' we were looking at when the
     *  exception occurred.
     */
    ANTLR_MARKER	m_index;

    /** Indicates what the current token/tree was when the error occurred. Since not
     *  all input streams will be able to retrieve the nth token, we track it here
     *  instead. This is for parsers, and even tree parsers may set this.
     */
    const TokenType*		m_token;

    /** Pointer to the next exception in the chain (if any)
     */
    ExceptionBaseType*  m_nextException;

    /** Indicates the token we were expecting to see next when the error occurred
     */
    ANTLR_UINT32	m_expecting;

    /** Indicates a set of tokens that we were expecting to see one of when the
     *  error occurred. It is a following bitset list, so you can use load it and use ->toIntList() on it
     *  to generate an array of integer tokens that it represents.
     */
    BitsetListType*	m_expectingSet;

    /** If this is a tree parser exception then the node is set to point to the node
     * that caused the issue.
     */
    TokenType*		m_node;

    /** The current character when an error occurred - for lexers.
     */
    ANTLR_UCHAR		m_c;

    /** Track the line at which the error occurred in case this is
     *  generated from a lexer.  We need to track this since the
     *  unexpected char doesn't carry the line info.
     */
    ANTLR_UINT32   	m_line;

    /** Character position in the line where the error occurred.
     */
    ANTLR_INT32   	m_charPositionInLine;

    /** decision number for NVE
     */
    ANTLR_UINT32   	m_decisionNum;

    /** State for NVE
     */
    ANTLR_UINT32	m_state;

    /** Rule name for failed predicate exception
     */
    StringType		m_ruleName;

    /** Pointer to the input stream that this exception occurred in.
     */
    IntStreamType* 	m_input;

public:
	StringType& get_message();
	StringType& get_streamName();
	ANTLR_MARKER get_index() const;
	const TokenType* get_token() const;
	ExceptionBaseType* get_nextException() const;
	ANTLR_UINT32 get_expecting() const;
	BitsetListType* get_expectingSet() const;
	TokenType* get_node() const;
	ANTLR_UCHAR get_c() const;
	ANTLR_UINT32 get_line() const;
	ANTLR_INT32 get_charPositionInLine() const;
	ANTLR_UINT32 get_decisionNum() const;
	ANTLR_UINT32 get_state() const;
	StringType& get_ruleName();
	IntStreamType* get_input() const;
	void  set_message( const StringType& message );
	void  set_streamName( const StringType& streamName );
	void  set_index( ANTLR_MARKER index );
	void  set_token( const TokenType* token );
	void  set_nextException( ExceptionBaseType* nextException );
	void  set_expecting( ANTLR_UINT32 expecting );
	void  set_expectingSet( BitsetListType* expectingSet );
	void  set_node( TokenType* node );
	void  set_c( ANTLR_UCHAR c );
	void  set_line( ANTLR_UINT32 line );
	void  set_charPositionInLine( ANTLR_INT32 charPositionInLine );
	void  set_decisionNum( ANTLR_UINT32 decisionNum );
	void  set_state( ANTLR_UINT32 state );
	void  set_ruleName( const StringType& ruleName );
	void  set_input( IntStreamType* input );
	StringType getDescription() const;
	
	virtual StringType getName() const = 0;
	virtual ANTLR_UINT32 getType() const = 0;
	virtual void print() const = 0;
	virtual void displayRecognitionError( ANTLR_UINT8** tokenNames, StringStreamType& str ) const = 0;

    virtual ~ANTLR_ExceptionBase();

protected:
	ANTLR_ExceptionBase(const StringType& message);
};


template<class ImplTraits, ExceptionType Ex, class StreamType>
class ANTLR_Exception  :  public ImplTraits::template ExceptionBaseType<StreamType>
{
public:
	typedef typename ImplTraits::StringType StringType;
	typedef typename ImplTraits::StringStreamType StringStreamType;
	typedef typename ImplTraits::BitsetType BitsetType;
	typedef typename ImplTraits::template ExceptionBaseType<StreamType> BaseType;

public:
	template<typename BaseRecognizerType>
	ANTLR_Exception(BaseRecognizerType* recognizer, const StringType& message);

	const StringType& get_name() const;
	virtual StringType getName() const;
	virtual ANTLR_UINT32 getType() const;
	virtual void print() const;
	virtual void displayRecognitionError( ANTLR_UINT8** tokenNames, StringStreamType& str_stream) const;
};

}

#include "antlr3exception.inl"

#endif
