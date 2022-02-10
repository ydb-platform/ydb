namespace antlr3 {

template<class ImplTraits>
Lexer<ImplTraits>::Lexer(ANTLR_UINT32 sizeHint, RecognizerSharedStateType* state)
	:Lexer<ImplTraits>::RecognizerType(sizeHint, state)
	,m_input(NULL)
{
}

template<class ImplTraits>
Lexer<ImplTraits>::Lexer(ANTLR_UINT32 sizeHint, InputStreamType* input, RecognizerSharedStateType* state)
	:Lexer<ImplTraits>::RecognizerType(sizeHint, state)
{
	this->setCharStream(input);
}

template<class ImplTraits>
typename Lexer<ImplTraits>::InputStreamType* Lexer<ImplTraits>::get_input() const
{
	return m_input;
}

template<class ImplTraits>
typename Lexer<ImplTraits>::IntStreamType* Lexer<ImplTraits>::get_istream() const
{
	return m_input;
}

template<class ImplTraits>
typename Lexer<ImplTraits>::RecognizerType* Lexer<ImplTraits>::get_rec()
{
	return this;
}

template<class ImplTraits>
typename Lexer<ImplTraits>::TokenSourceType* Lexer<ImplTraits>::get_tokSource()
{
	return this;
}

template<class ImplTraits>
void Lexer<ImplTraits>::displayRecognitionError( ANTLR_UINT8** , ExceptionBaseType* ex)
{
	StringStreamType	err_stream;

	// See if there is a 'filename' we can use
    //
    if( ex->getName().empty() )
    {
		err_stream << "-unknown source-(";
    }
    else
    {
		err_stream << ex->get_streamName().c_str();
		err_stream << "(";
    }
    err_stream << ex->get_line() << ")";

	err_stream << ": lexer error " <<  ex->getName() << '(' << ex->getType() << ')' << " :\n\t"
		   << ex->get_message() << " at position [" << ex->get_line() << ", "
		   << ex->get_charPositionInLine()+1 << "], ";

	{
		ANTLR_UINT32	width;

		width	= ANTLR_UINT32_CAST(( (ANTLR_UINT8*)(m_input->get_data()) +
									  (m_input->size() )) - (ANTLR_UINT8*)( ex->get_index() ));

		if	(width >= 1)
		{
			if	(isprint(ex->get_c() ))
			{
				err_stream << "near '" << (typename StringType::value_type) ex->get_c() << "' :\n";
			}
			else
			{
				err_stream << "near char(" << std::hex << ex->get_c() << std::dec << ") :\n";
			}
			err_stream << "\t";
			err_stream.width( width > 20 ? 20 : width );
			err_stream << (typename StringType::const_pointer)ex->get_index() << "\n";
		}
		else
		{
			err_stream << "(end of input).\n\t This indicates a poorly specified lexer RULE\n\t or unterminated input element such as: \"STRING[\"]\n";
			err_stream << "\t The lexer was matching from line "
					   << this->get_state()->get_tokenStartLine()
					   << ", offset " << this->get_state()->get_tokenStartCharPositionInLine()
					   << ", which\n\t ";
			width = ANTLR_UINT32_CAST(((ANTLR_UINT8*)(m_input->get_data() )+
										(m_input->size())) -
										(ANTLR_UINT8*)(this->get_state()->get_tokenStartCharIndex() ));

			if	(width >= 1)
			{
				err_stream << "looks like this:\n\t\t";
				err_stream.width( width > 20 ? 20 : width );
				err_stream << (typename StringType::const_pointer)this->get_state()->get_tokenStartCharIndex() << "\n";
			}
			else
			{
				err_stream << "is also the end of the line, so you must check your lexer rules\n";
			}
		}
	}
	ImplTraits::displayRecognitionError( err_stream.str() );
}

template<class ImplTraits>
void Lexer<ImplTraits>::fillExceptionData( ExceptionBaseType* ex )
{
	ex->set_c( m_input->LA(1) );					/* Current input character			*/
	ex->set_line( m_input->get_line() );						/* Line number comes from stream		*/
	ex->set_charPositionInLine( m_input->get_charPositionInLine() );	    /* Line offset also comes from the stream   */
	ex->set_index( m_input->index() );
	ex->set_streamName( m_input->get_fileName() );
	ex->set_message( "Unexpected character" );
}

template<class ImplTraits>
void	Lexer<ImplTraits>::setCharStream(InputStreamType* input)
{
    /* Install the input interface
     */
    m_input	= input;

    /* Set the current token to nothing
     */
	RecognizerSharedStateType* state = this->get_rec()->get_state();
    state->set_token_present( false );
	state->set_text("");
    state->set_tokenStartCharIndex(-1);

    /* Copy the name of the char stream to the token source
     */
    this->get_tokSource()->set_fileName( input->get_fileName() );
}

template<class ImplTraits>
void	Lexer<ImplTraits>::pushCharStream(InputStreamType* input)
{
	// We have a stack, so we can save the current input stream
	// into it.
	//
	this->get_istream()->mark();
	this->get_rec()->get_state()->get_streams().push(this->get_input());

	// And now we can install this new one
	//
	this->setCharStream(input);
}

template<class ImplTraits>
void	Lexer<ImplTraits>::popCharStream()
{
	InputStreamType* input;

    // If we do not have a stream stack or we are already at the
    // stack bottom, then do nothing.
    //
    typename RecognizerSharedStateType::StreamsType& streams = this->get_rec()->get_state()->get_streams();
    if	( streams.size() > 0)
    {
		// We just leave the current stream to its fate, we do not close
		// it or anything as we do not know what the programmer intended
		// for it. This method can always be overridden of course.
		// So just find out what was currently saved on the stack and use
		// that now, then pop it from the stack.
		//
		input	= streams.top();
		streams.pop();

		// Now install the stream as the current one.
		//
		this->setCharStream(input);
		this->get_istream()->rewindLast();
    }
    return;
}

template<class ImplTraits>
void	Lexer<ImplTraits>::emit(const CommonTokenType* token)
{
	this->get_rec()->get_state()->set_token(token);
}

template<class ImplTraits>
typename Lexer<ImplTraits>::CommonTokenType*	Lexer<ImplTraits>::emit()
{
	/* We could check pointers to token factories and so on, but
    * we are in code that we want to run as fast as possible
    * so we are not checking any errors. So make sure you have installed an input stream before
    * trying to emit a new token.
    */
	RecognizerSharedStateType* state = this->get_rec()->get_state();
	state->set_token_present(true);
    CommonTokenType* token = state->get_token();
	token->set_input( this->get_input() );

    /* Install the supplied information, and some other bits we already know
    * get added automatically, such as the input stream it is associated with
    * (though it can all be overridden of course)
    */
    token->set_type( state->get_type() );
    token->set_channel( state->get_channel() );
    token->set_startIndex( state->get_tokenStartCharIndex() );
    token->set_stopIndex( this->getCharIndex() - 1 );
    token->set_line( state->get_tokenStartLine() );
    token->set_charPositionInLine( state->get_tokenStartCharPositionInLine() );

	token->set_tokText( state->get_text() );
    token->set_lineStart( this->get_input()->get_currentLine() );

    return  token;
}

template<class ImplTraits>
Lexer<ImplTraits>::~Lexer()
{
	// This may have ben a delegate or delegator lexer, in which case the
	// state may already have been freed (and set to NULL therefore)
	// so we ignore the state if we don't have it.
	//
	RecognizerSharedStateType* state = this->get_rec()->get_state();

	if	( state != NULL)
	{
		state->get_streams().clear();
	}
}

template<class ImplTraits>
bool	Lexer<ImplTraits>::matchs(ANTLR_UCHAR* str )
{
	RecognizerSharedStateType* state = this->get_rec()->get_state();
	while   (*str != ANTLR_STRING_TERMINATOR)
	{
		if  ( this->get_istream()->LA(1) != (*str))
		{
			if	( state->get_backtracking() > 0)
			{
				state->set_failed(true);
				return false;
			}

			this->exConstruct();
			state->set_failed( true );

			/* TODO: Implement exception creation more fully perhaps
			 */
			this->recover();
			return  false;
		}

		/* Matched correctly, do consume it
		 */
		this->get_istream()->consume();
		str++;

	}
	/* Reset any failed indicator
	 */
	state->set_failed( false );
	return  true;
}

template<class ImplTraits>
bool	Lexer<ImplTraits>::matchc(ANTLR_UCHAR c)
{
	if	(this->get_istream()->LA(1) == c)
	{
		/* Matched correctly, do consume it
		 */
		this->get_istream()->consume();

		/* Reset any failed indicator
		 */
		this->get_rec()->get_state()->set_failed( false );

		return	true;
	}

	/* Failed to match, exception and recovery time.
	 */
	if(this->get_rec()->get_state()->get_backtracking() > 0)
	{
		this->get_rec()->get_state()->set_failed( true );
		return	false;
	}

	this->exConstruct();

	/* TODO: Implement exception creation more fully perhaps
	 */
	this->recover();

	return  false;
}

template<class ImplTraits>
bool	Lexer<ImplTraits>::matchRange(ANTLR_UCHAR low, ANTLR_UCHAR high)
{
    ANTLR_UCHAR    c;

    /* What is in the stream at the moment?
     */
    c	= this->get_istream()->LA(1);
    if	( c >= low && c <= high)
    {
		/* Matched correctly, consume it
		 */
		this->get_istream()->consume();

		/* Reset any failed indicator
		 */
		this->get_rec()->get_state()->set_failed( false );

		return	true;
    }

    /* Failed to match, execption and recovery time.
     */

    if	(this->get_rec()->get_state()->get_backtracking() > 0)
    {
		this->get_rec()->get_state()->set_failed( true );
		return	false;
    }

    this->exConstruct();

    /* TODO: Implement exception creation more fully
     */
    this->recover();

    return  false;
}

template<class ImplTraits>
void		Lexer<ImplTraits>::matchAny()
{
	this->get_istream()->consume();
}

template<class ImplTraits>
void		Lexer<ImplTraits>::recover()
{
	this->get_istream()->consume();
}

template<class ImplTraits>
ANTLR_UINT32	Lexer<ImplTraits>::getLine()
{
	return  this->get_input()->get_line();
}

template<class ImplTraits>
ANTLR_MARKER	Lexer<ImplTraits>::getCharIndex()
{
	return this->get_istream()->index();
}

template<class ImplTraits>
ANTLR_UINT32	Lexer<ImplTraits>::getCharPositionInLine()
{
	return  this->get_input()->get_charPositionInLine();
}

template<class ImplTraits>
typename Lexer<ImplTraits>::StringType	Lexer<ImplTraits>::getText()
{
	RecognizerSharedStateType* state = this->get_rec()->get_state();
	if ( !state->get_text().empty() )
	{
		return	state->get_text();

	}
	return  this->get_input()->substr( state->get_tokenStartCharIndex(),
									this->getCharIndex() - this->get_input()->get_charByteSize()
							);
}

template<class ImplTraits>
void Lexer<ImplTraits>::exConstruct()
{
	new ANTLR_Exception<ImplTraits, RECOGNITION_EXCEPTION, InputStreamType>( this->get_rec(), "" );
}

template< class ImplTraits>
typename Lexer<ImplTraits>::TokenType*	Lexer<ImplTraits>::getMissingSymbol( IntStreamType*,
										  ExceptionBaseType*,
										  ANTLR_UINT32	, BitsetListType*)
{
	return NULL;
}

template< class ImplTraits>
ANTLR_INLINE const typename Lexer<ImplTraits>::RecognizerType* Lexer<ImplTraits>::get_rec() const
{
	return this;
}

template< class ImplTraits>
ANTLR_INLINE const typename Lexer<ImplTraits>::RecognizerType* Lexer<ImplTraits>::get_recognizer() const
{
	return this->get_rec();
}

template< class ImplTraits>
ANTLR_INLINE typename Lexer<ImplTraits>::RecognizerSharedStateType* Lexer<ImplTraits>::get_lexstate() const
{
	return this->get_rec()->get_state();
}

template< class ImplTraits>
ANTLR_INLINE void Lexer<ImplTraits>::set_lexstate( RecognizerSharedStateType* lexstate )
{
	this->get_rec()->set_state(lexstate);
}

template< class ImplTraits>
ANTLR_INLINE const typename Lexer<ImplTraits>::TokenSourceType* Lexer<ImplTraits>::get_tokSource() const
{
	return this;
}

template< class ImplTraits>
ANTLR_INLINE typename Lexer<ImplTraits>::CommonTokenType* Lexer<ImplTraits>::get_ltoken() const
{
	return this->get_lexstate()->token();
}

template< class ImplTraits>
ANTLR_INLINE void Lexer<ImplTraits>::set_ltoken( const CommonTokenType* ltoken )
{
	this->get_lexstate()->set_token( ltoken );
}

template< class ImplTraits>
ANTLR_INLINE bool Lexer<ImplTraits>::hasFailed() const
{
	return this->get_lexstate()->get_failed();
}

template< class ImplTraits>
ANTLR_INLINE ANTLR_INT32 Lexer<ImplTraits>::get_backtracking() const
{
	return this->get_lexstate()->get_backtracking();
}

template< class ImplTraits>
ANTLR_INLINE void Lexer<ImplTraits>::inc_backtracking()
{
	this->get_lexstate()->inc_backtracking();
}

template< class ImplTraits>
ANTLR_INLINE void Lexer<ImplTraits>::dec_backtracking()
{
	this->get_lexstate()->dec_backtracking();
}

template< class ImplTraits>
ANTLR_INLINE bool Lexer<ImplTraits>::get_failedflag() const
{
	return this->get_lexstate()->get_failed();
}

template< class ImplTraits>
ANTLR_INLINE void Lexer<ImplTraits>::set_failedflag( bool failed )
{
	this->get_lexstate()->set_failed(failed);
}

template< class ImplTraits>
ANTLR_INLINE typename Lexer<ImplTraits>::InputStreamType* Lexer<ImplTraits>::get_strstream() const
{
	return this->get_input();
}

template< class ImplTraits>
ANTLR_INLINE ANTLR_MARKER  Lexer<ImplTraits>::index() const
{
	return this->get_istream()->index();
}

template< class ImplTraits>
ANTLR_INLINE void	Lexer<ImplTraits>::seek(ANTLR_MARKER index)
{
	this->get_istream()->seek(index);
}

template< class ImplTraits>
ANTLR_INLINE const typename Lexer<ImplTraits>::CommonTokenType* Lexer<ImplTraits>::EOF_Token() const
{
	const CommonTokenType& eof_token = this->get_tokSource()->get_eofToken();
	return &eof_token;
}

template< class ImplTraits>
ANTLR_INLINE bool Lexer<ImplTraits>::hasException() const
{
	return this->get_lexstate()->get_error();
}

template< class ImplTraits>
ANTLR_INLINE typename Lexer<ImplTraits>::ExceptionBaseType* Lexer<ImplTraits>::get_exception() const
{
	return this->get_lexstate()->get_exception();
}

template< class ImplTraits>
ANTLR_INLINE void Lexer<ImplTraits>::constructEx()
{
	this->get_rec()->exConstruct();
}

template< class ImplTraits>
ANTLR_INLINE ANTLR_MARKER Lexer<ImplTraits>::mark()
{
	return this->get_istream()->mark();
}

template< class ImplTraits>
ANTLR_INLINE void Lexer<ImplTraits>::rewind(ANTLR_MARKER marker)
{
	this->get_istream()->rewind(marker);
}

template< class ImplTraits>
ANTLR_INLINE void Lexer<ImplTraits>::rewindLast()
{
	this->get_istream()->rewindLast();
}

template< class ImplTraits>
ANTLR_INLINE void Lexer<ImplTraits>::memoize(ANTLR_MARKER	ruleIndex, ANTLR_MARKER	ruleParseStart)
{
	this->get_rec()->memoize( ruleIndex, ruleParseStart );
}

template< class ImplTraits>
ANTLR_INLINE bool Lexer<ImplTraits>::haveParsedRule(ANTLR_MARKER	ruleIndex)
{
	return this->get_rec()->alreadyParsedRule(ruleIndex);
}

template< class ImplTraits>
ANTLR_INLINE void Lexer<ImplTraits>::setText( const StringType& text )
{
	this->get_lexstate()->set_text(text);
}

template< class ImplTraits>
ANTLR_INLINE void Lexer<ImplTraits>::skip()
{
	CommonTokenType& skipToken = this->get_tokSource()->get_skipToken();
	this->get_lexstate()->set_token( &skipToken );
}

template< class ImplTraits>
ANTLR_INLINE typename Lexer<ImplTraits>::RuleMemoType* Lexer<ImplTraits>::getRuleMemo() const
{
	return this->get_lexstate()->get_rulememo();
}

template< class ImplTraits>
ANTLR_INLINE void Lexer<ImplTraits>::setRuleMemo(RuleMemoType* rulememo)
{
	return this->get_lexstate()->set_rulememo(rulememo);
}

template< class ImplTraits>
ANTLR_INLINE typename Lexer<ImplTraits>::DebuggerType* Lexer<ImplTraits>::get_debugger() const
{
	return this->get_rec()->get_debugger();
}

template< class ImplTraits>
ANTLR_INLINE ANTLR_UINT32 Lexer<ImplTraits>::LA(ANTLR_INT32 i)
{
	return this->get_istream()->LA(i);
}

template< class ImplTraits>
ANTLR_INLINE void Lexer<ImplTraits>::consume()
{
	return this->get_istream()->consume();
}

}

