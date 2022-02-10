namespace antlr3 {

template< class ImplTraits >
Parser<ImplTraits>::Parser( ANTLR_UINT32 sizeHint, RecognizerSharedStateType* state )
	: RecognizerType( sizeHint, state )
{
	m_tstream = NULL;
}

template< class ImplTraits >
Parser<ImplTraits>::Parser( ANTLR_UINT32 sizeHint, TokenStreamType* tstream,
												RecognizerSharedStateType* state )
	: RecognizerType( sizeHint, state )
{
	this->setTokenStream( tstream );
}

template< class ImplTraits >
Parser<ImplTraits>::Parser( ANTLR_UINT32 sizeHint, TokenStreamType* tstream,
											DebugEventListenerType* dbg,
											RecognizerSharedStateType* state )
	: RecognizerType( sizeHint, state )
{
	this->setTokenStream( tstream );
	this->setDebugListener( dbg );
}

template< class ImplTraits >
ANTLR_INLINE typename Parser<ImplTraits>::TokenStreamType* Parser<ImplTraits>::get_tstream() const
{
	return m_tstream;
}

template< class ImplTraits >
ANTLR_INLINE typename Parser<ImplTraits>::IntStreamType* Parser<ImplTraits>::get_istream() const
{
	return m_tstream;
}

template< class ImplTraits >
ANTLR_INLINE typename Parser<ImplTraits>::IntStreamType* Parser<ImplTraits>::get_parser_istream() const
{
	return m_tstream;
}

template< class ImplTraits >
ANTLR_INLINE typename Parser<ImplTraits>::TokenStreamType* Parser<ImplTraits>::get_input() const
{
	return m_tstream;
}

template< class ImplTraits >
void Parser<ImplTraits>::fillExceptionData( ExceptionBaseType* ex )
{
	ex->set_token( new CommonTokenType(*(m_tstream->LT(1))) ); /* Current input token (clonned) - held by the exception */
	ex->set_line( ex->get_token()->get_line() );
	ex->set_charPositionInLine( ex->get_token()->get_charPositionInLine() );
	ex->set_index( this->get_istream()->index() );
	if( ex->get_token()->get_type() == CommonTokenType::TOKEN_EOF)
	{
		ex->set_streamName("");
	}
	else
	{
		ex->set_streamName( ex->get_token()->get_input()->get_fileName() );
	}
	ex->set_message("Unexpected token");
}

template< class ImplTraits >
void Parser<ImplTraits>::displayRecognitionError( ANTLR_UINT8** tokenNames, ExceptionBaseType* ex )
{
	typename ImplTraits::StringStreamType errtext;
	// See if there is a 'filename' we can use
	//
	if( ex->get_streamName().empty() )
	{
		if(ex->get_token()->get_type() == CommonTokenType::TOKEN_EOF)
		{
			errtext << "-end of input-(";
		}
		else
		{
			errtext << "-unknown source-(";
		}
	}
	else
	{
		errtext << ex->get_streamName() << "(";
	}

	// Next comes the line number
	//
	errtext << this->get_rec()->get_state()->get_exception()->get_line() << ") ";
	errtext << " : error " << this->get_rec()->get_state()->get_exception()->getType()
							<< " : "
							<< this->get_rec()->get_state()->get_exception()->get_message();

	// Prepare the knowledge we know we have
	//
	const CommonTokenType* theToken   = this->get_rec()->get_state()->get_exception()->get_token();
	StringType ttext = theToken->toString();

	errtext << ", at offset , "
			<< this->get_rec()->get_state()->get_exception()->get_charPositionInLine();
	if  (theToken != NULL)
	{
		if (theToken->get_type() == CommonTokenType::TOKEN_EOF)
		{
			errtext << ", at <EOF>";
		}
		else
		{
			// Guard against null text in a token
			//
			errtext << "\n    near " << ( ttext.empty()
											? "<no text for the token>" : ttext ) << "\n";
		}
	}

	ex->displayRecognitionError( tokenNames, errtext );
	ImplTraits::displayRecognitionError( errtext.str() );
}

template< class ImplTraits >
Parser<ImplTraits>::~Parser()
{
    if	(this->get_rec() != NULL)
    {
		// This may have ben a delegate or delegator parser, in which case the
		// state may already have been freed (and set to NULL therefore)
		// so we ignore the state if we don't have it.
		//
		RecognizerSharedStateType* state = this->get_rec()->get_state();
		if	(state != NULL)
		{
			state->get_following().clear();
		}
    }
}

template< class ImplTraits >
void	Parser<ImplTraits>::setDebugListener(DebugEventListenerType* dbg)
{
		// Set the debug listener. There are no methods to override
	// because currently the only ones that notify the debugger
	// are error reporting and recovery. Hence we can afford to
	// check and see if the debugger interface is null or not
	// there. If there is ever an occasion for a performance
	// sensitive function to use the debugger interface, then
	// a replacement function for debug mode should be supplied
	// and installed here.
	//
	this->get_rec()->set_debugger(dbg);

	// If there was a tokenstream installed already
	// then we need to tell it about the debug interface
	//
	if	(this->get_tstream() != NULL)
	{
		this->get_tstream()->setDebugListener(dbg);
	}
}

template< class ImplTraits >
ANTLR_INLINE void	Parser<ImplTraits>::setTokenStream(TokenStreamType* tstream)
{
	m_tstream = tstream;
    this->get_rec()->reset();
}

template< class ImplTraits >
ANTLR_INLINE typename Parser<ImplTraits>::TokenStreamType*	Parser<ImplTraits>::getTokenStream()
{
	return m_tstream;
}

template< class ImplTraits >
ANTLR_INLINE typename Parser<ImplTraits>::RecognizerType* Parser<ImplTraits>::get_rec()
{
	return this;
}

template< class ImplTraits >
ANTLR_INLINE void Parser<ImplTraits>::exConstruct()
{
	new ANTLR_Exception<ImplTraits, MISMATCHED_TOKEN_EXCEPTION, StreamType>( this->get_rec(), "" );
}

template< class ImplTraits >
typename Parser<ImplTraits>::TokenType*	Parser<ImplTraits>::getMissingSymbol( IntStreamType* istream,
										  ExceptionBaseType*,
										  ANTLR_UINT32			expectedTokenType,
										  BitsetListType*	)
{
	// Dereference the standard pointers
	//
	TokenStreamType *cts = static_cast<TokenStreamType*>(istream);

	// Work out what to use as the current symbol to make a line and offset etc
	// If we are at EOF, we use the token before EOF
	//
	const CommonTokenType* current = cts->LT(1);
	if	(current->get_type() == CommonTokenType::TOKEN_EOF)
	{
		current = cts->LT(-1);
	}

	CommonTokenType* token = new CommonTokenType;

	// Set some of the token properties based on the current token
	//
	token->set_line(current->get_line());
	token->set_charPositionInLine( current->get_charPositionInLine());
	token->set_channel( TOKEN_DEFAULT_CHANNEL );
	token->set_type(expectedTokenType);
	token->set_lineStart( current->get_lineStart() );
	
	// Create the token text that shows it has been inserted
	//
	if ( expectedTokenType == CommonTokenType::TOKEN_EOF )
	{
		token->setText( "<missing EOF>" );
	} else {
		typename ImplTraits::StringStreamType text;
		text << "<missing " << this->get_rec()->get_state()->get_tokenName(expectedTokenType) << ">";
		token->setText( text.str().c_str() );
	}
	// Finally return the pointer to our new token
	//
	return	token;
}

template< class ImplTraits >
void Parser<ImplTraits>::mismatch(ANTLR_UINT32 ttype, BitsetListType* follow)
{
    // Install a mismatched token exception in the exception stack
    //
	new ANTLR_Exception<ImplTraits, MISMATCHED_TOKEN_EXCEPTION, StreamType>(this, "");

	//With the statement below, only the parsers are allowed to compile fine
	IntStreamType* is = this->get_istream();


	if	(this->mismatchIsUnwantedToken(is, ttype))
	{
		// Now update it to indicate this is an unwanted token exception
		//
		new ANTLR_Exception<ImplTraits, UNWANTED_TOKEN_EXCEPTION, StreamType>(this, "");
		return;
	}

	if	( this->mismatchIsMissingToken(is, follow))
	{
		// Now update it to indicate this is an unwanted token exception
		//
		new ANTLR_Exception<ImplTraits, MISSING_TOKEN_EXCEPTION, StreamType>(this, "");
		return;
	}

	// Just a mismatched token is all we can dtermine
	//
	new ANTLR_Exception<ImplTraits, MISMATCHED_TOKEN_EXCEPTION, StreamType>(this, "");

	return;
}

template< class ImplTraits>
ANTLR_INLINE const typename Parser<ImplTraits>::RecognizerType* Parser<ImplTraits>::get_recognizer() const
{
	return this;
}

template< class ImplTraits>
ANTLR_INLINE typename Parser<ImplTraits>::RecognizerSharedStateType* Parser<ImplTraits>::get_psrstate() const
{
	return this->get_recognizer()->get_state();
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::set_psrstate(RecognizerSharedStateType* state)
{
	this->get_rec()->set_state( state );
}

template< class ImplTraits>
ANTLR_INLINE bool Parser<ImplTraits>::haveParsedRule(ANTLR_MARKER	ruleIndex)
{
	return this->get_rec()->alreadyParsedRule(ruleIndex);
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::memoize(ANTLR_MARKER	ruleIndex, ANTLR_MARKER	ruleParseStart)
{
	return this->get_rec()->memoize( ruleIndex, ruleParseStart );
}

template< class ImplTraits>
ANTLR_INLINE ANTLR_MARKER  Parser<ImplTraits>::index() const
{
	return this->get_istream()->index();
}

template< class ImplTraits>
ANTLR_INLINE bool Parser<ImplTraits>::hasException() const
{
	return this->get_psrstate()->get_error();
}

template< class ImplTraits>
ANTLR_INLINE typename Parser<ImplTraits>::ExceptionBaseType* Parser<ImplTraits>::get_exception() const
{
	return this->get_psrstate()->get_exception();
}

template< class ImplTraits>
ANTLR_INLINE const typename Parser<ImplTraits>::CommonTokenType* Parser<ImplTraits>::matchToken( ANTLR_UINT32 ttype, BitsetListType* follow )
{
	return this->get_rec()->match( ttype, follow );
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::matchAnyToken()
{
	return this->get_rec()->matchAny();
}

template< class ImplTraits>
ANTLR_INLINE const typename Parser<ImplTraits>::FollowingType& Parser<ImplTraits>::get_follow_stack() const
{
	return this->get_psrstate()->get_following();
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::followPush(const BitsetListType& follow)
{
#ifndef  SKIP_FOLLOW_SETS
	this->get_rec()->get_state()->get_following().push(follow);
#endif
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::followPop()
{
#ifndef  SKIP_FOLLOW_SETS
	this->get_rec()->get_state()->get_following().pop();
#endif
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::precover()
{
	return this->get_rec()->recover();
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::preporterror()
{
	return this->get_rec()->reportError();
}

template< class ImplTraits>
ANTLR_INLINE ANTLR_UINT32 Parser<ImplTraits>::LA(ANTLR_INT32 i)
{
	return this->get_istream()->LA(i);
}

template< class ImplTraits>
ANTLR_INLINE const typename Parser<ImplTraits>::CommonTokenType*  Parser<ImplTraits>::LT(ANTLR_INT32 k)
{
	return this->get_input()->LT(k);
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::constructEx()
{
	this->get_rec()->constructEx();
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::consume()
{
	this->get_istream()->consume();
}

template< class ImplTraits>
ANTLR_INLINE ANTLR_MARKER Parser<ImplTraits>::mark()
{
	return this->get_istream()->mark();
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::rewind(ANTLR_MARKER marker)
{
	this->get_istream()->rewind(marker);
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::rewindLast()
{
	this->get_istream()->rewindLast();
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::seek(ANTLR_MARKER index)
{
	this->get_istream()->seek(index);
}

template< class ImplTraits>
ANTLR_INLINE bool Parser<ImplTraits>::get_perror_recovery() const
{
	return this->get_psrstate()->get_errorRecovery();
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::set_perror_recovery( bool val )
{
	this->get_psrstate()->set_errorRecovery(val);
}

template< class ImplTraits>
ANTLR_INLINE bool Parser<ImplTraits>::hasFailed() const
{
	return this->get_psrstate()->get_failed();
}

template< class ImplTraits>
ANTLR_INLINE bool Parser<ImplTraits>::get_failedflag() const
{
	return this->get_psrstate()->get_failed();
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::set_failedflag( bool failed )
{
	this->get_psrstate()->set_failed(failed);
}

template< class ImplTraits>
ANTLR_INLINE ANTLR_INT32 Parser<ImplTraits>::get_backtracking() const
{
	return this->get_psrstate()->get_backtracking();
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::inc_backtracking()
{
	this->get_psrstate()->inc_backtracking();
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::dec_backtracking()
{
	this->get_psrstate()->dec_backtracking();
}

template< class ImplTraits>
ANTLR_INLINE typename Parser<ImplTraits>::CommonTokenType* Parser<ImplTraits>::recoverFromMismatchedSet(BitsetListType*	follow)
{
	return this->get_rec()->recoverFromMismatchedSet(follow);
}

template< class ImplTraits>
ANTLR_INLINE bool	Parser<ImplTraits>::recoverFromMismatchedElement(BitsetListType*	follow)
{
	return this->get_rec()->recoverFromMismatchedElement(follow);
}

template< class ImplTraits>
ANTLR_INLINE typename Parser<ImplTraits>::RuleMemoType* Parser<ImplTraits>::getRuleMemo() const
{
	return this->get_psrstate()->get_ruleMemo();
}

template< class ImplTraits>
ANTLR_INLINE void Parser<ImplTraits>::setRuleMemo(RuleMemoType* rulememo)
{
	this->get_psrstate()->set_ruleMemo(rulememo);
}

template< class ImplTraits>
ANTLR_INLINE typename Parser<ImplTraits>::DebuggerType* Parser<ImplTraits>::get_debugger() const
{
	return this->get_rec()->get_debugger();
}

template< class ImplTraits>
ANTLR_INLINE typename Parser<ImplTraits>::TokenStreamType* Parser<ImplTraits>::get_strstream() const
{
	return this->get_tstream();
}

template< class ImplTraits>
ANTLR_INLINE RuleReturnValue<ImplTraits>::RuleReturnValue(BaseParserType* /*psr*/) 
{
	start = NULL;
	stop = NULL;
}

template< class ImplTraits>
ANTLR_INLINE RuleReturnValue<ImplTraits>::RuleReturnValue( const RuleReturnValue& val )
{
	start	= val.start;
	stop	= val.stop;
}

template< class ImplTraits>
ANTLR_INLINE RuleReturnValue<ImplTraits>& RuleReturnValue<ImplTraits>::operator=( const RuleReturnValue& val )
{
	start	= val.start;
	stop	= val.stop;
	return *this;
}

template< class ImplTraits>
ANTLR_INLINE RuleReturnValue<ImplTraits>::~RuleReturnValue()
{
}

template< class ImplTraits>
ANTLR_INLINE void RuleReturnValue<ImplTraits>::call_start_placeholder(BaseParserType *parser)
{
	start = parser->LT(1); 
	stop = start;
}

template< class ImplTraits>
ANTLR_INLINE void RuleReturnValue<ImplTraits>::call_stop_placeholder(BaseParserType *parser)
{
	stop = parser->LT(-1);
}

template< class ImplTraits>
ANTLR_INLINE RuleReturnValue_1<ImplTraits>::RuleReturnValue_1()
	: parser()
{
}

template< class ImplTraits>
RuleReturnValue_1<ImplTraits>::RuleReturnValue_1( BaseParserType* psr )
	: RuleReturnValue_1<ImplTraits>::BaseType(psr)
	, parser(psr)
{
	BaseType::start = psr->LT(1);
	BaseType::stop = BaseType::start;
}

template< class ImplTraits>
RuleReturnValue_1<ImplTraits>::RuleReturnValue_1( const RuleReturnValue_1& val )
	: RuleReturnValue_1<ImplTraits>::BaseType(val)
	, parser(val.parser)
{
}

template< class ImplTraits>
void RuleReturnValue_1<ImplTraits>::call_start_placeholder(BaseParserType*)
{
}

template< class ImplTraits>
RuleReturnValue_1<ImplTraits>::~RuleReturnValue_1()
{
	if( parser && parser->get_backtracking() == 0 )
	{
		if( BaseType::stop == NULL )
			BaseType::stop = BaseType::parser->LT(-1);
		if( BaseType::stop != NULL )
		{
			ANTLR_MARKER start_token_idx	= BaseType::start->get_index() + 1;
			ANTLR_MARKER stop_token_idx		= BaseType::stop->get_index() - 1;
			if( start_token_idx > stop_token_idx )
				return;
			parser->getTokenStream()->discardTokens( start_token_idx, stop_token_idx);
		}
	}
}

}
