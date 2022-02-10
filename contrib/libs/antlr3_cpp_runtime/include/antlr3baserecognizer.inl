namespace antlr3 {

template< class ImplTraits, class StreamType >
BaseRecognizer<ImplTraits, StreamType>::BaseRecognizer(ANTLR_UINT32 sizeHint,
											RecognizerSharedStateType* state)
{
	m_debugger = NULL;

	// If we have been supplied with a pre-existing recognizer state
	// then we just install it, otherwise we must create one from scratch
	//
	if	(state == NULL)
	{
		m_state = new RecognizerSharedStateType();
		m_state->set_sizeHint( sizeHint );
	}
	else
	{
		// Install the one we were given, and do not reset it here
		// as it will either already have been initialized or will
		// be in a state that needs to be preserved.
		//
		m_state = state;
	}
}

template< class ImplTraits, class StreamType >
ANTLR_INLINE typename BaseRecognizer<ImplTraits, StreamType>::SuperType* BaseRecognizer<ImplTraits, StreamType>::get_super()
{
	return static_cast<SuperType*>(this);
}

template< class ImplTraits, class StreamType >
ANTLR_INLINE typename BaseRecognizer<ImplTraits, StreamType>::RecognizerSharedStateType* BaseRecognizer<ImplTraits, StreamType>::get_state() const
{
	return m_state;
}
template< class ImplTraits, class StreamType >
ANTLR_INLINE typename BaseRecognizer<ImplTraits, StreamType>::DebugEventListenerType* BaseRecognizer<ImplTraits, StreamType>::get_debugger() const
{
	return m_debugger;
}
template< class ImplTraits, class StreamType >
ANTLR_INLINE void BaseRecognizer<ImplTraits, StreamType>::set_state( RecognizerSharedStateType* state )
{
	m_state = state;
}
template< class ImplTraits, class StreamType >
ANTLR_INLINE void BaseRecognizer<ImplTraits, StreamType>::set_debugger( DebugEventListenerType* debugger )
{
	m_debugger = debugger;
}

template< class ImplTraits, class StreamType >
const typename BaseRecognizer<ImplTraits, StreamType>::UnitType* 
BaseRecognizer<ImplTraits, StreamType>::match(ANTLR_UINT32 ttype, BitsetListType* follow)
{
	SuperType*  super = static_cast<SuperType*>(this);
	IntStreamType* is = super->get_istream();

	// Pick up the current input token/node for assignment to labels
	//
	const UnitType* matchedSymbol = this->getCurrentInputSymbol(is);

	//if (is->LA(1) == ttype)
	if (matchedSymbol->get_type() == ttype)
	{
		// The token was the one we were told to expect
		//
		is->consume();					   // Consume that token from the stream
		m_state->set_errorRecovery(false); // Not in error recovery now (if we were)
		m_state->set_failed(false);	// The match was a success
		return matchedSymbol;								// We are done
	}

    // We did not find the expected token type, if we are backtracking then
    // we just set the failed flag and return.
    //
    if	( m_state->get_backtracking() > 0)
    {
		// Backtracking is going on
		//
		m_state->set_failed(true);
		return matchedSymbol;
	}

    // We did not find the expected token and there is no backtracking
    // going on, so we mismatch, which creates an exception in the recognizer exception
    // stack.
    //
	matchedSymbol = this->recoverFromMismatchedToken(ttype, follow);
    return matchedSymbol;

}

template< class ImplTraits, class StreamType >
void BaseRecognizer<ImplTraits, StreamType>::matchAny()
{
	SuperType*  super = static_cast<SuperType*>(this);
	IntStreamType* is = super->get_istream();

	is->consume();
	m_state->set_errorRecovery(false);
	m_state->set_failed(false);
    return;
}

template< class ImplTraits, class StreamType >
bool BaseRecognizer<ImplTraits, StreamType>::mismatchIsUnwantedToken(IntStreamType* is, ANTLR_UINT32 ttype)
{
	ANTLR_UINT32 nextt = is->LA(2);

	if	(nextt == ttype)
	{
		if(m_state->get_exception() != NULL)
			m_state->get_exception()->set_expecting(nextt);
		return true;		// This token is unknown, but the next one is the one we wanted
	}
	else
		return false;	// Neither this token, nor the one following is the one we wanted
}

template< class ImplTraits, class StreamType >
bool BaseRecognizer<ImplTraits, StreamType>::mismatchIsMissingToken(IntStreamType* is, BitsetListType* follow)
{
	bool	retcode;
	BitsetType*	followClone;
	BitsetType*	viableTokensFollowingThisRule;

	if	(follow == NULL)
	{
		// There is no information about the tokens that can follow the last one
		// hence we must say that the current one we found is not a member of the
		// follow set and does not indicate a missing token. We will just consume this
		// single token and see if the parser works it out from there.
		//
		return	false;
	}

	followClone						= NULL;
	viableTokensFollowingThisRule	= NULL;

	// The C bitset maps are laid down at compile time by the
	// C code generation. Hence we cannot remove things from them
	// and so on. So, in order to remove EOR (if we need to) then
	// we clone the static bitset.
	//
	followClone = follow->bitsetLoad();
	if	(followClone == NULL)
		return false;

	// Compute what can follow this grammar reference
	//
	if	(followClone->isMember( ImplTraits::CommonTokenType::EOR_TOKEN_TYPE))
	{
		// EOR can follow, but if we are not the start symbol, we
		// need to remove it.
		//
		followClone->remove(ImplTraits::CommonTokenType::EOR_TOKEN_TYPE);

		// Now compute the visiable tokens that can follow this rule, according to context
		// and make them part of the follow set.
		//
		viableTokensFollowingThisRule = this->computeCSRuleFollow();
		followClone->borInPlace(viableTokensFollowingThisRule);
	}

	/// if current token is consistent with what could come after set
	/// then we know we're missing a token; error recovery is free to
	/// "insert" the missing token
	///
	/// BitSet cannot handle negative numbers like -1 (EOF) so I leave EOR
	/// in follow set to indicate that the fall of the start symbol is
	/// in the set (EOF can follow).
	///
	if	(		followClone->isMember(is->LA(1))
			||	followClone->isMember(ImplTraits::CommonTokenType::EOR_TOKEN_TYPE)
		)
	{
		retcode = true;
	}
	else
	{
		retcode	= false;
	}

	if	(viableTokensFollowingThisRule != NULL)
	{
		delete viableTokensFollowingThisRule;
	}
	if	(followClone != NULL)
	{
		delete followClone;
	}

	return retcode;
}

template< class ImplTraits, class StreamType >
void BaseRecognizer<ImplTraits, StreamType>::mismatch(ANTLR_UINT32 ttype, BitsetListType* follow)
{
	this->get_super()->mismatch( ttype, follow );
}

template< class ImplTraits, class StreamType >
void	BaseRecognizer<ImplTraits, StreamType>::reportError()
{
	this->reportError( ClassForwarder<SuperType>() );
}

template< class ImplTraits, class StreamType >
void	BaseRecognizer<ImplTraits, StreamType>::reportError( ClassForwarder<LexerType> )
{
	// Indicate this recognizer had an error while processing.
	//
	m_state->inc_errorCount();

    this->displayRecognitionError(m_state->get_tokenNames());
}

template< class ImplTraits, class StreamType >
template<typename CompType>
void	BaseRecognizer<ImplTraits, StreamType>::reportError(ClassForwarder<CompType> )
{
	    // Invoke the debugger event if there is a debugger listening to us
	//
	if	( m_debugger != NULL)
	{
		m_debugger->recognitionException( m_state->get_exception() );
	}

    if	( m_state->get_errorRecovery() == true)
    {
		// Already in error recovery so don't display another error while doing so
		//
		return;
    }

    // Signal we are in error recovery now
    //
    m_state->set_errorRecovery(true);

	// Indicate this recognizer had an error while processing.
	//
	m_state->inc_errorCount();

	// Call the error display routine
	//
    this->displayRecognitionError( m_state->get_tokenNames() );
}

template< class ImplTraits, class StreamType >
void	BaseRecognizer<ImplTraits, StreamType>::displayRecognitionError(ANTLR_UINT8** tokenNames)
{
	// Retrieve some info for easy reading.
	//
	ExceptionBaseType* ex	    =		m_state->get_exception();
	StringType ttext;

	// See if there is a 'filename' we can use
	//
	SuperType* super = static_cast<SuperType*>(this);
	super->displayRecognitionError(tokenNames, ex);
}

template< class ImplTraits, class StreamType >
ANTLR_UINT32 BaseRecognizer<ImplTraits, StreamType>::getNumberOfSyntaxErrors()
{
	return	m_state->get_errorCount();
}

template< class ImplTraits, class StreamType >
void	BaseRecognizer<ImplTraits, StreamType>::recover()
{
	SuperType* super = static_cast<SuperType*>(this);
	IntStreamType* is = super->get_parser_istream();
	// Are we about to repeat the same error?
	//
    if	( m_state->get_lastErrorIndex() == is->index())
    {
		// The last error was at the same token index point. This must be a case
		// where LT(1) is in the recovery token set so nothing is
		// consumed. Consume a single token so at least to prevent
		// an infinite loop; this is a failsafe.
		//
		is->consume();
    }

    // Record error index position
    //
    m_state->set_lastErrorIndex( is->index() );

    // Work out the follows set for error recovery
    //
    BitsetType* followSet	= this->computeErrorRecoverySet();

    // Call resync hook (for debuggers and so on)
    //
    this->beginResync();

    // Consume tokens until we have resynced to something in the follows set
    //
    this->consumeUntilSet(followSet);

    // End resync hook
    //
    this->endResync();

    // Destroy the temporary bitset we produced.
    //
    delete followSet;

    // Reset the inError flag so we don't re-report the exception
    //
    m_state->set_error(false);
    m_state->set_failed(false);
}

template< class ImplTraits, class StreamType >
void	BaseRecognizer<ImplTraits, StreamType>::beginResync()
{
	if	(m_debugger != NULL)
	{
		m_debugger->beginResync();
	}
}

template< class ImplTraits, class StreamType >
void	BaseRecognizer<ImplTraits, StreamType>::endResync()
{
	if	(m_debugger != NULL)
	{
		m_debugger->endResync();
	}
}

template< class ImplTraits, class StreamType >
void	BaseRecognizer<ImplTraits, StreamType>::beginBacktrack(ANTLR_UINT32 level)
{
	if	(m_debugger != NULL)
	{
		m_debugger->beginBacktrack(level);
	}
}

template< class ImplTraits, class StreamType >
void	BaseRecognizer<ImplTraits, StreamType>::endBacktrack(ANTLR_UINT32 level, bool /*successful*/)
{
	if	(m_debugger != NULL)
	{
		m_debugger->endBacktrack(level);
	}
}

template< class ImplTraits, class StreamType >
typename BaseRecognizer<ImplTraits, StreamType>::BitsetType*	BaseRecognizer<ImplTraits, StreamType>::computeErrorRecoverySet()
{
	return   this->combineFollows(false);
}

template< class ImplTraits, class StreamType >
typename BaseRecognizer<ImplTraits, StreamType>::BitsetType*	BaseRecognizer<ImplTraits, StreamType>::computeCSRuleFollow()
{
	return   this->combineFollows(false);
}

template< class ImplTraits, class StreamType >
typename BaseRecognizer<ImplTraits, StreamType>::BitsetType*	BaseRecognizer<ImplTraits, StreamType>::combineFollows(bool exact)
{
	BitsetType*	followSet;
    BitsetType*	localFollowSet;
    ANTLR_UINT32	top;
    ANTLR_UINT32	i;

    top	= static_cast<ANTLR_UINT32>( m_state->get_following().size() );

    followSet	    = new BitsetType(0);
	localFollowSet	= NULL;

    for (i = top; i>0; i--)
    {
		localFollowSet =  m_state->get_following().at(i-1).bitsetLoad();

		if  (localFollowSet != NULL)
		{
			followSet->borInPlace(localFollowSet);

			if	(exact == true)
			{
				if	(localFollowSet->isMember( ImplTraits::CommonTokenType::EOR_TOKEN_TYPE) == false)
				{
					// Only leave EOR in the set if at top (start rule); this lets us know
					// if we have to include the follow(start rule); I.E., EOF
					//
					if	(i>1)
					{
						followSet->remove(ImplTraits::CommonTokenType::EOR_TOKEN_TYPE);
					}
				}
				else
				{
					break;	// Cannot see End Of Rule from here, just drop out
				}
			}
			delete localFollowSet;
			localFollowSet = NULL;
		}
    }

	if	(localFollowSet != NULL)
	{
		delete localFollowSet;
	}
    return  followSet;
}

template< class ImplTraits, class StreamType >
const typename BaseRecognizer<ImplTraits, StreamType>::UnitType* 
BaseRecognizer<ImplTraits, StreamType>::recoverFromMismatchedToken( ANTLR_UINT32	ttype, BitsetListType*	follow)
{
	SuperType* super = static_cast<SuperType*>(this);
	IntStreamType* is = super->get_parser_istream();
	const UnitType* matchedSymbol;

	// If the next token after the one we are looking at in the input stream
	// is what we are looking for then we remove the one we have discovered
	// from the stream by consuming it, then consume this next one along too as
	// if nothing had happened.
	//
	if	( this->mismatchIsUnwantedToken( is, ttype) == true)
	{
		// Create an exception if we need one
		//
		new ANTLR_Exception<ImplTraits, UNWANTED_TOKEN_EXCEPTION, StreamType>(this, "");

		// Call resync hook (for debuggers and so on)
		//
		if	(m_debugger != NULL)
		{
			m_debugger->beginResync();
		}

		// "delete" the extra token
		//
		this->beginResync();
		is->consume();
		this->endResync();
		// End resync hook
		//
		if	(m_debugger != NULL)
		{
			m_debugger->endResync();
		}

		// Print out the error after we consume so that ANTLRWorks sees the
		// token in the exception.
		//
		this->reportError();

		// Return the token we are actually matching
		//
		matchedSymbol = this->getCurrentInputSymbol(is);

		// Consume the token that the rule actually expected to get as if everything
		// was hunky dory.
		//
		is->consume();

		m_state->set_error(false); // Exception is not outstanding any more

		return	matchedSymbol;
	}

	// Single token deletion (Unwanted above) did not work
	// so we see if we can insert a token instead by calculating which
	// token would be missing
	//
	if	( this->mismatchIsMissingToken(is, follow))
	{
		// We can fake the missing token and proceed
		//
		new ANTLR_Exception<ImplTraits, MISSING_TOKEN_EXCEPTION, StreamType>(this, "");
		matchedSymbol = this->getMissingSymbol( is, m_state->get_exception(), ttype, follow);
		m_state->get_exception()->set_token( matchedSymbol );
		m_state->get_exception()->set_expecting(ttype);

		// Print out the error after we insert so that ANTLRWorks sees the
		// token in the exception.
		//
		this->reportError();

		m_state->set_error(false);	// Exception is not outstanding any more

		return	matchedSymbol;
	}

	// Create an exception if we need one
	//
	new ANTLR_Exception<ImplTraits, RECOGNITION_EXCEPTION, StreamType>(this, "");

	// Neither deleting nor inserting tokens allows recovery
	// must just report the exception.
	//
	m_state->set_error(true);
	return NULL;
}

template< class ImplTraits, class StreamType >
const typename BaseRecognizer<ImplTraits, StreamType>::UnitType* 
BaseRecognizer<ImplTraits, StreamType>::recoverFromMismatchedSet(BitsetListType*	follow)
{
	SuperType* super = static_cast<SuperType*>(this);
	IntStreamType* is = super->get_parser_istream();
	const UnitType* matchedSymbol;

	if	(this->mismatchIsMissingToken(is, follow) == true)
	{
		// We can fake the missing token and proceed
		//
		new ANTLR_Exception<ImplTraits, MISSING_TOKEN_EXCEPTION, StreamType>(this);
		matchedSymbol = this->getMissingSymbol(is, m_state->get_exception(), follow);
		m_state->get_exception()->set_token(matchedSymbol);

		// Print out the error after we insert so that ANTLRWorks sees the
		// token in the exception.
		//
		this->reportError();

		m_state->set_error(false);	// Exception is not outstanding any more

		return	matchedSymbol;
	}

    // TODO - Single token deletion like in recoverFromMismatchedToken()
    //
    m_state->set_error(true);
	m_state->set_failed(true);
	return NULL;
}

template< class ImplTraits, class StreamType >
bool  BaseRecognizer<ImplTraits, StreamType>::recoverFromMismatchedElement(BitsetListType*	followBits)
{
	SuperType* super = static_cast<SuperType*>(this);
	IntStreamType* is = super->get_parser_istream();

	BitsetType* follow	= followBits->load();
	BitsetType*   viableToksFollowingRule;

    if	(follow == NULL)
    {
		/* The follow set is NULL, which means we don't know what can come
		 * next, so we "hit and hope" by just signifying that we cannot
		 * recover, which will just cause the next token to be consumed,
		 * which might dig us out.
		 */
		return	false;
    }

    /* We have a bitmap for the follow set, hence we can compute
     * what can follow this grammar element reference.
     */
    if	(follow->isMember( ImplTraits::CommonTokenType::EOR_TOKEN_TYPE) == true)
    {
		/* First we need to know which of the available tokens are viable
		 * to follow this reference.
		 */
		viableToksFollowingRule	= this->computeCSRuleFollow();

		/* Remove the EOR token, which we do not wish to compute with
		 */
		follow->remove( ImplTraits::CommonTokenType::EOR_TOKEN_TYPE);
		delete viableToksFollowingRule;
		/* We now have the computed set of what can follow the current token
		 */
    }

    /* We can now see if the current token works with the set of tokens
     * that could follow the current grammar reference. If it looks like it
     * is consistent, then we can "insert" that token by not throwing
     * an exception and assuming that we saw it.
     */
    if	( follow->isMember(is->LA(1)) == true)
    {
		/* report the error, but don't cause any rules to abort and stuff
		 */
		this->reportError();
		if	(follow != NULL)
		{
			delete follow;
		}
		m_state->set_error(false);
		m_state->set_failed(false);
		return true;	/* Success in recovery	*/
    }

    if	(follow != NULL)
    {
		delete follow;
    }

    /* We could not find anything viable to do, so this is going to
     * cause an exception.
     */
    return  false;
}

template< class ImplTraits, class StreamType >
void	BaseRecognizer<ImplTraits, StreamType>::consumeUntil(ANTLR_UINT32   tokenType)
{
	SuperType* super = static_cast<SuperType*>(this);
	IntStreamType* is = super->get_parser_istream();

	// What do have at the moment?
    //
    ANTLR_UINT32 ttype	= is->LA(1);

    // Start eating tokens until we get to the one we want.
    //
    while   (ttype != ImplTraits::CommonTokenType::TOKEN_EOF && ttype != tokenType)
    {
		is->consume();
		ttype	= is->LA(1);
    }
}

template< class ImplTraits, class StreamType >
void	BaseRecognizer<ImplTraits, StreamType>::consumeUntilSet(BitsetType*	set)
{
    ANTLR_UINT32	    ttype;
	SuperType* super = static_cast<SuperType*>(this);
	IntStreamType* is = super->get_parser_istream();

    // What do have at the moment?
    //
    ttype	= is->LA(1);

    // Start eating tokens until we get to one we want.
    //
    while   (ttype != ImplTraits::CommonTokenType::TOKEN_EOF && set->isMember(ttype) == false)
    {
		is->consume();
		ttype	= is->LA(1);
    }

}

template< class ImplTraits, class StreamType >
ANTLR_MARKER	BaseRecognizer<ImplTraits, StreamType>::getRuleMemoization( ANTLR_INTKEY	ruleIndex, ANTLR_MARKER	ruleParseStart)
{
	/* The rule memos are an ANTLR3_LIST of ANTLR3_LIST.
     */
	typedef IntTrie<ImplTraits, ANTLR_MARKER> RuleListType;
	typedef TrieEntry<ImplTraits, std::shared_ptr<RuleListType>> EntryType;
	typedef TrieEntry<ImplTraits, ANTLR_MARKER> SubEntryType;
    ANTLR_MARKER	stopIndex;
    EntryType*	entry;

    /* See if we have a list in the ruleMemos for this rule, and if not, then create one
     * as we will need it eventually if we are being asked for the memo here.
     */
    entry	= m_state->get_ruleMemo()->get(ruleIndex);

    if	(entry == NULL)
    {
		/* Did not find it, so create a new one for it, with a bit depth based on the
		 * size of the input stream. We need the bit depth to incorporate the number if
		 * bits required to represent the largest possible stop index in the input, which is the
		 * last character. An int stream is free to return the largest 64 bit offset if it has
		 * no idea of the size, but you should remember that this will cause the leftmost
		 * bit match algorithm to run to 63 bits, which will be the whole time spent in the trie ;-)
		 */
		m_state->get_ruleMemo()->add( ruleIndex, std::make_shared<RuleListType>(63) );

		/* We cannot have a stopIndex in a trie we have just created of course
		 */
		return	MEMO_RULE_UNKNOWN;
    }

    std::shared_ptr<RuleListType> ruleList	= entry->get_data();

    /* See if there is a stop index associated with the supplied start index.
     */
    stopIndex	= 0;

    SubEntryType* sub_entry = ruleList->get(ruleParseStart);
    if (sub_entry != NULL)
    {
		stopIndex = sub_entry->get_data();
    }

    if	(stopIndex == 0)
    {
		return MEMO_RULE_UNKNOWN;
    }

    return  stopIndex;
}

template< class ImplTraits, class StreamType >
bool	BaseRecognizer<ImplTraits, StreamType>::alreadyParsedRule(ANTLR_MARKER	ruleIndex)
{
	SuperType* super = static_cast<SuperType*>(this);
	IntStreamType* is = super->get_istream();

    /* See if we have a memo marker for this.
     */
    ANTLR_MARKER stopIndex	    = this->getRuleMemoization( ruleIndex, is->index() );

    if	(stopIndex  == MEMO_RULE_UNKNOWN)
    {
		return false;
    }

    if	(stopIndex == MEMO_RULE_FAILED)
    {
		m_state->set_failed(true);
    }
    else
    {
		is->seek(stopIndex+1);
    }

    /* If here then the rule was executed for this input already
     */
    return  true;
}

template< class ImplTraits, class StreamType >
void	BaseRecognizer<ImplTraits, StreamType>::memoize(ANTLR_MARKER ruleIndex, ANTLR_MARKER ruleParseStart)
{
   /* The rule memos are an ANTLR3_LIST of ANTLR3_LIST.
    */
	typedef IntTrie<ImplTraits, ANTLR_MARKER> RuleListType;
	typedef TrieEntry<ImplTraits, std::shared_ptr<RuleListType>> EntryType;
    EntryType*	    entry;
    ANTLR_MARKER	    stopIndex;
	SuperType* super = static_cast<SuperType*>(this);
	IntStreamType* is = super->get_istream();

    stopIndex	= (m_state->get_failed() == true) ? MEMO_RULE_FAILED : is->index() - 1;

    entry	= m_state->get_ruleMemo()->get(ruleIndex);

    if	(entry != NULL)
    {
		std::shared_ptr<RuleListType>	ruleList = entry->get_data();

		/* If we don't already have this entry, append it. The memoize trie does not
		 * accept duplicates so it won't add it if already there and we just ignore the
		 * return code as we don't care if it is there already.
		 */
		ruleList->add(ruleParseStart, stopIndex);
    }
}

template< class ImplTraits, class StreamType >
const typename BaseRecognizer<ImplTraits, StreamType>::UnitType*
BaseRecognizer<ImplTraits, StreamType>::getCurrentInputSymbol( IntStreamType* istream )
{
	return this->getCurrentInputSymbol( istream, ClassForwarder<SuperType>() );
}

template< class ImplTraits, class StreamType >
const typename BaseRecognizer<ImplTraits, StreamType>::UnitType*
BaseRecognizer<ImplTraits, StreamType>::getCurrentInputSymbol(IntStreamType* /*istream*/, ClassForwarder<LexerType>)
{
	return NULL;
}

template< class ImplTraits, class StreamType >
const typename BaseRecognizer<ImplTraits, StreamType>::UnitType*
BaseRecognizer<ImplTraits, StreamType>::getCurrentInputSymbol(IntStreamType* istream, ClassForwarder<ParserType>)
{
	typedef typename ImplTraits::TokenStreamType TokenStreamType;
	TokenStreamType* token_stream = static_cast<TokenStreamType*>(istream);
	return token_stream->LT(1);
}

template< class ImplTraits, class StreamType >
const typename BaseRecognizer<ImplTraits, StreamType>::UnitType*
BaseRecognizer<ImplTraits, StreamType>::getCurrentInputSymbol(IntStreamType* istream, ClassForwarder<TreeParserType>)
{
	typedef typename ImplTraits::TreeNodeStreamType TreeNodeStreamType;
	TreeNodeStreamType*	ctns = static_cast<TreeNodeStreamType*>(istream);
	return ctns->LT(1);
}


template< class ImplTraits, class StreamType >
typename BaseRecognizer<ImplTraits, StreamType>::UnitType*	BaseRecognizer<ImplTraits, StreamType>::getMissingSymbol( IntStreamType* istream,
										  ExceptionBaseType*		e,
										  ANTLR_UINT32			expectedTokenType,
										  BitsetListType*	follow)
{
	return this->get_super()->getMissingSymbol( istream, e, expectedTokenType, follow );
}


template< class ImplTraits, class StreamType >
	template<typename Predicate>
bool  BaseRecognizer<ImplTraits, StreamType>::synpred(ClassForwarder<Predicate> pred)
{
	ANTLR_MARKER   start;
    SuperType* super = static_cast<SuperType*>(this);
	IntStreamType* is = super->get_istream();

    /* Begin backtracking so we can get back to where we started after trying out
     * the syntactic predicate.
     */
    start   = is->mark();
    m_state->inc_backtracking();

    /* Try the syntactical predicate
     */
    this->get_super()->synpred( pred );

    /* Reset
     */
    is->rewind(start);
    m_state->dec_backtracking();

    if	( m_state->get_failed() == true)
    {
		/* Predicate failed
		 */
		m_state->set_failed(false);
		return	false;
    }
    else
    {
		/* Predicate was successful
		 */
		m_state->set_failed(false);
		return	true;
    }
}

template< class ImplTraits, class StreamType >
void BaseRecognizer<ImplTraits, StreamType>::exConstruct()
{
	this->get_super()->exConstruct();
}

template< class ImplTraits, class StreamType >
void  BaseRecognizer<ImplTraits, StreamType>::reset()
{
	this->reset( ClassForwarder<SuperType>() );
}

template< class ImplTraits, class StreamType >
template< typename CompType >
void  BaseRecognizer<ImplTraits, StreamType>::reset( ClassForwarder<CompType> )
{
	typedef typename RecognizerSharedStateType::RuleMemoType RuleMemoType;
	 m_state->get_following().clear();

	// Reset the state flags
	//
	m_state->set_errorRecovery(false);
	m_state->set_lastErrorIndex(-1);
	m_state->set_failed(false);
	m_state->set_errorCount(0);
	m_state->set_backtracking(0);

	if	(m_state->get_ruleMemo() != NULL)
	{
		delete m_state->get_ruleMemo();
		m_state->set_ruleMemo( new RuleMemoType(15) );	/* 16 bit depth is enough for 32768 rules! */
	}
}

template< class ImplTraits, class StreamType >
void  BaseRecognizer<ImplTraits, StreamType>::reset( ClassForwarder<LexerType> )
{
	m_state->set_token_present( false );
    m_state->set_type( ImplTraits::CommonTokenType::TOKEN_INVALID );
    m_state->set_channel( TOKEN_DEFAULT_CHANNEL );
    m_state->set_tokenStartCharIndex( -1 );
    m_state->set_tokenStartCharPositionInLine(-1);
    m_state->set_tokenStartLine( -1 );
    m_state->set_text("");
}

template< class ImplTraits, class StreamType >
BaseRecognizer<ImplTraits, StreamType>::~BaseRecognizer()
{
	// Did we have a state allocated?
	//
	if	(m_state != NULL)
	{
		// Free any rule memoization we set up
		//
		if	(m_state->get_ruleMemo() != NULL)
		{
			delete m_state->get_ruleMemo();
			m_state->set_ruleMemo(NULL);
		}


		// Free any exception space we have left around
		//
		ExceptionBaseType* thisE = m_state->get_exception();
		if	(thisE != NULL)
		{
			delete thisE;
		}

		// Free the shared state memory
		//
		delete m_state;
	}

	// Free the actual recognizer space
	//
}



}
