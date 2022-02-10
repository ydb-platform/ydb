namespace antlr3 {

template<class ImplTraits, class CtxType>
CyclicDFA<ImplTraits, CtxType>::CyclicDFA( ANTLR_INT32	decisionNumber
				, const ANTLR_UCHAR*	description
				, const ANTLR_INT32* const	eot
				, const ANTLR_INT32* const	eof
				, const ANTLR_INT32* const	min
				, const ANTLR_INT32* const	max
				, const ANTLR_INT32* const	accept
				, const ANTLR_INT32* const	special
				, const ANTLR_INT32* const *const	transition )
				:m_decisionNumber(decisionNumber)
				, m_eot(eot)
				, m_eof(eof)
				, m_min(min)
				, m_max(max)
				, m_accept(accept)
				, m_special(special)
				, m_transition(transition)
{
	m_description = description;
}

template<class ImplTraits, class CtxType>
CyclicDFA<ImplTraits, CtxType>::CyclicDFA( const CyclicDFA& dfa )
{
	m_decisionNumber = dfa.m_decisionNumber;
	m_description = dfa.m_description;
	m_eot = dfa.m_eot;
	m_eof = dfa.m_eof;
	m_min = dfa.m_min;
	m_max = dfa.m_max;
	m_accept = dfa.m_accept;
	m_special = dfa.m_special;
	m_transition = dfa.m_transition;
}

template<class ImplTraits, class CtxType>
CyclicDFA<ImplTraits, CtxType>& CyclicDFA<ImplTraits, CtxType>::operator=( const CyclicDFA& dfa)
{
	m_decisionNumber = dfa.m_decisionNumber;
	m_description = dfa.m_description;
	m_eot = dfa.m_eot;
	m_eof = dfa.m_eof;
	m_min = dfa.m_min;
	m_max = dfa.m_max;
	m_accept = dfa.m_accept;
	m_special = dfa.m_special;
	m_transition = dfa.m_transition;
	return *this;
}

template<class ImplTraits, class CtxType>
ANTLR_INT32	CyclicDFA<ImplTraits, CtxType>::specialStateTransition(CtxType * ,
																	RecognizerType* ,
																	IntStreamType* , ANTLR_INT32 )
{
	return -1;
}

template<class ImplTraits, class CtxType>
ANTLR_INT32	CyclicDFA<ImplTraits, CtxType>::specialTransition(CtxType * /*ctx*/,
																	RecognizerType* /*recognizer*/,
																	IntStreamType* /*is*/, ANTLR_INT32 /*s*/)
{
	return 0;
}

template<class ImplTraits, class CtxType>
  template<typename SuperType>
ANTLR_INT32	CyclicDFA<ImplTraits, CtxType>::predict(CtxType * ctx,
															RecognizerType* recognizer,
															IntStreamType* is, SuperType& super)
{
	ANTLR_MARKER	mark;
    ANTLR_INT32	s;
    ANTLR_INT32	specialState;
    ANTLR_INT32	c;

    mark	= is->mark();	    /* Store where we are right now	*/
    s		= 0;		    /* Always start with state 0	*/

	for (;;)
	{
		/* Pick out any special state entry for this state
		 */
		specialState	= m_special[s];

		/* Transition the special state and consume an input token
		 */
		if  (specialState >= 0)
		{
			s = super.specialStateTransition(ctx, recognizer, is, specialState);

			// Error?
			//
			if	(s<0)
			{
				// If the predicate/rule raised an exception then we leave it
				// in tact, else we have an NVA.
				//
				if	(recognizer->get_state()->get_error() != true)
				{
					this->noViableAlt(recognizer, s);
				}
				is->rewind(mark);
				return	0;
			}
			is->consume();
			continue;
		}

		/* Accept state?
		 */
		if  (m_accept[s] >= 1)
		{
			is->rewind(mark);
			return  m_accept[s];
		}

		/* Look for a normal transition state based upon the input token element
		 */
		c = is->LA(1);

		/* Check against min and max for this state
		 */
		if  (c>= m_min[s] && c <= m_max[s])
		{
			ANTLR_INT32   snext;

			/* What is the next state?
			 */
			snext = m_transition[s][c - m_min[s]];

			if	(snext < 0)
			{
				/* Was in range but not a normal transition
				 * must check EOT, which is like the else clause.
				 * eot[s]>=0 indicates that an EOT edge goes to another
				 * state.
				 */
				if  ( m_eot[s] >= 0)
				{
					s = m_eot[s];
					is->consume();
					continue;
				}
				this->noViableAlt(recognizer, s);
				is->rewind(mark);
				return	0;
			}

			/* New current state - move to it
			 */
			s	= snext;
			is->consume();
			continue;
		}
		/* EOT Transition?
		 */
		if  ( m_eot[s] >= 0)
		{
			s	= m_eot[s];
			is->consume();
			continue;
		}
		/* EOF transition to accept state?
		 */
		if  ( c == ImplTraits::CommonTokenType::TOKEN_EOF && m_eof[s] >= 0)
		{
			is->rewind(mark);
			return  m_accept[m_eof[s]];
		}

		/* No alt, so bomb
		 */
		this->noViableAlt(recognizer, s);
		is->rewind(mark);
		return 0;
	}
}

template<class ImplTraits, class CtxType>
void CyclicDFA<ImplTraits, CtxType>::noViableAlt(RecognizerType* rec, ANTLR_UINT32 s)
{
	// In backtracking mode, we just set the failed flag so that the
	// alt can just exit right now. If we are parsing though, then
	// we want the exception to be raised.
	//
    if	(rec->get_state()->get_backtracking() > 0)
    {
		rec->get_state()->set_failed(true);
    }
	else
	{
		ANTLR_Exception<ImplTraits, NO_VIABLE_ALT_EXCEPTION, StreamType>* ex 
			= new ANTLR_Exception<ImplTraits, NO_VIABLE_ALT_EXCEPTION, StreamType>( rec, (const char*)m_description );
		ex->set_decisionNum( m_decisionNumber );
		ex->set_state(s);
	}
}

}
