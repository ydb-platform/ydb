namespace antlr3 {

template< class ImplTraits >
TreeParser<ImplTraits>::TreeParser( ANTLR_UINT32 sizeHint, TreeNodeStreamType* ctnstream,
											RecognizerSharedStateType* state)
											:RecognizerType( sizeHint, state )
{
	/* Install the tree node stream
	*/
	this->setTreeNodeStream(ctnstream);

}

template< class ImplTraits >
TreeParser<ImplTraits>::~TreeParser()
{
	this->get_rec()->get_state()->get_following().clear();
}

template< class ImplTraits >
typename TreeParser<ImplTraits>::TreeNodeStreamType* TreeParser<ImplTraits>::get_ctnstream() const
{
	return m_ctnstream;
}

template< class ImplTraits >
typename TreeParser<ImplTraits>::IntStreamType* TreeParser<ImplTraits>::get_istream() const
{
	return m_ctnstream;
}

template< class ImplTraits >
typename TreeParser<ImplTraits>::IntStreamType* TreeParser<ImplTraits>::get_parser_istream() const
{
	return m_ctnstream;
}

template< class ImplTraits >
typename TreeParser<ImplTraits>::RecognizerType* TreeParser<ImplTraits>::get_rec()
{
	return this;
}

template< class ImplTraits >
void TreeParser<ImplTraits>::fillExceptionData( ExceptionBaseType* ex )
{
	auto xxx = m_ctnstream->LT(1);
	//ex->set_token( m_ctnstream->LT(1) );	    /* Current input tree node */
	ex->set_line( ex->get_token()->get_line() );
	ex->set_charPositionInLine( ex->get_token()->get_charPositionInLine() );
	ex->set_index( m_ctnstream->index() );

	// Are you ready for this? Deep breath now...
	//
	{
		TreeTypePtr tnode;

		tnode		= ex->get_token();

		if	(tnode->get_token()    == NULL)
		{
			ex->set_streamName("-unknown source-" );
		}
		else
		{
			if	( tnode->get_token()->get_input() == NULL)
			{
				ex->set_streamName("");
			}
			else
			{
				ex->set_streamName(	tnode->get_token()->get_input()->get_fileName() );
			}
		}
		ex->set_message("Unexpected node");
	}
}

template< class ImplTraits >
void TreeParser<ImplTraits>::displayRecognitionError( ANTLR_UINT8** /*tokenNames*/, ExceptionBaseType* ex )
{
	typename ImplTraits::StringStreamType errtext;
	// See if there is a 'filename' we can use
	//
	if( ex->get_streamName().empty() )
	{
		if(ex->get_token()->get_type() == ImplTraits::CommonTokenType::TOKEN_EOF)
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

	IntStreamType* is			= this->get_istream();
	TreeTypePtr theBaseTree	= this->get_rec()->get_state()->get_exception()->get_token();
	StringType ttext		= theBaseTree->toStringTree();

	if  (theBaseTree != NULL)
	{
		TreeTypePtr  theCommonTree	=  static_cast<TreeTypePtr>(theBaseTree);
		if	(theCommonTree != NULL)
		{
			CommonTokenType* theToken	= theBaseTree->getToken();
		}
		errtext << ", at offset "
			    << theBaseTree->getCharPositionInLine();
		errtext << ", near " << ttext;
	}
	ex->displayRecognitionError( errtext );
	ImplTraits::displayRecognitionError( errtext.str() );
}

template< class ImplTraits >
void	TreeParser<ImplTraits>::setTreeNodeStream(TreeNodeStreamType* input)
{
	m_ctnstream = input;
    this->get_rec()->reset();
    m_ctnstream->reset();
}

template< class ImplTraits >
typename TreeParser<ImplTraits>::TreeNodeStreamType* TreeParser<ImplTraits>::getTreeNodeStream()
{
	return m_ctnstream;
}

template< class ImplTraits >
void TreeParser<ImplTraits>::exConstruct()
{
	new ANTLR_Exception<ImplTraits, MISMATCHED_TREE_NODE_EXCEPTION, TreeNodeStreamType>( this->get_rec(), "" );
}

template< class ImplTraits >
void TreeParser<ImplTraits>::mismatch(ANTLR_UINT32 ttype, BitsetListType* follow)
{
	this->exConstruct();
    this->recoverFromMismatchedToken(ttype, follow);
}

template< class ImplTraits >
typename TreeParser<ImplTraits>::TokenType*
TreeParser<ImplTraits>::getMissingSymbol( IntStreamType* istream, ExceptionBaseType*		/*e*/,
					  ANTLR_UINT32	 expectedTokenType, BitsetListType*	/*follow*/)
{
	TreeNodeStreamType*		tns;
	TreeTypePtr				node;
	TreeTypePtr				current;
	CommonTokenType*		token;
	StringType				text;

	// Dereference the standard pointers
	//
    tns	    = static_cast<TreeNodeStreamType*>(istream);

	// Create a new empty node, by stealing the current one, or the previous one if the current one is EOF
	//
	current	= tns->LT(1);

	if	(current == tns->get_EOF_NODE_p())
	{
		current = tns->LT(-1);
	}
	node	= current->dupNode();

	// Find the newly dupicated token
	//
	token	= node->getToken();

	// Create the token text that shows it has been inserted
	//
	token->setText("<missing ");
	text = token->getText();
	text.append((const char *)this->get_rec()->get_state()->get_tokenName(expectedTokenType));
	text.append((const char *)">");

	// Finally return the pointer to our new node
	//
	return	node;
}


}
