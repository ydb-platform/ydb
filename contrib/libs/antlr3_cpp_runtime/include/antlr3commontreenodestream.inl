namespace antlr3 {

template<class ImplTraits>
CommonTreeNodeStream<ImplTraits>::CommonTreeNodeStream(ANTLR_UINT32 hint)
{
	this->init(hint);
}

template<class ImplTraits>
void CommonTreeNodeStream<ImplTraits>::init( ANTLR_UINT32 hint )
{
	m_root = NULL;
	m_adaptor = new TreeAdaptorType;
	// Create the node list map
	//
	if	(hint == 0)
		hint = DEFAULT_INITIAL_BUFFER_SIZE;
	m_nodes.reserve( DEFAULT_INITIAL_BUFFER_SIZE );

	m_p = -1;
	m_currentNode = NULL;
	m_previousNode = NULL;
	m_currentChildIndex = 0; 
	m_absoluteNodeIndex = 0;
	m_lookAhead = NULL;
	m_lookAheadLength = 0;
	m_head = 0;
	m_tail = 0;
	m_uniqueNavigationNodes = false;
	m_isRewriter = false;

	CommonTokenType* token		= new CommonTokenType(CommonTokenType::TOKEN_UP);
	token->set_tokText( "UP" );
	m_UP.set_token( token );

	token		= new CommonTokenType(CommonTokenType::TOKEN_DOWN);
	token->set_tokText( "DOWN" );
	m_DOWN.set_token( token );

	token		= new CommonTokenType(CommonTokenType::TOKEN_EOF);
	token->set_tokText( "EOF" );
	m_EOF_NODE.set_token( token );

	token		= new CommonTokenType(CommonTokenType::TOKEN_INVALID);
	token->set_tokText( "INVALID" );
	m_EOF_NODE.set_token( token );
}

template<class ImplTraits>
CommonTreeNodeStream<ImplTraits>::CommonTreeNodeStream( const CommonTreeNodeStream& ctn )
{
	m_root = ctn.m_root;
	m_adaptor = ctn.m_adaptor;
	m_nodes.reserve( DEFAULT_INITIAL_BUFFER_SIZE );
	m_nodeStack = ctn.m_nodeStack;
	m_p = -1;
	m_currentNode = NULL;
	m_previousNode = NULL;
	m_currentChildIndex = 0; 
	m_absoluteNodeIndex = 0;
	m_lookAhead = NULL;
	m_lookAheadLength = 0;
	m_head = 0;
	m_tail = 0;
	m_uniqueNavigationNodes = false;
	m_isRewriter = true;

	m_UP.set_token( ctn.m_UP.get_token() );
	m_DOWN.set_token( ctn.m_DOWN.get_token() );
	m_EOF_NODE.set_token( ctn.m_EOF_NODE.get_token() );
	m_INVALID_NODE.set_token( ctn.m_INVALID_NODE.get_token() );
}

template<class ImplTraits>
CommonTreeNodeStream<ImplTraits>::CommonTreeNodeStream( TreeTypePtr tree, ANTLR_UINT32 hint )
{
	this->init(hint);
	m_root = tree;
}

template<class ImplTraits>
CommonTreeNodeStream<ImplTraits>::~CommonTreeNodeStream()
{
	// If this is a rewrting stream, then certain resources
	// belong to the originating node stream and we do not
	// free them here.
	//
	if	( m_isRewriter != true)
	{
		delete m_adaptor;

		m_nodeStack.clear();

		delete m_INVALID_NODE.get_token();
		delete m_EOF_NODE.get_token();
		delete m_DOWN.get_token();
		delete m_UP.get_token();
	}
	
	m_nodes.clear();
}

template<class ImplTraits>
typename CommonTreeNodeStream<ImplTraits>::TreeTypePtr	CommonTreeNodeStream<ImplTraits>::_LT(ANTLR_INT32 k)
{
	if	( m_p == -1)
	{
		this->fillBufferRoot();
	}

	if	(k < 0)
	{
		return this->LB(-k);
	}
	else if	(k == 0)
	{
		return	&(m_INVALID_NODE);
	}

	// k was a legitimate request, 
	//
	if	(( m_p + k - 1) >= (ANTLR_INT32)(m_nodes.size()))
	{
		return &(m_EOF_NODE);
	}

	return	m_nodes[ m_p + k - 1 ];
}

template<class ImplTraits>
typename CommonTreeNodeStream<ImplTraits>::TreeTypePtr	CommonTreeNodeStream<ImplTraits>::getTreeSource()
{
	return m_root;
}

template<class ImplTraits>
typename CommonTreeNodeStream<ImplTraits>::TreeAdaptorType*	CommonTreeNodeStream<ImplTraits>::getTreeAdaptor()
{
	return m_adaptor;
}

template<class ImplTraits>
void  CommonTreeNodeStream<ImplTraits>::set_uniqueNavigationNodes(bool uniqueNavigationNodes)
{
	m_uniqueNavigationNodes = uniqueNavigationNodes;
}

template<class ImplTraits>
typename CommonTreeNodeStream<ImplTraits>::StringType  CommonTreeNodeStream<ImplTraits>::toString()
{
    return  this->toStringSS(m_root, NULL);
}

template<class ImplTraits>
typename CommonTreeNodeStream<ImplTraits>::StringType  CommonTreeNodeStream<ImplTraits>::toStringSS(TreeTypePtr start, TreeTypePtr stop)
{
	StringType  buf;
    this->toStringWork(start, stop, buf);
    return  buf;
}

template<class ImplTraits>
void CommonTreeNodeStream<ImplTraits>::toStringWork(TreeTypePtr start, TreeTypePtr stop, StringType& str)
{
	ANTLR_UINT32   n;
	ANTLR_UINT32   c;
	StringStreamType buf;

	if	(!start->isNilNode() )
	{
		StringType	text;

		text	= start->toString();

		if  (text.empty())
		{
			buf << ' ';
			buf << start->getType();
		}
		else
			buf << text;
	}

	if	(start == stop)
	{
		return;		/* Finished */
	}

	n = start->getChildCount();

	if	(n > 0 && ! start->isNilNode() )
	{
		buf << ' ';
		buf << CommonTokenType::TOKEN_DOWN;
	}

	for	(c = 0; c<n ; c++)
	{
		TreeTypePtr   child;

		child = start->getChild(c);
		this->toStringWork(child, stop, buf);
	}

	if	(n > 0 && ! start->isNilNode() )
	{
		buf << ' ';
		buf << CommonTokenType::TOKEN_UP;
	}
	str = buf.str();
}

template<class ImplTraits>
typename  CommonTreeNodeStream<ImplTraits>::TreeTypePtr	CommonTreeNodeStream<ImplTraits>::get(ANTLR_INT32 k)
{
	if( m_p == -1 )
	{
		this->fillBufferRoot();
	}

	return m_nodes[k];
}

template<class ImplTraits>
void	CommonTreeNodeStream<ImplTraits>::replaceChildren(TreeTypePtr parent, 
															ANTLR_INT32 startChildIndex, 
															ANTLR_INT32 stopChildIndex, 
															TreeTypePtr t)
{
	if	(parent != NULL)
	{
		TreeAdaptorType*	adaptor;
		adaptor	= this->getTreeAdaptor();
		adaptor->replaceChildren(parent, startChildIndex, stopChildIndex, t);
	}
}

template<class ImplTraits>
typename CommonTreeNodeStream<ImplTraits>::TreeTypePtr CommonTreeNodeStream<ImplTraits>::LB(ANTLR_INT32 k)
{
	if	( k==0)
	{
		return	&(m_INVALID_NODE);
	}

	if	( (m_p - k) < 0)
	{
		return	&(m_INVALID_NODE);
	}

	return m_nodes[ m_p - k ];
}

template<class ImplTraits>
void CommonTreeNodeStream<ImplTraits>::addNavigationNode(ANTLR_UINT32 ttype)
{
	TreeTypePtr	    node;

	node = NULL;

	if	(ttype == CommonTokenType::TOKEN_DOWN)
	{
		if  (this->hasUniqueNavigationNodes() == true)
		{
			node    = this->newDownNode();
		}
		else
		{
			node    = &m_DOWN;
		}
	}
	else
	{
		if  (this->hasUniqueNavigationNodes() == true)
		{
			node    = this->newUpNode();
		}
		else
		{
			node    = &m_UP;
		}
	}

	// Now add the node we decided upon.
	//
	m_nodes.push_back(node);
}

template<class ImplTraits>
typename CommonTreeNodeStream<ImplTraits>::TreeTypePtr	CommonTreeNodeStream<ImplTraits>::newDownNode()
{
	TreeTypePtr	    dNode;
    CommonTokenType*    token;

    token					= new CommonTokenType(CommonTokenType::TOKEN_DOWN);
	token->set_tokText("DOWN");
    dNode					= new TreeType(token);
    return  &dNode;
}

template<class ImplTraits>
typename CommonTreeNodeStream<ImplTraits>::TreeTypePtr	CommonTreeNodeStream<ImplTraits>::newUpNode()
{
	TreeTypePtr	    uNode;
    CommonTokenType*    token;

    token					= new CommonTokenType(CommonTokenType::TOKEN_UP);
	token->set_tokText("UP");
    uNode					= new TreeType(token);
    return  &uNode;

}

template<class ImplTraits>
bool  CommonTreeNodeStream<ImplTraits>::hasUniqueNavigationNodes() const
{
	 return  m_uniqueNavigationNodes;
}

template<class ImplTraits>
ANTLR_UINT32	CommonTreeNodeStream<ImplTraits>::getLookaheadSize()
{
	return	m_tail < m_head 
	    ?	(m_lookAheadLength - m_head + m_tail)
	    :	(m_tail - m_head);
}

template<class ImplTraits>
void	CommonTreeNodeStream<ImplTraits>::push(ANTLR_INT32 index)
{
	m_nodeStack.push(m_p);	// Save current index
	this->seek(index);
}

template<class ImplTraits>
ANTLR_INT32	CommonTreeNodeStream<ImplTraits>::pop()
{
	ANTLR_INT32	retVal;

	retVal = m_nodeStack.top();
	m_nodeStack.pop();
	this->seek(retVal);
	return retVal;
}

template<class ImplTraits>
void	CommonTreeNodeStream<ImplTraits>::reset()
{
	if	( m_p != -1)
	{
		m_p	= 0;
	}
	BaseType::m_lastMarker		= 0;


	// Free and reset the node stack only if this is not
	// a rewriter, which is going to reuse the originating
	// node streams node stack
	//
	if  (m_isRewriter != true)
		m_nodeStack.clear();
}

template<class ImplTraits>
void CommonTreeNodeStream<ImplTraits>::fillBufferRoot()
{
	// Call the generic buffer routine with the root as the
	// argument
	//
	this->fillBuffer(m_root);
	m_p = 0;					// Indicate we are at buffer start
}

template<class ImplTraits>
void CommonTreeNodeStream<ImplTraits>::fillBuffer(TreeTypePtr t)
{
	bool	nilNode;
	ANTLR_UINT32	nCount;
	ANTLR_UINT32	c;

	nilNode = m_adaptor->isNilNode(t);

	// If the supplied node is not a nil (list) node then we
	// add in the node itself to the vector
	//
	if	(nilNode == false)
	{
		m_nodes.push_back(t);	
	}

	// Only add a DOWN node if the tree is not a nil tree and
	// the tree does have children.
	//
	nCount = t->getChildCount();

	if	(nilNode == false && nCount>0)
	{
		this->addNavigationNode( CommonTokenType::TOKEN_DOWN);
	}

	// We always add any children the tree contains, which is
	// a recursive call to this function, which will cause similar
	// recursion and implement a depth first addition
	//
	for	(c = 0; c < nCount; c++)
	{
		this->fillBuffer( m_adaptor->getChild(t, c));
	}

	// If the tree had children and was not a nil (list) node, then we
	// we need to add an UP node here to match the DOWN node
	//
	if	(nilNode == false && nCount > 0)
	{
		this->addNavigationNode(CommonTokenType::TOKEN_UP);
	}
}



}

