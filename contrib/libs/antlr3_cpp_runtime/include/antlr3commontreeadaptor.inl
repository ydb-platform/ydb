namespace antlr3 {

template <typename ImplTraits>
CommonResourcePoolManager<ImplTraits>::CommonResourcePoolManager(CommonTreeStore<ImplTraits> * pool)
	: m_pool(pool)
{}

template <typename ImplTraits>
CommonResourcePoolManager<ImplTraits>::CommonResourcePoolManager()
	: m_pool(NULL)
{}

template <typename ImplTraits>
CommonResourcePoolManager<ImplTraits>::~CommonResourcePoolManager()
{};

template <typename ImplTraits>
void
CommonResourcePoolManager<ImplTraits>::operator()(TreeType* releasedResource) const
{
	if (releasedResource && m_pool)
		m_pool->reuse(releasedResource);
}

template <class ImplTraits>
CommonTreeStore<ImplTraits>::CommonTreeStore()
	: m_manager(this)
{}

template <class ImplTraits>
typename CommonTreeStore<ImplTraits>::TreeTypePtr
CommonTreeStore<ImplTraits>::create()
{
	if (m_recycleBin.empty())
	{
		TreeTypePtr retval = TreeTypePtr(new TreeType, m_manager);
		m_treeStore.push_back(std::unique_ptr<TreeType>(retval.get()));
		return retval;
	} else {
		TreeType* resource = m_recycleBin.back();
		m_recycleBin.pop_back();
		return TreeTypePtr(resource, m_manager);
	}
}

template <class ImplTraits>
typename CommonTreeStore<ImplTraits>::CommonTokenType*
CommonTreeStore<ImplTraits>::createToken()
{
	CommonTokenType* retval = new CommonTokenType;
	m_tokenStore.push_back(std::unique_ptr<CommonTokenType>(retval));
	return retval;
}

template <class ImplTraits>
typename CommonTreeStore<ImplTraits>::CommonTokenType*
CommonTreeStore<ImplTraits>::createToken( const CommonTokenType* fromToken)
{
	CommonTokenType* retval = new CommonTokenType(*fromToken);
	m_tokenStore.push_back(std::unique_ptr<CommonTokenType>(retval));
	return retval;
}

template <class ImplTraits>
void
CommonTreeStore<ImplTraits>::reuse(TreeType* releasedResource)
{
	if (contains(m_recycleBin, releasedResource))
	{
		throw std::string("Grrr double reuse");
	}
	releasedResource->reuse();
	m_recycleBin.push_back(releasedResource);
}

template<class ImplTraits>
CommonTreeAdaptor<ImplTraits>::CommonTreeAdaptor(DebuggerType*)
{}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr
CommonTreeAdaptor<ImplTraits>::nilNode()
{
	return this->create(NULL);
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr
CommonTreeAdaptor<ImplTraits>::dupTree( const TreeType* tree)
{
	if (tree == NULL)
		return NULL;
	return std::move(this->dupTreeImpl(tree, NULL));
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr
CommonTreeAdaptor<ImplTraits>::dupTree( const TreeTypePtr& tree)
{
	if (tree == NULL)
		return NULL;
	return std::move(dupTreeImpl(tree.get(), NULL));
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr
CommonTreeAdaptor<ImplTraits>::dupTreeImpl( const TreeType *root, TreeType* parent)
{
	TreeTypePtr newTree(dupNode(root));

	// Ensure new subtree root has parent/child index set
	//
	this->setChildIndex( newTree, root->get_childIndex() );
	this->setParent(newTree, parent);

	ChildrenType const& r_children = root->get_children();
	for (auto i = r_children.begin(); i != r_children.end(); ++i)
	{
		// add child's clone
		this->addChild(newTree, dupTreeImpl(i->get(), newTree.get()));
	}

	return	newTree;
}

template<class ImplTraits>
void	CommonTreeAdaptor<ImplTraits>::addChild( TreeTypePtr& t, TreeTypePtr& child)
{
	if	(t != NULL && child != NULL)
	{
		t->addChild(child);
	}
}

template<class ImplTraits>
void	CommonTreeAdaptor<ImplTraits>::addChild( TreeTypePtr& t, TreeTypePtr&& child)
{
	if	(t != NULL && child != NULL)
	{
		t->addChild(child);
	}
}

template<class ImplTraits>
void	CommonTreeAdaptor<ImplTraits>::addChildToken( TreeTypePtr& t, CommonTokenType* child)
{
	if	(t != NULL && child != NULL)
	{
		this->addChild(t, this->create(child));
	}
}

template<class ImplTraits>
void	CommonTreeAdaptor<ImplTraits>::setParent( TreeTypePtr& child, TreeType* parent)
{
	child->set_parent(parent);
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeType*
CommonTreeAdaptor<ImplTraits>::getParent( TreeTypePtr& child)
{
	if ( child==NULL )
		return NULL;
	return child->getParent();
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr
CommonTreeAdaptor<ImplTraits>::errorNode( CommonTokenType*,
											const CommonTokenType*,
											const CommonTokenType*)
{
	// Use the supplied common tree node stream to get another tree from the factory
	// TODO: Look at creating the erronode as in Java, but this is complicated by the
	// need to track and free the memory allocated to it, so for now, we just
	// want something in the tree that isn't a NULL pointer.
	//
	return this->create( CommonTokenType::TOKEN_INVALID, "Tree Error Node");

}

template<class ImplTraits>
bool	CommonTreeAdaptor<ImplTraits>::isNilNode( TreeTypePtr& t)
{
	return t->isNilNode();
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr
CommonTreeAdaptor<ImplTraits>::becomeRoot( TreeTypePtr& newRootTree, TreeTypePtr& oldRootTree)
{
	/* Protect against tree rewrites if we are in some sort of error
	 * state, but have tried to recover. In C we can end up with a null pointer
	 * for a tree that was not produced.
	 */
	if	(newRootTree == NULL)
	{
		return	std::move(oldRootTree);
	}

	/* root is just the new tree as is if there is no
	 * current root tree.
	 */
	if	(oldRootTree == NULL)
	{
		return	std::move(newRootTree);
	}

	/* Produce ^(nil real-node)
	 */
	if	(newRootTree->isNilNode())
	{
		if	(newRootTree->getChildCount() > 1)
		{
			/* TODO: Handle tree exceptions 
			 */
			fprintf(stderr, "More than one node as root! TODO: Create tree exception handling\n");
			return std::move(newRootTree);
		}

		/* The new root is the first child, keep track of the original newRoot
		 * because if it was a Nil Node, then we can reuse it now.
		 */
		TreeTypePtr saveRoot = std::move(newRootTree);
		newRootTree = std::move(saveRoot->getChild(0));

		// Will Reclaim the old nilNode() saveRoot here
	}

	/* Add old root into new root. addChild takes care of the case where oldRoot
	 * is a flat list (nill rooted tree). All children of oldroot are added to
	 * new root.
	 */
	newRootTree->addChild(oldRootTree);

	/* Always returns new root structure
	 */
	return	std::move(newRootTree);
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr
CommonTreeAdaptor<ImplTraits>::becomeRoot( TreeTypePtr&& newRootTree, TreeTypePtr& oldRootTree)
{
	/* Protect against tree rewrites if we are in some sort of error
	 * state, but have tried to recover. In C we can end up with a null pointer
	 * for a tree that was not produced.
	 */
	if	(newRootTree == NULL)
	{
		return	std::move(oldRootTree);
	}

	/* root is just the new tree as is if there is no
	 * current root tree.
	 */
	if	(oldRootTree == NULL)
	{
		return	std::move(newRootTree);
	}

	/* Produce ^(nil real-node)
	 */
	if	(newRootTree->isNilNode())
	{
		if	(newRootTree->getChildCount() > 1)
		{
			/* TODO: Handle tree exceptions
			 */
			fprintf(stderr, "More than one node as root! TODO: Create tree exception handling\n");
			return std::move(newRootTree);
		}

		/* The new root is the first child, keep track of the original newRoot
		 * because if it was a Nil Node, then we can reuse it now.
		 */
		TreeTypePtr saveRoot = std::move(newRootTree);
		newRootTree = std::move(saveRoot->getChild(0));

		// will Reclaim the old nilNode() here saveRoot.
	}

	/* Add old root into new root. addChild takes care of the case where oldRoot
	 * is a flat list (nill rooted tree). All children of oldroot are added to
	 * new root.
	 */
	newRootTree->addChild(oldRootTree);

	/* Always returns new root structure
	 */
	return	std::move(newRootTree);
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr
CommonTreeAdaptor<ImplTraits>::becomeRootToken( CommonTokenType* newRoot, TreeTypePtr& oldRoot)
{
	return	this->becomeRoot(this->create(newRoot), oldRoot);
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr
CommonTreeAdaptor<ImplTraits>::create( CommonTokenType const* payload)
{
	TreeTypePtr retval = TreeStoreType::create();
	retval->set_token(payload);
	return retval;
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr
CommonTreeAdaptor<ImplTraits>::create( ANTLR_UINT32 tokenType, const CommonTokenType* fromToken)
{
	/* Create the new token */
	auto newToken = this->createToken(fromToken);
	/* Set the type of the new token to that supplied */
	newToken->set_type(tokenType);
	/* Return a new node based upon this token */
	return this->create(newToken);
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr
CommonTreeAdaptor<ImplTraits>::create( ANTLR_UINT32 tokenType, const CommonTokenType* fromToken, const char* text)
{
	if (fromToken == NULL)
		return create(tokenType, text);
	/* Create the new token */
	auto newToken = this->createToken(fromToken);
	/* Set the type of the new token to that supplied */
	newToken->set_type(tokenType);
	/* Set the text of the token accordingly */
	newToken->setText(text);
	/* Return a new node based upon this token */
	return	this->create(newToken);
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr
CommonTreeAdaptor<ImplTraits>::create( ANTLR_UINT32 tokenType, const CommonTokenType* fromToken, typename CommonTreeAdaptor<ImplTraits>::StringType const& text)
{
	if (fromToken == NULL)
		return create(tokenType, text);
	/* Create the new token */
	auto newToken = this->createToken(fromToken);
	/* Set the type of the new token to that supplied */
	newToken->set_type(tokenType);
	/* Set the text of the token accordingly */
	newToken->set_tokText(text);
	/* Return a new node based upon this token */
	return	this->create(newToken);
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr
CommonTreeAdaptor<ImplTraits>::create( ANTLR_UINT32 tokenType, const char* text)
{
	auto fromToken = this->createToken(tokenType, text);
	return	this->create(fromToken);
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr
CommonTreeAdaptor<ImplTraits>::create( ANTLR_UINT32 tokenType, typename CommonTreeAdaptor<ImplTraits>::StringType const& text)
{
	auto fromToken = this->createToken(tokenType, text);
	return	this->create(fromToken);
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr
CommonTreeAdaptor<ImplTraits>::dupNode(const TreeType* treeNode)
{
	if (treeNode == NULL)
		return TreeStoreType::null();
	TreeTypePtr retval(TreeStoreType::create());
	treeNode->dupNode(retval.get());
	return retval;
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr
CommonTreeAdaptor<ImplTraits>::dupNode(const TreeTypePtr& treeNode)
{
	if (treeNode == NULL)
		return TreeStoreType::null();
	TreeTypePtr retval(TreeStoreType::create());
	treeNode->dupNode(retval.get());
	return retval;
}

template<class ImplTraits>
ANTLR_UINT32	CommonTreeAdaptor<ImplTraits>::getType( TreeTypePtr& t)
{
	if ( t==NULL)
		return CommonTokenType::TOKEN_INVALID;
	return t->getType();
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::StringType	CommonTreeAdaptor<ImplTraits>::getText( TreeTypePtr& t)
{
	return t->getText();
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr&	    CommonTreeAdaptor<ImplTraits>::getChild( TreeTypePtr& t, ANTLR_UINT32 i)
{
	if ( t==NULL )
		return NULL;
	return t->getChild(i);
}

template<class ImplTraits>
void	CommonTreeAdaptor<ImplTraits>::setChild( TreeTypePtr& t, ANTLR_UINT32 i, TreeTypePtr& child)
{
	t->setChild(i, child);
}

template<class ImplTraits>
void	CommonTreeAdaptor<ImplTraits>::deleteChild( TreeTypePtr& t, ANTLR_UINT32 i)
{
	t->deleteChild(i);
}

template<class ImplTraits>
void	CommonTreeAdaptor<ImplTraits>::setChildIndex( TreeTypePtr& t, ANTLR_INT32 i)
{
	if( t!= NULL)
		t->set_childIndex(i);
}

template<class ImplTraits>
ANTLR_INT32	CommonTreeAdaptor<ImplTraits>::getChildIndex( TreeTypePtr& t)
{
	if ( t==NULL )
		return 0;
	return t->getChildIndex();
}

template<class ImplTraits>
ANTLR_UINT32	CommonTreeAdaptor<ImplTraits>::getChildCount( TreeTypePtr& t)
{
	if ( t==NULL )
		return 0;
	return t->getChildCount();
}

template<class ImplTraits>
ANTLR_UINT64	CommonTreeAdaptor<ImplTraits>::getUniqueID( TreeTypePtr& node )
{
	return	reinterpret_cast<ANTLR_UINT64>(node);
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::CommonTokenType*
CommonTreeAdaptor<ImplTraits>::createToken( ANTLR_UINT32 tokenType, const char* text)
{
	CommonTokenType* newToken = TreeStoreType::createToken();
	newToken->set_tokText( text );
	newToken->set_type(tokenType);
	return newToken;
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::CommonTokenType*
CommonTreeAdaptor<ImplTraits>::createToken( ANTLR_UINT32 tokenType, typename CommonTreeAdaptor<ImplTraits>::StringType const& text)
{
	CommonTokenType* newToken = TreeStoreType::createToken();
	newToken->set_tokText( text );
	newToken->set_type(tokenType);
	return newToken;
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::CommonTokenType*
CommonTreeAdaptor<ImplTraits>::createToken( const CommonTokenType* fromToken)
{
	CommonTokenType* newToken = TreeStoreType::createToken(fromToken);
	return newToken;
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::CommonTokenType*  
CommonTreeAdaptor<ImplTraits>::getToken( TreeTypePtr& t)
{
	return t->getToken();
}

template<class ImplTraits>
void CommonTreeAdaptor<ImplTraits>::setTokenBoundaries( TreeTypePtr& t, const CommonTokenType* startToken, const CommonTokenType* stopToken)
{
	ANTLR_MARKER   start = 0;
	ANTLR_MARKER   stop = 0;

	if	(t == NULL)
		return;

	if	( startToken != NULL)
		start = startToken->get_tokenIndex();

	if	( stopToken != NULL)
		stop = stopToken->get_tokenIndex();

	t->set_startIndex(start);
	t->set_stopIndex(stop);
}

template<class ImplTraits>
ANTLR_MARKER	CommonTreeAdaptor<ImplTraits>::getTokenStartIndex( TreeTypePtr& t)
{
	if ( t==NULL )
		return -1;
	return t->get_tokenStartIndex();
}

template<class ImplTraits>
ANTLR_MARKER	CommonTreeAdaptor<ImplTraits>::getTokenStopIndex( TreeTypePtr& t)
{
	if ( t==NULL )
		return -1;
	return t->get_tokenStopIndex();
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::StringType	 CommonTreeAdaptor<ImplTraits>::makeDot( TreeTypePtr& theTree)
{
	// The string we are building up
	//
	StringType		dotSpec;
	char            buff[64];
	StringType      text;
	
	dotSpec = "digraph {\n\n"
			"\tordering=out;\n"
			"\tranksep=.4;\n"
			"\tbgcolor=\"lightgrey\";  node [shape=box, fixedsize=false, fontsize=12, fontname=\"Helvetica-bold\", fontcolor=\"blue\"\n"
			"\twidth=.25, height=.25, color=\"black\", fillcolor=\"white\", style=\"filled, solid, bold\"];\n\n"
			"\tedge [arrowsize=.5, color=\"black\", style=\"bold\"]\n\n";

    if	(theTree == NULL)
	{
		// No tree, so create a blank spec
		//
		dotSpec->append("n0[label=\"EMPTY TREE\"]\n");
		return dotSpec;
	}

    sprintf(buff, "\tn%p[label=\"", theTree);
	dotSpec.append(buff);
    text = this->getText(theTree);
    for (std::size_t j = 0; j < text.size(); j++)
    {
            switch(text[j])
            {
                case '"':
                    dotSpec.append("\\\"");
                    break;

                case '\n':
                    dotSpec.append("\\n");
                    break;

                case '\r':
                    dotSpec.append("\\r");
                    break;

                default:
                    dotSpec += text[j];
                    break;
            }
    }
	dotSpec->append("\"]\n");

	// First produce the node defintions
	//
	this->defineDotNodes(theTree, dotSpec);
	dotSpec.append("\n");
	this->defineDotEdges(theTree, dotSpec);
	
	// Terminate the spec
	//
	dotSpec.append("\n}");

	// Result
	//
	return dotSpec;
}

template<class ImplTraits>
void CommonTreeAdaptor<ImplTraits>::replaceChildren( TreeTypePtr parent, ANTLR_INT32 startChildIndex, ANTLR_INT32 stopChildIndex, TreeTypePtr t)
{
	if	(parent != NULL)
		parent->replaceChildren(startChildIndex, stopChildIndex, t);
}

template<class ImplTraits>
CommonTreeAdaptor<ImplTraits>::~CommonTreeAdaptor()
{
#ifdef ANTLR3_DEBUG
	std::cout << "SZ" << TreeStoreType::size() << std::endl;
	std::cout << "RZ" << TreeStoreType::m_recycleBin.size() << std::endl;

	auto i = TreeStoreType::m_treeStore.begin();

	std::cout
		<< ' '
		<< "Node    " << '\t' << "Parent  " << '\t' << "Type" << '\t' << "toStringTree" << std::endl;

	for(; i != TreeStoreType::m_treeStore.end(); ++i)
	{
		std::cout
			<< (TreeStoreType::contains(TreeStoreType::m_recycleBin, i->get()) ? '*' : ' ')
			<< i->get() << '\t'
			<< (const void *) (*i)->get_parent() << '\t'
			<< (*i)->getType() << '\t'
			<< (*i)->getChildCount() << '\t'
			<< (*i)->toStringTree() << '\t'
			<< std::endl;
	}
#endif
}

template<class ImplTraits>
void CommonTreeAdaptor<ImplTraits>::defineDotNodes(TreeTypePtr t, const StringType& dotSpec)
{
	// How many nodes are we talking about?
	//
	int	nCount;
	int i;
	TreeTypePtr child;
	char	buff[64];
	StringType	text;
	int	j;

	// Count the nodes
	//
	nCount = this->getChildCount(t);

	if	(nCount == 0)
	{
		// This will already have been included as a child of another node
		// so there is nothing to add.
		//
		return;
	}

	// For each child of the current tree, define a node using the
	// memory address of the node to name it
	//
	for	(i = 0; i<nCount; i++)
	{

		// Pick up a pointer for the child
		//
		child = this->getChild(t, i);

		// Name the node
		//
		sprintf(buff, "\tn%p[label=\"", child);
		dotSpec->append(buff);
		text = this->getText(child);
		for (j = 0; j < text.size(); j++)
		{
            switch(text[j])
            {
                case '"':
                    dotSpec.append("\\\"");
                    break;

                case '\n':
                    dotSpec.append("\\n");
                    break;

                case '\r':
                    dotSpec.append("\\r");
                    break;

                default:
                    dotSpec += text[j];
                    break;
            }
		}
		dotSpec.append("\"]\n");

		// And now define the children of this child (if any)
		//
		this->defineDotNodes(child, dotSpec);
	}
	
	// Done
	//
	return;
}

template<class ImplTraits>
void CommonTreeAdaptor<ImplTraits>::defineDotEdges(TreeTypePtr t, const StringType& dotSpec)
{
	// How many nodes are we talking about?
	//
	int	nCount;
	if	(t == NULL)
	{
		// No tree, so do nothing
		//
		return;
	}

	// Count the nodes
	//
	nCount = this->getChildCount(t);

	if	(nCount == 0)
	{
		// This will already have been included as a child of another node
		// so there is nothing to add.
		//
		return;
	}

	// For each child, define an edge from this parent, then process
	// and children of this child in the same way
	//
	for	(int i=0; i<nCount; i++)
	{
		TreeTypePtr child;
		char	buff[128];
        StringType text;

		// Next child
		//
		child	= this->getChild(t, i);

		// Create the edge relation
		//
		sprintf(buff, "\t\tn%p -> n%p\t\t// ",  t, child);
        
		dotSpec.append(buff);

		// Document the relationship
		//
        text = this->getText(t);
		for (std::size_t j = 0; j < text.size(); j++)
        {
                switch(text[j])
                {
                    case '"':
                        dotSpec.append("\\\"");
                        break;

                    case '\n':
                        dotSpec.append("\\n");
                        break;

                    case '\r':
                        dotSpec.append("\\r");
                        break;

                    default:
                        dotSpec += text[j];
                        break;
                }
        }

        dotSpec.append(" -> ");

        text = this->getText(child);
        for (std::size_t j = 0; j < text.size(); j++)
        {
                switch(text[j])
                {
                    case '"':
                        dotSpec.append("\\\"");
                        break;

                    case '\n':
                        dotSpec.append("\\n");
                        break;

                    case '\r':
                        dotSpec.append("\\r");
                        break;

                    default:
                        dotSpec += text[j];
                        break;
                }
        }
		dotSpec.append("\n");
        
		// Define edges for this child
		//
		this->defineDotEdges(child, dotSpec);
	}

	// Done
	//
	return;
}

template<class ImplTraits>
typename CommonTreeAdaptor<ImplTraits>::TreeTypePtr CommonTreeAdaptor<ImplTraits>::rulePostProcessing( TreeTypePtr& root)
{
	TreeTypePtr saveRoot = std::move(root);

	if (saveRoot != NULL && saveRoot->isNilNode())
	{
		if (saveRoot->getChildCount() == 0)
		{
			return TreeTypePtr(NULL, root.get_deleter());
		}
		else if (saveRoot->getChildCount() == 1)
		{
			TreeTypePtr newRoot = std::move(saveRoot->getChild(0));
			newRoot->set_parent(NULL);
			newRoot->set_childIndex(-1);
			
			// The root we were given was a nil node, with one child, which means it has
			// been abandoned and would be lost in the node factory.
			// saveRoot will be releases and put back into factory
			//
			return newRoot;
		}
	}
	return saveRoot;
}

template<class ImplTraits>
DebugTreeAdaptor<ImplTraits>::DebugTreeAdaptor( DebuggerType* debugger )
{
	m_debugger = debugger;
}

template<class ImplTraits>
void DebugTreeAdaptor<ImplTraits>::setDebugEventListener( DebuggerType* debugger)
{
	m_debugger = debugger;
}

template<class ImplTraits>
typename DebugTreeAdaptor<ImplTraits>::TreeTypePtr	  DebugTreeAdaptor<ImplTraits>::nilNode()
{
	TreeTypePtr	t = this->create(NULL);
	m_debugger->createNode(t);
	return	t;
}

template<class ImplTraits>
void	DebugTreeAdaptor<ImplTraits>::addChild(TreeTypePtr& t, TreeTypePtr& child)
{
	if	(t != NULL && child != NULL)
	{
		t->addChild(child);
		m_debugger->addChild(t, child);
	}
}

template<class ImplTraits>
void	DebugTreeAdaptor<ImplTraits>::addChildToken(TreeTypePtr& t, CommonTokenType* child)
{
	TreeTypePtr	tc;
	if	(t != NULL && child != NULL)
	{
		tc = this->create(child);
		this->addChild(t, tc);
		m_debugger->addChild(t, tc);
	}
}

template<class ImplTraits>
typename DebugTreeAdaptor<ImplTraits>::TreeTypePtr DebugTreeAdaptor<ImplTraits>::becomeRoot( TreeTypePtr& newRootTree, TreeTypePtr& oldRootTree )
{
	TreeTypePtr t = super::becomeRoot(newRootTree, oldRootTree);
	m_debugger->becomeRoot(newRootTree, oldRootTree);
	return t;
}

template<class ImplTraits>
typename DebugTreeAdaptor<ImplTraits>::TreeTypePtr DebugTreeAdaptor<ImplTraits>::becomeRootToken(CommonTokenType* newRoot, TreeTypePtr& oldRoot)
{
	TreeTypePtr	t =	super::becomeRoot(this->create(newRoot), oldRoot);
	m_debugger->becomeRoot(t, oldRoot);
	return t;
}

template<class ImplTraits>
typename DebugTreeAdaptor<ImplTraits>::TreeTypePtr DebugTreeAdaptor<ImplTraits>::createTypeToken(ANTLR_UINT32 tokenType, CommonTokenType* fromToken)
{
	TreeTypePtr t;
	t = this->createTypeToken(tokenType, fromToken);
	m_debugger->createNode(t);
	return t;
}

template<class ImplTraits>
typename DebugTreeAdaptor<ImplTraits>::TreeTypePtr DebugTreeAdaptor<ImplTraits>::createTypeTokenText(ANTLR_UINT32 tokenType, CommonTokenType* fromToken, ANTLR_UINT8* text)
{
	TreeTypePtr t;
	t = this->createTypeTokenText(tokenType, fromToken, text);
	m_debugger->createNode(t);
	return t;
}
	
template<class ImplTraits>
typename DebugTreeAdaptor<ImplTraits>::TreeTypePtr DebugTreeAdaptor<ImplTraits>::createTypeText( ANTLR_UINT32 tokenType, ANTLR_UINT8* text)
{
	TreeTypePtr t;
	t = this->createTypeText(tokenType, text);
	m_debugger->createNode(t);
	return t;
}

template<class ImplTraits>
typename DebugTreeAdaptor<ImplTraits>::TreeTypePtr DebugTreeAdaptor<ImplTraits>::dupTree( const TreeTypePtr& tree)
{
	TreeTypePtr t;

	// Call the normal dup tree mechanism first
	//
	t = this->dupTreeImpl(tree, NULL);

	// In order to tell the debugger what we have just done, we now
	// simulate the tree building mechanism. THis will fire
	// lots of debugging events to the client and look like we
	// duped the tree..
	//
	this->simulateTreeConstruction( t);

	return t;
}

template<class ImplTraits>
typename DebugTreeAdaptor<ImplTraits>::TreeTypePtr DebugTreeAdaptor<ImplTraits>::dupTree( const TreeType* tree)
{
	TreeTypePtr t;

	// Call the normal dup tree mechanism first
	//
	t = this->dupTreeImpl(tree, NULL);

	// In order to tell the debugger what we have just done, we now
	// simulate the tree building mechanism. THis will fire
	// lots of debugging events to the client and look like we
	// duped the tree..
	//
	this->simulateTreeConstruction( t);

	return t;
}

template<class ImplTraits>
void DebugTreeAdaptor<ImplTraits>::simulateTreeConstruction(TreeTypePtr& tree)
{
	ANTLR_UINT32		n;
	ANTLR_UINT32		i;
	TreeTypePtr	child;

	// Send the create node event
	//
	m_debugger->createNode(tree);

	n = this->getChildCount(tree);
	for	(i = 0; i < n; i++)
	{
		child = this->getChild(tree, i);
		this->simulateTreeConstruction(child);
		m_debugger->addChild(tree, child);
	}
}

}
