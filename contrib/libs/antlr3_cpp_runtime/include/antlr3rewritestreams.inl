namespace antlr3 {

template<class ImplTraits, class ElementType>
RewriteRuleElementStream<ImplTraits, ElementType>::RewriteRuleElementStream(TreeAdaptorType* adaptor,
	const char* description)
{
	this->init(adaptor, description);
}

template<class ImplTraits, class ElementType>
RewriteRuleElementStream<ImplTraits, ElementType>::RewriteRuleElementStream(TreeAdaptorType* adaptor,
	const char* description,
	const ElementType* oneElement)
{
	this->init(adaptor, description);
	if( oneElement != NULL )
		this->add( oneElement );
}

template<class ImplTraits, class ElementType>
RewriteRuleElementStream<ImplTraits, ElementType>::RewriteRuleElementStream(TreeAdaptorType* adaptor,
	const char* description,
	const ElementsType& elements)
	: m_elements(elements)
{
	this->init(adaptor, description);
}

template<class ImplTraits, class ElementType>
void RewriteRuleElementStream<ImplTraits, ElementType>::init(TreeAdaptorType* adaptor,
	const char* description)
{
	m_adaptor = adaptor;
	m_cursor  = 0;
	m_dirty	  = false;
}

template<class ImplTraits>
RewriteRuleTokenStream<ImplTraits>::RewriteRuleTokenStream(TreeAdaptorType* adaptor,
	const char* description)
	//: BaseType(adaptor, description)
{
}

template<class ImplTraits>
RewriteRuleTokenStream<ImplTraits>::RewriteRuleTokenStream(TreeAdaptorType* adaptor,
	const char* description,
	const TokenType* oneElement)
	//: BaseType(adaptor, description, oneElement)
{
}

template<class ImplTraits>
RewriteRuleTokenStream<ImplTraits>::RewriteRuleTokenStream(TreeAdaptorType* adaptor,
	const char* description,
	const ElementsType& elements)
	//: BaseType(adaptor, description, elements)
{
}

template<class ImplTraits>
RewriteRuleSubtreeStream<ImplTraits>::RewriteRuleSubtreeStream(TreeAdaptorType* adaptor,
	const char* description)
	//: BaseType(adaptor, description)
{
}

template<class ImplTraits>
RewriteRuleSubtreeStream<ImplTraits>::RewriteRuleSubtreeStream(TreeAdaptorType* adaptor,
	const char* description,
	TreeTypePtr& oneElement)
	//: BaseType(adaptor, description, oneElement)
{
}

template<class ImplTraits>
RewriteRuleSubtreeStream<ImplTraits>::RewriteRuleSubtreeStream(TreeAdaptorType* adaptor,
	const char* description,
	const ElementsType& elements)
	//: BaseType(adaptor, description, elements)
{
}

/*
template<class ImplTraits>
RewriteRuleNodeStream<ImplTraits>::RewriteRuleNodeStream(TreeAdaptorType* adaptor,
	const char* description)
	: BaseType(adaptor, description)
{
}

template<class ImplTraits>
RewriteRuleNodeStream<ImplTraits>::RewriteRuleNodeStream(TreeAdaptorType* adaptor,
	const char* description,
	TokenType* oneElement)
	: BaseType(adaptor, description, oneElement)
{
}

template<class ImplTraits>
RewriteRuleNodeStream<ImplTraits>::RewriteRuleNodeStream(TreeAdaptorType* adaptor,
	const char* description,
	const ElementsType& elements)
	: BaseType(adaptor, description, elements)
{
}
*/

template<class ImplTraits, class ElementType>
void RewriteRuleElementStream<ImplTraits, ElementType>::reset()
{
	m_cursor = 0;
	m_dirty = true;
}

template<class ImplTraits, class ElementType>
void RewriteRuleElementStream<ImplTraits, ElementType>::add(ElementType* el)
{
	if ( el== NULL )
		return;

	m_elements.push_back(el);
}

template<class ImplTraits, class ElementType>
ElementType* RewriteRuleElementStream<ImplTraits, ElementType>::_next()
{
	ANTLR_UINT32 n = this->size();

	if (n == 0)
	{
		// This means that the stream is empty
		return NULL;	// Caller must cope with this (TODO throw RewriteEmptyStreamException)
	}

	// Traversed all the available elements already?
	if ( m_cursor >= n) // out of elements?
	{
		if (n == 1)
		{
			// Special case when size is single element, it will just dup a lot
			//return this->toTree(m_singleElement);
			return this->toTree(m_elements.at(0));
		}

		// Out of elements and the size is not 1, so we cannot assume
		// that we just duplicate the entry n times (such as ID ent+ -> ^(ID ent)+)
		// This means we ran out of elements earlier than was expected.
		//
		return NULL;	// Caller must cope with this (TODO throw RewriteEmptyStreamException)
	}

	// More than just a single element so we extract it from the
	// vector.
	ElementType* t = this->toTree(m_elements.at(m_cursor));
	m_cursor++;
	return t;
}

template<class ImplTraits, class ElementType>
ElementType
RewriteRuleElementStream<ImplTraits, ElementType>::nextTree()
{
	ANTLR_UINT32 n = this->size();
	if ( m_dirty || ( (m_cursor >=n) && (n==1)) )
	{
		// if out of elements and size is 1, dup
		ElementType* el = this->_next();
		return this->dup(el);
	}

	// test size above then fetch
	ElementType*  el = this->_next();
	return el;
}

/*
template<class ImplTraits, class SuperType>
typename RewriteRuleElementStream<ImplTraits, SuperType>::TokenType*
RewriteRuleElementStream<ImplTraits, SuperType>::nextToken()
{
	return this->_next();
}

template<class ImplTraits, class SuperType>
typename RewriteRuleElementStream<ImplTraits, SuperType>::TokenType*
RewriteRuleElementStream<ImplTraits, SuperType>::next()
{
	ANTLR_UINT32   s;
	s = this->size();
	if ( (m_cursor >= s) && (s == 1) )
	{
		TreeTypePtr el;
		el = this->_next();
		return	this->dup(el);
	}
	return this->_next();
}

*/

template<class ImplTraits, class ElementType>
ElementType*
RewriteRuleElementStream<ImplTraits, ElementType>::dup( ElementType* element)
{
	return dupImpl(element);
}

template<class ImplTraits, class ElementType>
ElementType*
RewriteRuleElementStream<ImplTraits, ElementType>::dupImpl( typename ImplTraits::CommonTokenType* element)
{
	return NULL; // TODO throw here
}

template<class ImplTraits, class ElementType>
ElementType*
RewriteRuleElementStream<ImplTraits, ElementType>::dupImpl( typename ImplTraits::TreeTypePtr element)
{
	return m_adaptor->dupTree(element);
}

template<class ImplTraits>
typename RewriteRuleSubtreeStream<ImplTraits>::TreeTypePtr	
RewriteRuleSubtreeStream<ImplTraits>::dup(TreeTypePtr element)
{
	return this->dupTree(element);
}

template<class ImplTraits>
typename RewriteRuleSubtreeStream<ImplTraits>::TreeTypePtr
RewriteRuleSubtreeStream<ImplTraits>::dupTree(TreeTypePtr element)
{
	return BaseType::m_adaptor->dupNode(element);
}

template<class ImplTraits, class ElementType>
ElementType*
RewriteRuleElementStream<ImplTraits, ElementType>::toTree( ElementType* element)
{
	return element;
}

/*
template<class ImplTraits>
typename RewriteRuleNodeStream<ImplTraits>::TreeTypePtr
RewriteRuleNodeStream<ImplTraits>::toTree(TreeTypePtr element)
{
	return this->toTreeNode(element);
}

template<class ImplTraits>
typename RewriteRuleNodeStream<ImplTraits>::TreeTypePtr
RewriteRuleNodeStream<ImplTraits>::toTreeNode(TreeTypePtr element)
{
	return BaseType::m_adaptor->dupNode(element);
}
*/

template<class ImplTraits, class ElementType>
bool RewriteRuleElementStream<ImplTraits, ElementType>::hasNext()
{
	if ( !m_elements.empty() && m_cursor < m_elements.size())
	{
		return true;
	}
	else
	{
		return false;
	}
}

template<class ImplTraits >
typename RewriteRuleTokenStream<ImplTraits>::TreeTypePtr
RewriteRuleTokenStream<ImplTraits>::nextNode()
{
	TokenType *Token = this->nextToken();
	//return BaseType::m_adaptor->create(Token);
	return m_adaptor->create(Token);
}

/*
template<class ImplTraits>
typename RewriteRuleTokenStream<ImplTraits>::TreeTypePtr
RewriteRuleTokenStream<ImplTraits>::nextNodeToken()
{
	return BaseType::m_adaptor->create(this->_next());
}
*/

/// Number of elements available in the stream
///
template<class ImplTraits, class ElementType>
ANTLR_UINT32 RewriteRuleElementStream<ImplTraits, ElementType>::size()
{
	return (ANTLR_UINT32)(m_elements.size());
}

template<class ImplTraits, class ElementType>
typename RewriteRuleElementStream<ImplTraits, ElementType>::StringType
RewriteRuleElementStream<ImplTraits, ElementType>::getDescription()
{
	if ( m_elementDescription.empty() )
	{
		m_elementDescription = "<unknown source>";
	}
	return  m_elementDescription;
}

template<class ImplTraits, class ElementType>
RewriteRuleElementStream<ImplTraits, ElementType>::~RewriteRuleElementStream()
{
    // Before placing the stream back in the pool, we
	// need to clear any vector it has. This is so any
	// free pointers that are associated with the
	// entries are called. However, if this particular function is called
    // then we know that the entries in the stream are definitely
    // tree nodes. Hence we check to see if any of them were nilNodes as
    // if they were, we can reuse them.
	//
	// We have some elements to traverse
	//
	for (ANTLR_UINT32 i = 0; i < m_elements.size(); i++)
	{
		ElementType *tree = m_elements.at(i);
		//if  ( (tree != NULL) && tree->isNilNode() )
		{
			// Had to remove this for now, check is not comprehensive enough
			// tree->reuse(tree);
		}
	}
	m_elements.clear();
}

template<class ImplTraits>
typename RewriteRuleTokenStream<ImplTraits>::TokenType*
RewriteRuleTokenStream<ImplTraits>::nextToken()
{
	return this->_next();
}

template<class ImplTraits>
typename RewriteRuleSubtreeStream<ImplTraits>::TreeTypePtr
RewriteRuleSubtreeStream<ImplTraits>::nextNode(TreeTypePtr element)
{
	//System.out.println("nextNode: elements="+elements+", singleElement="+((Tree)singleElement).toStringTree());
	ANTLR_UINT32 n = this->size();
	if ( BaseType::m_dirty || (BaseType::m_cursor>=n && n==1) ) {
		// if out of elements and size is 1, dup (at most a single node
		// since this is for making root nodes).
		TreeTypePtr el = this->_next();
		return BaseType::m_adaptor->dupNode(el);
	}
	// test size above then fetch
	TreeType *tree = this->_next();
	while (BaseType::m_adaptor.isNil(tree) && BaseType::m_adaptor.getChildCount(tree) == 1)
		tree = BaseType::m_adaptor->getChild(tree, 0);
	//System.out.println("_next="+((Tree)tree).toStringTree());
	TreeType *el = BaseType::m_adaptor->dupNode(tree); // dup just the root (want node here)
	return el;
}

}
