//
// ChildNodesList.cpp
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/DOM/ChildNodesList.h"
#include "DBPoco/DOM/Node.h"
#include "DBPoco/DOM/Document.h"


namespace DBPoco {
namespace XML {


ChildNodesList::ChildNodesList(const Node* pParent):
	_pParent(pParent)
{
	DB_poco_check_ptr (pParent);

	_pParent->duplicate();
}


ChildNodesList::~ChildNodesList()
{
	_pParent->release();
}


Node* ChildNodesList::item(unsigned long index) const
{
	unsigned long n = 0;
	Node* pCur = _pParent->firstChild();
	while (pCur && n++ < index)
	{
		pCur = pCur->nextSibling();
	}
	return pCur;
}


unsigned long ChildNodesList::length() const
{
	unsigned long n = 0;
	Node* pCur = _pParent->firstChild();
	while (pCur)
	{
		++n;
		pCur = pCur->nextSibling();
	}
	return n;
}


void ChildNodesList::autoRelease()
{
	_pParent->ownerDocument()->autoReleasePool().add(this);
}


} } // namespace DBPoco::XML
