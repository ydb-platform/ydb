//
// ChildNodesList.h
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the ChildNodesList class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_DOM_ChildNodesList_INCLUDED
#define DB_DOM_ChildNodesList_INCLUDED


#include "DBPoco/DOM/NodeList.h"
#include "DBPoco/XML/XML.h"


namespace DBPoco
{
namespace XML
{


    class XML_API ChildNodesList : public NodeList
    // This implementation of NodeList is returned
    // by Node::getChildNodes().
    {
    public:
        Node * item(unsigned long index) const;
        unsigned long length() const;

        void autoRelease();

    protected:
        ChildNodesList(const Node * pParent);
        ~ChildNodesList();

    private:
        ChildNodesList();

        const Node * _pParent;

        friend class AbstractNode;
    };


}
} // namespace DBPoco::XML


#endif // DB_DOM_ChildNodesList_INCLUDED
