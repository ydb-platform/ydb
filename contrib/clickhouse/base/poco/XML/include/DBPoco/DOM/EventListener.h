//
// EventListener.h
//
// Library: XML
// Package: DOM
// Module:  DOMEvents
//
// Definition of the DOM EventListener interface.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_DOM_EventListener_INCLUDED
#define DB_DOM_EventListener_INCLUDED


#include "DBPoco/XML/XML.h"
#include "DBPoco/XML/XMLString.h"


namespace DBPoco
{
namespace XML
{


    class Event;


    class XML_API EventListener
    /// The EventListener interface is the primary method for handling events. Users
    /// implement the EventListener interface and register their listener on an
    /// EventTarget using the AddEventListener method. The users should also remove
    /// their EventListener from its EventTarget after they have completed using
    /// the listener.
    ///
    /// When a Node is copied using the cloneNode method the EventListeners attached
    /// to the source Node are not attached to the copied Node. If the user wishes
    /// the same EventListeners to be added to the newly created copy the user must
    /// add them manually.
    {
    public:
        virtual void handleEvent(Event * evt) = 0;
        /// This method is called whenever an event occurs of the
        /// type for which the EventListener interface was registered.

    protected:
        virtual ~EventListener();
    };


}
} // namespace DBPoco::XML


#endif // DB_DOM_EventListener_INCLUDED
