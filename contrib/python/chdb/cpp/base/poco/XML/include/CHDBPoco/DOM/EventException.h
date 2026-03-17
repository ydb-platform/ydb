//
// EventException.h
//
// Library: XML
// Package: DOM
// Module:  DOMEvents
//
// Definition of the DOM EventException class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_DOM_EventException_INCLUDED
#define CHDB_DOM_EventException_INCLUDED


#include "CHDBPoco/XML/XML.h"
#include "CHDBPoco/XML/XMLException.h"


namespace CHDBPoco
{
namespace XML
{


    class XML_API EventException : public XMLException
    /// Event operations may throw an EventException as
    /// specified in their method descriptions.
    {
    public:
        enum
        {
            UNSPECIFIED_EVENT_TYPE_ERR = 0 /// If the Event's type was not specified by initializing the
            /// event before the method was called. Specification of the Event's
            /// type as null or an empty string will also trigger this exception.
        };

        EventException(int code);
        /// Creates an EventException with the given error code.

        EventException(const EventException & exc);
        /// Creates an EventException by copying another one.

        ~EventException() noexcept;
        /// Destroys the EventException.

        EventException & operator=(const EventException & exc);

        const char * name() const noexcept;
        /// Returns a static string describing the exception.

        const char * className() const noexcept;
        /// Returns the name of the exception class.

        unsigned short code() const;
        /// Returns the Event exception code.

    protected:
        CHDBPoco::Exception * clone() const;

    private:
        EventException();
    };


    //
    // inlines
    //
    inline unsigned short EventException::code() const
    {
        return UNSPECIFIED_EVENT_TYPE_ERR;
    }


}
} // namespace CHDBPoco::XML


#endif // CHDB_DOM_EventException_INCLUDED
