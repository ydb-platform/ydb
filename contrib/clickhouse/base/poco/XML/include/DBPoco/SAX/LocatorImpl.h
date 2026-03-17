//
// LocatorImpl.h
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// An implementation of the SAX Locator interface.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_SAX_LocatorImpl_INCLUDED
#define DB_SAX_LocatorImpl_INCLUDED


#include "DBPoco/SAX/Locator.h"
#include "DBPoco/XML/XML.h"
#include "DBPoco/XML/XMLString.h"


namespace DBPoco
{
namespace XML
{


    class XML_API LocatorImpl : public Locator
    /// Provide an optional convenience implementation of Locator.
    {
    public:
        LocatorImpl();
        /// Zero-argument constructor.
        ///
        /// This will not normally be useful, since the main purpose of this class is
        /// to make a snapshot of an existing Locator.

        LocatorImpl(const Locator & loc);
        /// Copy constructor.
        ///
        /// Create a persistent copy of the current state of a locator. When the original
        /// locator changes, this copy will still keep the original values (and it can be
        /// used outside the scope of DocumentHandler methods).

        ~LocatorImpl();
        /// Destroys the Locator.

        LocatorImpl & operator=(const Locator & loc);
        /// Assignment operator.

        XMLString getPublicId() const;
        /// Return the saved public identifier.

        XMLString getSystemId() const;
        /// Return the saved system identifier.

        int getLineNumber() const;
        /// Return the saved line number (1-based).

        int getColumnNumber() const;
        /// Return the saved column number (1-based).

        void setPublicId(const XMLString & publicId);
        /// Set the public identifier for this locator.

        void setSystemId(const XMLString & systemId);
        /// Set the system identifier for this locator.

        void setLineNumber(int lineNumber);
        /// Set the line number for this locator (1-based).

        void setColumnNumber(int columnNumber);
        /// Set the column number for this locator (1-based).

    private:
        XMLString _publicId;
        XMLString _systemId;
        int _lineNumber;
        int _columnNumber;
    };


}
} // namespace DBPoco::XML


#endif // DB_SAX_LocatorImpl_INCLUDED
