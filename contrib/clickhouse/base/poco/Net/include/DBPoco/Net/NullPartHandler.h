//
// NullPartHandler.h
//
// Library: Net
// Package: Messages
// Module:  NullPartHandler
//
// Definition of the NullPartHandler class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Net_NullPartHandler_INCLUDED
#define DB_Net_NullPartHandler_INCLUDED


#include "DBPoco/Net/Net.h"
#include "DBPoco/Net/PartHandler.h"


namespace DBPoco
{
namespace Net
{


    class Net_API NullPartHandler : public PartHandler
    /// A very special PartHandler that simply discards all data.
    {
    public:
        NullPartHandler();
        /// Creates the NullPartHandler.

        ~NullPartHandler();
        /// Destroys the NullPartHandler.

        void handlePart(const MessageHeader & header, std::istream & stream);
        /// Reads and discards all data from the stream.
    };


}
} // namespace DBPoco::Net


#endif // DB_Net_NullPartHandler_INCLUDED
