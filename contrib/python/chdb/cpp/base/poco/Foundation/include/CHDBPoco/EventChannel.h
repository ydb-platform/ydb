//
// EventChannel.h
//
// Library: Foundation
// Package: Logging
// Module:  EventChannel
//
// Definition of the EventChannel class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Foundation_EventChannel_INCLUDED
#define CHDB_Foundation_EventChannel_INCLUDED


#include "CHDBPoco/BasicEvent.h"
#include "CHDBPoco/Channel.h"
#include "CHDBPoco/Foundation.h"
#include "CHDBPoco/Message.h"


namespace CHDBPoco
{


class Foundation_API EventChannel : public Channel
/// The EventChannel fires the messageLogged event for every log message
/// received. This can be used to hook custom log message processing into
/// the logging framework.
{
public:
    CHDBPoco::BasicEvent<const Message> messageLogged;
    /// Fired when a message is logged by calling the log() method.

    EventChannel();
    /// Creates the EventChannel.

    void log(const Message & msg);
    /// Fires the messageLogged event.

protected:
    ~EventChannel();
    /// Destroys the EventChannel.
};


} // namespace CHDBPoco


#endif // CHDB_Foundation_EventChannel_INCLUDED
