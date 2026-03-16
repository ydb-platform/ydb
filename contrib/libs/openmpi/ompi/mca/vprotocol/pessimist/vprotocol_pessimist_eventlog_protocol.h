/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef __VPROTOCOL_PESSIMIST_EVENTLOG_PROTOCOL_H__
#define __VPROTOCOL_PESSIMIST_EVENTLOG_PROTOCOL_H__

#include "vprotocol_pessimist_event.h"

BEGIN_C_DECLS

/** Enum containing the command tags to remotely control event loggers
 */
typedef enum {
    VPROTOCOL_PESSIMIST_EVENTLOG_CLOSE_SERVER_CMD,
    VPROTOCOL_PESSIMIST_EVENTLOG_SAVE_SERVER_CMD,
    VPROTOCOL_PESSIMIST_EVENTLOG_LOAD_SERVER_CMD,

    VPROTOCOL_PESSIMIST_EVENTLOG_NEW_CLIENT_CMD,
    VPROTOCOL_PESSIMIST_EVENTLOG_QUIT_CLIENT_CMD,

    VPROTOCOL_PESSIMIST_EVENTLOG_PUT_EVENTS_CMD,
    VPROTOCOL_PESSIMIST_EVENTLOG_GET_EVENTS_CMD,
    VPROTOCOL_PESSIMIST_EVENTLOG_DEL_EVENTS_CMD,
    VPROTOCOL_PESSIMIST_EVENTLOG_ACK
} vprotocol_pessimist_event_logger_command_t;

#define VPROTOCOL_EVENT_LOGGER_NAME_FMT "ompi_ft_event_logger[%d]"

static inline void vprotocol_pessimist_event_datatype_create(
                                                    MPI_Datatype *event_dtt)
{
    MPI_Type_contiguous(2, MPI_UNSIGNED_LONG_LONG, event_dtt);
    MPI_Type_commit(event_dtt);
}

END_C_DECLS

#endif /* __VPROTOCOL_PESSIMIST_EVENTLOG_PROTOCOL_H__ */
