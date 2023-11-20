#pragma once

namespace NYql {

    struct TYtEvents {
        enum {
            ES_START_OPERATION = EventSpaceBegin(NActors::TEvents::EEventSpace::ES_USERSPACE) + 10000,
            ES_START_OPERATION_RESPONSE,
            ES_GET_OPERATION,
            ES_GET_OPERATION_RESPONSE,
            ES_LIST_OPERATIONS,
            ES_LIST_OPERATIONS_RESPONSE,
            ES_GET_JOB,
            ES_GET_JOB_RESPONSE,
            ES_WRITE_FILE,
            ES_WRITE_FILE_RESPONSE,
            ES_READ_FILE,
            ES_READ_FILE_RESPONSE,
            ES_LIST_NODE,
            ES_LIST_NODE_RESPONSE,
            ES_CREATE_NODE,
            ES_CREATE_NODE_RESPONSE,
            ES_SET_NODE,
            ES_SET_NODE_RESPONSE,
            ES_GET_NODE,
            ES_GET_NODE_RESPONSE,
            ES_REMOVE_NODE,
            ES_REMOVE_NODE_RESPONSE,
            ES_START_TRANSACTION,
            ES_START_TRANSACTION_RESPONSE,

            ES_PRINT_JOB_STDERR
        };
    };
} // namespace NYql
