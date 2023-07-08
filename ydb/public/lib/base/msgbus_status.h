#pragma once
#include "defs.h"

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>
#include <util/generic/string.h>

namespace NKikimr {
namespace NMsgBusProxy {

#define RESPONSE_STATUS_MAP(XX) \
    XX(MSTATUS_UNKNOWN, 0) \
    XX(MSTATUS_OK, 1) \
    XX(MSTATUS_ERROR, 128) \
    XX(MSTATUS_INPROGRESS, 129) \
    XX(MSTATUS_TIMEOUT, 130) \
    XX(MSTATUS_NOTREADY, 131) \
    XX(MSTATUS_ABORTED, 132) \
    XX(MSTATUS_INTERNALERROR, 133) \
    XX(MSTATUS_REJECTED, 134)

enum EResponseStatus {
    RESPONSE_STATUS_MAP(ENUM_VALUE_GEN)
};

ENUM_TO_STRING(EResponseStatus, RESPONSE_STATUS_MAP)

void ExplainProposeTransactionStatus(ui32 status, TString& name, TString& description);
void ExplainExecutionEngineStatus(ui32 status, TString& name, TString& description);
void ExplainResponseStatus(ui32 status, TString& name, TString& description);

}}
