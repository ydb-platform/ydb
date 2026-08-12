/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * DO NOT EDIT, this is an Auto-generated file from:
 * buildscripts/semantic-convention/templates/registry/semantic_events-h.j2
 */

#pragma once

#include "opentelemetry/common/macros.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace semconv
{
namespace rpc
{

/**
  This event represents an exception that occurred during an outgoing RPC call, such as network
  failures, timeouts, serialization errors, or other errors that prevent the call from completing
  successfully. <p> This event SHOULD be recorded when an exception occurs during RPC client call
  operations. Instrumentations SHOULD set the severity to WARN (severity number 13) when recording
  this event. Instrumentations MAY provide a configuration option to populate exception events with
  the attributes captured on the corresponding RPC client span.
 */
static constexpr const char *kRpcClientCallException = "rpc.client.call.exception";

/**
  Describes a message sent or received within the context of an RPC call.

  @deprecated
  {"note": "Deprecated, no replacement at this time.", "reason": "obsoleted"}
  <p>
  In the lifetime of an RPC stream, an event for each message sent/received on client and server
  spans SHOULD be created. In case of unary calls message events SHOULD NOT be recorded.
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kRpcMessage = "rpc.message";

/**
  This event represents an exception that occurred during incoming RPC call processing, such as
  application errors, internal failures, or other exceptions that prevent the server from
  successfully handling the call. <p> This event SHOULD be recorded when an exception occurs during
  RPC server call processing. Instrumentations SHOULD set the severity to ERROR (severity number 17)
  when recording this event. Instrumentations MAY provide a configuration option to populate
  exception events with the attributes captured on the corresponding RPC server span.
 */
static constexpr const char *kRpcServerCallException = "rpc.server.call.exception";

}  // namespace rpc
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
