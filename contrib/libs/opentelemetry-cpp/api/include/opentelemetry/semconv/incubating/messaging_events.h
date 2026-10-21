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
namespace messaging
{

/**
  This event represents an exception that occurred during a messaging create operation.
  <p>
  This event SHOULD be recorded when an exception occurs during a messaging create operation.
  Instrumentations SHOULD set the severity to WARN (severity number 13) when recording this event.
  Instrumentations MAY provide a configuration option to populate exception events with the
  attributes captured on the corresponding messaging create span.
 */
static constexpr const char *kMessagingCreateException = "messaging.create.exception";

/**
  This event represents an exception that occurred during messaging process operations.
  <p>
  This event SHOULD be recorded when an exception occurs during messaging process operations.
  Instrumentations SHOULD set the severity to ERROR (severity number 17) when recording this event.
  Instrumentations MAY provide a configuration option to populate exception events with the
  attributes captured on the corresponding messaging process span.
 */
static constexpr const char *kMessagingProcessException = "messaging.process.exception";

/**
  This event represents an exception that occurred during a messaging receive operation.
  <p>
  This event SHOULD be recorded when an exception occurs during a messaging receive operation.
  Instrumentations SHOULD set the severity to WARN (severity number 13) when recording this event.
  Instrumentations MAY provide a configuration option to populate exception events with the
  attributes captured on the corresponding messaging receive span.
 */
static constexpr const char *kMessagingReceiveException = "messaging.receive.exception";

/**
  This event represents an exception that occurred during a messaging send operation.
  <p>
  This event SHOULD be recorded when an exception occurs during a messaging send operation.
  Instrumentations SHOULD set the severity to WARN (severity number 13) when recording this event.
  Instrumentations MAY provide a configuration option to populate exception events with the
  attributes captured on the corresponding messaging send span.
 */
static constexpr const char *kMessagingSendException = "messaging.send.exception";

/**
  This event represents an exception that occurred during a messaging settle operation.
  <p>
  This event SHOULD be recorded when an exception occurs during a messaging settle operation.
  Instrumentations SHOULD set the severity to WARN (severity number 13) when recording this event.
  Instrumentations MAY provide a configuration option to populate exception events with the
  attributes captured on the corresponding messaging settle span.
 */
static constexpr const char *kMessagingSettleException = "messaging.settle.exception";

}  // namespace messaging
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
