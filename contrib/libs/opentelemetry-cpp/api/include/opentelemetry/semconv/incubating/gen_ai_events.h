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
namespace gen_ai
{

/**
  This event describes the assistant message passed to GenAI system.

  @deprecated
  {"note": "Chat history is reported on @code gen_ai.input.messages @endcode attribute on spans or
  @code gen_ai.client.inference.operation.details @endcode event.\n", "reason": "uncategorized"}
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kGenAiAssistantMessage =
    "gen_ai.assistant.message";

/**
  This event describes the Gen AI response message.

  @deprecated
  {"note": "Chat history is reported on @code gen_ai.output.messages @endcode attribute on spans or
  @code gen_ai.client.inference.operation.details @endcode event.\n", "reason": "uncategorized"}
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kGenAiChoice = "gen_ai.choice";

/**
  Describes the details of a GenAI completion request including chat history and parameters.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"} <p> This event is opt-in and could be
  used to store input and output details independently from traces.
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kGenAiClientInferenceOperationDetails =
    "gen_ai.client.inference.operation.details";

/**
  This event represents an exception that occurred during a Generative AI client operation, such as
  API errors, rate limiting, model errors, timeouts, or other errors that prevent the operation from
  completing successfully.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"} <p> This event SHOULD be recorded when
  an exception occurs during Generative AI client operations. Instrumentations SHOULD set the
  severity to WARN (severity number 13) when recording this event. Instrumentations MAY provide a
  configuration option to populate exception events with the attributes captured on the
  corresponding Generative AI client span.
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kGenAiClientOperationException =
    "gen_ai.client.operation.exception";

/**
  This event captures the result of evaluating GenAI output for quality, accuracy, or other
  characteristics. This event SHOULD be parented to GenAI operation span being evaluated when
  possible or set @code gen_ai.response.id @endcode when span id is not available.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"}
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kGenAiEvaluationResult =
    "gen_ai.evaluation.result";

/**
  This event describes the system instructions passed to the GenAI model.

  @deprecated
  {"note": "Chat history is reported on @code gen_ai.system_instructions @endcode attribute on spans
  or @code gen_ai.client.inference.operation.details @endcode event.\n", "reason": "uncategorized"}
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kGenAiSystemMessage = "gen_ai.system.message";

/**
  This event describes the response from a tool or function call passed to the GenAI model.

  @deprecated
  {"note": "Chat history is reported on @code gen_ai.input.messages @endcode attribute on spans or
  @code gen_ai.client.inference.operation.details @endcode event.\n", "reason": "uncategorized"}
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kGenAiToolMessage = "gen_ai.tool.message";

/**
  This event describes the user message passed to the GenAI model.

  @deprecated
  {"note": "Chat history is reported on @code gen_ai.input.messages @endcode attribute on spans or
  @code gen_ai.client.inference.operation.details @endcode event.\n", "reason": "uncategorized"}
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kGenAiUserMessage = "gen_ai.user.message";

}  // namespace gen_ai
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
