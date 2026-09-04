/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * DO NOT EDIT, this is an Auto-generated file from:
 * buildscripts/semantic-convention/templates/registry/semantic_attributes-h.j2
 */

#pragma once

#include "opentelemetry/common/macros.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace semconv
{
namespace openai
{

/**
  The type of OpenAI API being used.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"}
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kOpenaiApiType = "openai.api.type";

/**
  The service tier requested. May be a specific tier, default, or auto.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"}
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kOpenaiRequestServiceTier =
    "openai.request.service_tier";

/**
  The service tier used for the response.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"}
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kOpenaiResponseServiceTier =
    "openai.response.service_tier";

/**
  A fingerprint to track any eventual change in the Generative AI environment.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"}
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kOpenaiResponseSystemFingerprint =
    "openai.response.system_fingerprint";

namespace OpenaiApiTypeValues
{
/**
  The OpenAI <a href="https://developers.openai.com/api/reference/chat-completions/overview">Chat
  Completions API</a>.
 */
static constexpr const char *kChatCompletions = "chat_completions";

/**
  The OpenAI <a href="https://developers.openai.com/api/reference/responses/overview">Responses
  API</a>.
 */
static constexpr const char *kResponses = "responses";

}  // namespace OpenaiApiTypeValues

namespace OpenaiRequestServiceTierValues
{
/**
  The system will utilize scale tier credits until they are exhausted.
 */
static constexpr const char *kAuto = "auto";

/**
  The system will utilize the default scale tier.
 */
static constexpr const char *kDefault = "default";

}  // namespace OpenaiRequestServiceTierValues

}  // namespace openai
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
