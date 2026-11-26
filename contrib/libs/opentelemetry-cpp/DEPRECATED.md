# Deprecated

This document lists all the items currently deprecated in opentelemetry-cpp.

Deprecated items will be removed in the future.

## Guidelines

### Maintainer guidelines

See the [deprecation-process](./docs/deprecation-process.md)

## [TEMPLATE]

### New Deprecation Title (Template)

#### Announcement (Template)

#### Motivation (Template)

#### Scope (Template)

#### Mitigation (Template)

#### Planned removal (Template)

## [Platforms]

N/A

## [Compilers]

N/A

## [Third party dependencies]

N/A

## [Build tools]

N/A

## [Build scripts]

### Legacy CMake option names

#### Announcement (legacy CMake option names)

* Version: `TO-BE-RELEASED-VERSION`
* Date: `TO-BE-RELEASED-DATE`
* PR: [#4268](https://github.com/open-telemetry/opentelemetry-cpp/pull/4268)

#### Motivation (legacy CMake option names)

Generic CMake option names can collide with variables owned by applications,
parent projects, and third-party dependencies. The `OTELCPP_` prefix identifies
options owned by opentelemetry-cpp.

#### Scope (legacy CMake option names)

The following legacy CMake option names are deprecated:

| Legacy name | Replacement |
| --- | --- |
| `BUILD_PACKAGE` | `OTELCPP_BUILD_PACKAGE` |
| `BUILD_W3CTRACECONTEXT_TEST` | `OTELCPP_BUILD_W3CTRACECONTEXT_TEST` |
| `OPENTELEMETRY_BUILD_DLL` | `OTELCPP_BUILD_DLL` |
| `OPENTELEMETRY_EXTERNAL_COMPONENT_PATH` | `OTELCPP_EXTERNAL_COMPONENT_PATH` |
| `OPENTELEMETRY_INSTALL` | `OTELCPP_INSTALL` |
| `OPENTELEMETRY_SKIP_DYNAMIC_LOADING_TESTS` | `OTELCPP_SKIP_DYNAMIC_LOADING_TESTS` |
| `TARBALL` | `OTELCPP_TARBALL` |
| `WITH_ABI_VERSION_1` | `OTELCPP_WITH_ABI_VERSION_1` |
| `WITH_ABI_VERSION_2` | `OTELCPP_WITH_ABI_VERSION_2` |
| `WITH_API_ONLY` | `OTELCPP_WITH_API_ONLY` |
| `WITH_ASYNC_EXPORT_PREVIEW` | `OTELCPP_WITH_ASYNC_EXPORT_PREVIEW` |
| `WITH_BENCHMARK` | `OTELCPP_WITH_BENCHMARK` |
| `WITH_CONFIGURATION` | `OTELCPP_WITH_CONFIGURATION` |
| `WITH_CURL_LOGGING` | `OTELCPP_WITH_CURL_LOGGING` |
| `WITH_ELASTICSEARCH` | `OTELCPP_WITH_ELASTICSEARCH` |
| `WITH_ETW` | `OTELCPP_WITH_ETW` |
| `WITH_EXAMPLES` | `OTELCPP_WITH_EXAMPLES` |
| `WITH_EXAMPLES_HTTP` | `OTELCPP_WITH_EXAMPLES_HTTP` |
| `WITH_FUNC_TESTS` | `OTELCPP_WITH_FUNC_TESTS` |
| `WITH_GSL` | `OTELCPP_WITH_GSL` |
| `WITH_HTTP_CLIENT_CURL` | `OTELCPP_WITH_HTTP_CLIENT_CURL` |
| `WITH_METRICS_BOUND_INSTRUMENTS_PREVIEW` | `OTELCPP_WITH_METRICS_BOUND_INSTRUMENTS_PREVIEW` |
| `WITH_METRICS_EXEMPLAR_PREVIEW` | `OTELCPP_WITH_METRICS_EXEMPLAR_PREVIEW` |
| `WITH_NO_DEPRECATED_CODE` | `OTELCPP_WITH_NO_DEPRECATED_CODE` |
| `WITH_NO_GETENV` | `OTELCPP_WITH_NO_GETENV` |
| `WITH_OPENTRACING` | `OTELCPP_WITH_OPENTRACING` |
| `WITH_OTLP_FILE` | `OTELCPP_WITH_OTLP_FILE` |
| `WITH_OTLP_GRPC` | `OTELCPP_WITH_OTLP_GRPC` |
| `WITH_OTLP_GRPC_CREDENTIAL_PREVIEW` | `OTELCPP_WITH_OTLP_GRPC_CREDENTIAL_PREVIEW` |
| `WITH_OTLP_HTTP` | `OTELCPP_WITH_OTLP_HTTP` |
| `WITH_OTLP_HTTP_COMPRESSION` | `OTELCPP_WITH_OTLP_HTTP_COMPRESSION` |
| `WITH_OTLP_UTF8_VALIDITY` | `OTELCPP_WITH_OTLP_UTF8_VALIDITY` |
| `WITH_PROMETHEUS` | `OTELCPP_WITH_PROMETHEUS` |
| `WITH_RESOURCE_DETECTORS_PREVIEW` | `OTELCPP_WITH_RESOURCE_DETECTORS_PREVIEW` |
| `WITH_STL` | `OTELCPP_WITH_STL` |
| `WITH_THREAD_INSTRUMENTATION_PREVIEW` | `OTELCPP_WITH_THREAD_INSTRUMENTATION_PREVIEW` |
| `WITH_ZIPKIN` | `OTELCPP_WITH_ZIPKIN` |

`BUILD_TESTING` is not listed because it is a standard CMake variable, not a
deprecated alias. When opentelemetry-cpp is the top-level project,
`BUILD_TESTING` provides the default for `OTELCPP_BUILD_TESTING`. Set
`OTELCPP_BUILD_TESTING` explicitly to control opentelemetry-cpp tests.

#### Mitigation (legacy CMake option names)

Update CMake command lines, presets, build scripts, and CI configurations to
use the replacement names. The legacy names are still accepted during the
deprecation period and produce a deprecation warning at configure time. When
both names are set, the `OTELCPP_` name takes precedence.

#### Planned removal (legacy CMake option names)

The legacy CMake option names will be removed in a future release. The
replacement names are not affected by this removal.

### CMake exporter builder components merged into exporter components

#### Announcement (exporter builder components)

The following CMake install components are deprecated:

* `exporters_ostream_builder` (merged into `exporters_ostream`)
* `exporters_otlp_builder_utils` (merged into `exporters_otlp_common`)
* `exporters_otlp_file_builder` (merged into `exporters_otlp_file`)
* `exporters_otlp_grpc_builder` (merged into `exporters_otlp_grpc`)
* `exporters_otlp_http_builder` (merged into `exporters_otlp_http`)
* `exporters_prometheus_builder` (merged into `exporters_prometheus`)

#### Motivation (exporter builder components)

The core configuration libraries with the builder interfaces are now installed
unconditionally with the SDK. The exporter builder targets have their own CMake
component but do not bring in any external dependencies. Merging the builder
component into its associated exporter component simplifies the experience for
users: if CMake finds the exporter component it also imports the builder target.

#### Scope (exporter builder components)

The separate builder CMake install components listed above are deprecated and
merged into their corresponding exporter components.

#### Mitigation (exporter builder components)

Remove the explicit builder component from `find_package` calls:

```cmake
# Before: explicitly importing the builder component
find_package(opentelemetry-cpp CONFIG REQUIRED COMPONENTS exporters_otlp_grpc exporters_otlp_grpc_builder)

# After: the builder is included with the exporter component
find_package(opentelemetry-cpp CONFIG REQUIRED COMPONENTS exporters_otlp_grpc)
```

Apply the same change for each deprecated builder component listed above.

#### Planned removal (exporter builder components)

These CMake install components will be removed in a future release.

## [opentelemetry-cpp API]

### Deprecation of EventLogger

#### Announcement (EventLogger)

The `EventLogger` API was deprecated by:

* [API] Deprecate event logger
  [#3285](https://github.com/open-telemetry/opentelemetry-cpp/pull/3285)

on Feb 25, 2025, as published in release 1.20.

#### Motivation (EventLogger)

The specification for `EventLogger` was deprecated by:

* Deprecate Event API and SDK in favor of Emit Event in the Log API
  [#4319](https://github.com/open-telemetry/opentelemetry-specification/pull/4319)

The opentelemetry-cpp API and SDK deprecates the EventLogger to be aligned
with the opentelemetry specification.

#### Scope (EventLogger)

The following classes or methods are deprecated in ABI version 1:

* API
  * class opentelemetry::logs::EventLogger
  * class opentelemetry::logs::EventLoggerProvider
  * class opentelemetry::logs::NoopEventLogger
  * class opentelemetry::logs::NoopEventLoggerProvider
  * method opentelemetry::logs::Provider::GetEventLoggerProvider()
  * method opentelemetry::logs::Provider::SetEventLoggerProvider()
* SDK
  * class opentelemetry::sdk::logs::EventLogger
  * class opentelemetry::sdk::logs::EventLoggerProvider
  * class opentelemetry::sdk::logs::EventLoggerProviderFactory

The same are removed from ABI version 2.

#### Mitigation (EventLogger)

In respective order, the following should be used in place of `EventLogger`:

* API
  * class opentelemetry::logs::Logger
  * class opentelemetry::logs::LoggerProvider
  * class opentelemetry::logs::NoopLogger
  * class opentelemetry::logs::NoopLoggerProvider
  * method opentelemetry::logs::Provider::GetLoggerProvider()
  * method opentelemetry::logs::Provider::SetLoggerProvider()
* SDK
  * class opentelemetry::sdk::logs::Logger
  * class opentelemetry::sdk::logs::LoggerProvider
  * class opentelemetry::sdk::logs::LoggerProviderFactory

Instrumented applications, that currently use
`opentelemetry::logs::Provider::GetEventLoggerProvider()`,
are strongly encouraged to migrate and use
`opentelemetry::logs::Provider::GetLoggerProvider()` instead.

Main applications, when installing and configuring an SDK, that currently use
`opentelemetry::logs::Provider::SetEventLoggerProvider()`,
are strongly encouraged to migrate and use
`opentelemetry::logs::Provider::SetLoggerProvider()` instead.

This migration can be implemented now,
as `Logger` is available already in ABI version 1.

This migration is mandatory and a pre requisite for adoption of ABI version 2 later.

#### Planned removal (EventLogger)

All the deprecated code is already removed from ABI version 2, since Feb
2025.

While EventLogger will not be removed from ABI version 1,
the ABI version 1 itself will be deprecated and decommissioned in the long
term, after ABI version 2 is declared stable.

By the time only ABI version 2 is available and supported,
any code still using the deprecated `EventLogger` will break.

## [opentelemetry-cpp SDK]

N/A

## [opentelemetry-cpp Exporter]

N/A

## [Documentation]

N/A

## Semantic conventions

N/A
