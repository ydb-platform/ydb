# Logging

## Purpose

In {{ydb-short-name}}, **structured logging** of occurring events is performed.
Structured logging means that in the log (work journal), the textual description of events is separated from the parameters of those events, and the parameter values themselves are stored and processed independently of each other. For example, if a file read error occurs, its description is text like "File read error", and the parameters are the file path, the system error code, the textual description of the system error, and so on.

Separating event descriptions from their parameters can subsequently be used for efficient message search, statistics collection, optimized log storage, and other tasks.

## Basic tools for writing messages to the log {#simple-logging}

In most cases, when writing messages to the log, the following conditions are met:

1. The source code is contained in some file `.cpp` (logging from header files is supported but [discussed further](#log-in-header-file)).
2. In one file `.cpp`, all messages are written on behalf of the same component. Therefore, at the beginning of the file (but after all `#include` directives), the macro `YDB_LOG_THIS_FILE_COMPONENT` must be defined, which sets the component code for the entire file.

{% note info %}

{% include [log_components](./_includes/log_components.md) %}

When writing a message to the log, the component code on whose behalf the message is written must be explicitly or implicitly specified. Subsequently, the component name is always output to the log and can be used to search or filter the log contents.

In addition, for each component, its own logging parameters can be [configured](../reference/configuration/log_config.md#entry-objects).

{% endnote %}

Example:

```cpp
#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATESTORAGE
```

{% include [undef-ydb-log-this-file-component](./_includes/undef-ydb-log-this-file-component.md) %}

3. Logging occurs during the operation of an [actor](../concepts/glossary.md#actor), and a variable `NActors::TlsActivationContext` is available for working with the context (in particular, for sending messages to the logging actor).

{% note warning %}

If at least one of the three listed conditions is not met, then you should use [extended logging tools](#extended-logging).

{% endnote %}

Further in this file `.cpp`, the following macros can be used for logging messages:

|Macro|Message level  |
|--|--|
|`YDB_LOG_EMERG(message, ...values...)`  | A system failure is possible (for example, cluster failure).|
|`YDB_LOG_ALERT(message, ...values...)`  | System degradation is possible; system components may fail. |
|`YDB_LOG_CRIT(message, ...values...)`  | Critical state.|
|`YDB_LOG_ERROR(message, ...values...)`  | Non-critical error. |
|`YDB_LOG_WARN(message, ...values...)`  | A warning that should be responded to and fixed if it is not temporary. |
|`YDB_LOG_NOTICE(message, ...values...)`  | An event significant to the system or user has occurred.|
|`YDB_LOG_INFO(message, ...values...)`  | Debug information for statistics collection. |
|`YDB_LOG_DEBUG(message, ...values...)`  | Debug information for developers. |
|`YDB_LOG_TRACE(message, ...values...)`  | Very detailed debug information.|

The following parameters are specified in the arguments of the calls to the listed macros:

- `message` — text message;
- `...values...` — optional parameters. Each parameter is specified as a pair `{name, value}`, where `name` is a text string with the parameter name, and `value` is the parameter value.

Examples of writing messages to the journal:

1. Message without parameters:

```cpp
YDB_LOG_INFO("Module started");
```

2. Message with parameters:

```cpp
YDB_LOG_ERROR("Unable to open file",
    {"sourceFilePath", filename},
    {"errorCode", err});
```

{% cut "Dynamic determination of the message level" %}

In some cases, the message level is determined dynamically at runtime. Then you should use a macro that takes the message level as an argument. The macro syntax:

```cpp
YDB_LOG(prio, message, ...values...);
```

Usage example:

```cpp
auto prio = NActors::NLog::PRI_ERROR;
...
YDB_LOG(prio, "Unable to open file",
    {"sourceFilePath", filename},
    {"errorCode", err});
```

{% endcut %}

## Extended tools for writing messages to the log {#extended-logging}

### Logging core

The lowest-level tool for writing messages to the journal is the macro `YDB_LOG_CTX_COMP`. It checks the need for logging (according to [settings](../reference/configuration/log_config.md)), forms a text message, attaches optional parameters to it, and then sends all this information to the logging actor. Next, the logging actor processes the received message according to its current settings (for example, writes it to a file or syslog).

{% note info %}

All other logging tools (including [basic logging tools](#simple-logging)) are built on the basis of the macro `YDB_LOG_CTX_COMP`.

{% endnote %}

The full syntax of the logging macro is as follows:

```cpp
YDB_LOG_CTX_COMP(ctx, prio, comp, message, ...values...)
```

The following parameters are specified in the macro call arguments:

- `ctx` — actor execution context (required to send a message to the logging actor);
- `prio` — message logging level (corresponds to [logging levels](../reference/configuration/log_config.md#log-levels));
- `comp` — component identifier;
- `message` — text message;
- `...values...` — optional parameters. Each parameter is specified as a pair `{name, value}`, where `name` is a text string with the parameter name, and `value` is the parameter value.

In the examples below, `EXAMPLE_COMP_CODE` denotes the logging component code. In working code, specify an existing component instead, for example `NKikimrServices::STATESTORAGE`.

{% cut "Examples of writing a message to the journal" %}

1. Message without parameters:

```cpp
YDB_LOG_CTX_COMP(ctx, PRI_INFO, EXAMPLE_COMP_CODE, "Module started");
```

2. Message with parameters:

```cpp
YDB_LOG_CTX_COMP(ctx, PRI_ERROR, EXAMPLE_COMP_CODE, "Unable to open file",
    {"sourceFilePath", filename},
    {"errorCode", err});
```

In this example, a message with the text `Unable to open file` and two parameters is written: `sourceFilePath` (the value is taken from the variable `filename`) and `errorCode` (the value is taken from the variable `err`). The execution context `ctx` is used to send the message to the logging actor, and `EXAMPLE_COMP_CODE` is passed as the component code.

{% endcut %}

The macro `YDB_LOG_CTX_COMP` implies dynamic determination of the message level and passing it as an argument. By analogy with basic logging tools, there are a number of macros that do not require passing the message level as an argument.

{% cut "Macros without specifying the message level as an argument" %}

|Macro|Message level  |
|--|--|
|`YDB_LOG_EMERG_CTX_COMP`  | A system failure is possible (for example, cluster failure).|
|`YDB_LOG_ALERT_CTX_COMP`  | System degradation is possible; system components may fail. |
|`YDB_LOG_CRIT_CTX_COMP`  | Critical state.|
|`YDB_LOG_ERROR_CTX_COMP`  | Non-critical error. |
|`YDB_LOG_WARN_CTX_COMP`  | A warning that should be responded to and fixed if it is not temporary. |
|`YDB_LOG_NOTICE_CTX_COMP`  | An event significant to the system or user has occurred.|
|`YDB_LOG_INFO_CTX_COMP`  | Debug information for statistics collection. |
|`YDB_LOG_DEBUG_CTX_COMP`  | Debug information for developers. |
|`YDB_LOG_TRACE_CTX_COMP`  | Very detailed debug information.|

The macros listed in the table do not require specifying the argument `prio`.

Example:

```cpp
YDB_LOG_ERROR_CTX_COMP(ctx, EXAMPLE_COMP_CODE, "Unable to open file",
    {"sourceFilePath", filename},
    {"errorCode", err});
```

{% endcut %}

Many other macros discussed below are based on the macro `YDB_LOG_CTX_COMP`. Their names are constructed according to the following scheme:

1. The macro name always begins with `YDB_LOG`.
2. If the macro does not require specifying the message level, then the message level name is added to the macro name with an underscore.
3. If the macro requires specifying the execution context, then `_CTX` is added to the macro name.
4. If the macro requires specifying the component, then `_COMP` is added to the macro name.

### Using the standard execution context

In most cases, the execution context available through the global variable `NActors::TlsActivationContext` is used to send messages to the logging actor. To ensure that the source texts are not visually overloaded with references to this global variable, an additional macro `YDB_LOG_COMP` has been introduced, which always uses `NActors::TlsActivationContext` and does not require specifying the parameter `CTX`.

Example:

```cpp
YDB_LOG_COMP(PRI_ERROR, EXAMPLE_COMP_CODE, "Unable to open file",
    {"sourceFilePath", filename},
    {"errorCode", err});
```

{% note info %}

The construction of the macro name `YDB_LOG_COMP` fully fits the basic macro naming principle discussed in the [logging core](#extended-logging).

{% endnote %}

In this example, the standard execution context is used, and logging occurs on behalf of the component with the code `EXAMPLE_COMP_CODE`.

{% cut "Macros without specifying the message level as an argument" %}

|Macro|Message level  |
|--|--|
|`YDB_LOG_EMERG_COMP`  | A system failure is possible (for example, cluster failure).|
|`YDB_LOG_ALERT_COMP`  | System degradation is possible; system components may fail. |
|`YDB_LOG_CRIT_COMP`  | Critical state.|
|`YDB_LOG_ERROR_COMP`  | Non-critical error. |
|`YDB_LOG_WARN_COMP`  | A warning that should be responded to and fixed if it is not temporary. |
|`YDB_LOG_NOTICE_COMP`  | An event significant to the system or user has occurred.|
|`YDB_LOG_INFO_COMP`  | Debug information for statistics collection. |
|`YDB_LOG_DEBUG_COMP`  | Debug information for developers. |
|`YDB_LOG_TRACE_COMP`  | Very detailed debug information.|

Example:

```cpp
YDB_LOG_ERROR_COMP(EXAMPLE_COMP_CODE, "Unable to open file",
    {"sourceFilePath", filename},
    {"errorCode", err});
```

{% endcut %}

### Using the default component

The same mechanism is used as in [basic logging tools](#simple-logging):

1. At the beginning of the file (but after all `#include` directives), the macro `YDB_LOG_THIS_FILE_COMPONENT` must be defined. It sets the component code for the entire file.

{% include [undef-ydb-log-this-file-component](./_includes/undef-ydb-log-this-file-component.md) %}

2. Further in the file, logging macros that do not require specifying the component code should be used. They are similar to those discussed earlier, but their name does not contain the string `_COMP`.

Example:

```cpp
#define YDB_LOG_THIS_FILE_COMPONENT EXAMPLE_COMP_CODE
...
YDB_LOG_CTX(ctx, PRI_ERROR, "Unable to open file",
    {"sourceFilePath", filename},
    {"errorCode", err});
```

{% cut "Macros without specifying the message level as an argument" %}

|Macro|Message level  |
|--|--|
|`YDB_LOG_EMERG_CTX`  | A system failure is possible (for example, cluster failure).|
|`YDB_LOG_ALERT_CTX`  | System degradation is possible; system components may fail. |
|`YDB_LOG_CRIT_CTX`  | Critical state.|
|`YDB_LOG_ERROR_CTX`  | Non-critical error. |
|`YDB_LOG_WARN_CTX`  | A warning that should be responded to and fixed if it is not temporary. |
|`YDB_LOG_NOTICE_CTX`  | An event significant to the system or user has occurred.|
|`YDB_LOG_INFO_CTX`  | Debug information for statistics collection. |
|`YDB_LOG_DEBUG_CTX`  | Debug information for developers. |
|`YDB_LOG_TRACE_CTX`  | Very detailed debug information.|

Example:

```cpp
#define YDB_LOG_THIS_FILE_COMPONENT EXAMPLE_COMP_CODE
...
YDB_LOG_ERROR_CTX(ctx, "Unable to open file",
    {"sourceFilePath", filename},
    {"errorCode", err});
```

{% endcut %}

### Logging in header files {#log-in-header-file}

{% note warning %}

It is strongly discouraged to define the macro `YDB_LOG_THIS_FILE_COMPONENT` in header files, as this can lead to the following consequences:

1. Complex logic for determining the component code (it is not obvious which header file the definition of `YDB_LOG_THIS_FILE_COMPONENT` comes from).
2. Compilation errors (in different files, the macro `YDB_LOG_THIS_FILE_COMPONENT` may be defined differently).

{% endnote %}

If logging is necessary in a header file, then macros that require explicit specification of the component code should be used:

1. `YDB_LOG_CTX_COMP` — requires explicit specification of the message level, execution context, and component code.
2. `YDB_LOG_XXXX_CTX_COMP` — does not require specifying the message level.
3. `YDB_LOG_COMP` — uses the standard execution context.
4. `YDB_LOG_XXXX_COMP` — uses the standard execution context and does not require specifying the message level.

## Constructing messages

### Reusable sets of attached parameters

If the same set of parameters is attached to different messages, then to avoid code duplication, you can create this parameter set in advance and then reuse it when logging various events. The macro `YDB_LOG_CREATE_MESSAGE` is intended for this purpose. It takes a set of parameters as arguments (in the form of pairs `{name, value}`), and as a result returns an object that can subsequently be used when sending messages to the journal.

{% cut "Example" %}

```cpp
#define YDB_LOG_THIS_FILE_COMPONENT EXAMPLE_COMP_CODE
...
void MyFunction(const std::string& filename) {
    ...
    // Create message parameters
    auto context = YDB_LOG_CREATE_MESSAGE(
        {"sourceFilePath", filename});
    ...
    if (err != 0) {
        YDB_LOG_ERROR("MyFunction failed",
            context,                            // Use message parameters
            {"errorCode", err});
        return;
    }
    ...
    YDB_LOG_NOTICE("MyFunction succeeded",
        context);                               // Use message parameters
}
```

{% endcut %}

The result of calling the macro `YDB_LOG_CREATE_MESSAGE` is an instance of the class `TStructuredMessage`. This instance stores the set of parameters included in it, their names, types, and values. Instances of the class `TStructuredMessage` can be stored in local and global variables, copied, passed, and so on.

{% note info %}

The class `TStructuredMessage` can be considered a specialized container for storing pairs `{name, value}` attached to journal messages.

{% endnote %}

Operations for modifying a previously created parameter set are supported:

1. Adding and updating.

{% cut "Example" %}

```cpp
YDB_LOG_UPDATE_MESSAGE(context,
    {"socket", socketNum});
```

{% endcut %}

2. Removing parameters.

{% cut "Example" %}

```cpp
context.RemoveValue("socket");
```

{% endcut %}

### Nested parameter sets

It is possible to create nested sets of attached parameters. To do this, when logging a message (or creating another parameter set), a previously created value set (i.e., an instance of the class `TStructuredMessage`) must be specified as the value in the pair `{name, value}`. Then, all parameters from the set `value` will be automatically added to the resulting message (or created value set), but a prefix specified in `name` with the separator `.` will be added to the names of these parameters.

{% cut "Example" %}

```cpp
#define YDB_LOG_THIS_FILE_COMPONENT EXAMPLE_COMP_CODE
...
void MyFunction() {
    // Create message parameters
    auto context = YDB_LOG_CREATE_MESSAGE(
        {"operationName", "read"},
        {"sourceFilePath", filename});
    ...
    if (err != 0) {
        YDB_LOG_ERROR("MyFunction failed",
            {"details", context},                            // Use message parameters
            {"errorCode", err});
        return;
    }
    ...
}
```

In this example, the parameters `details.operationName`, `details.sourceFilePath`, and `errorCode` will be attached to the message with the text `MyFunction failed`.

{% endcut %}

### Logging contexts

Forming and reusing sets of attached parameters is inconvenient in the following cases:

1. These sets need to be specified when logging a large number of messages.
2. These sets need to be passed to all called functions so that they can be specified there when logging messages.

You can simplify the source code by using a **logging context** — a set of parameters that will be automatically attached to all messages sent to the journal in the given execution thread. To do this, call the macro `YDB_LOG_CREATE_CONTEXT` and pass it pairs `{name, value}` (the same way as when sending messages to the journal or creating reusable sets), as well as previously created objects `TStructuredMessage`. The parameters listed in `YDB_LOG_CREATE_CONTEXT` are added during logging in the code block where this macro is called, as well as in nested code blocks and all called functions.

{% cut "Example of using a logging context" %}

```cpp
#define YDB_LOG_THIS_FILE_COMPONENT EXAMPLE_COMP_CODE
...
void MyFunction() {
    YDB_LOG_CREATE_CONTEXT(
        {"sourceFilePath", filename});
    ...
    if (errorCode != 0) {
        YDB_LOG_ERROR("MyFunction failed",     // The parameters sourceFilePath and errorCode will be attached to the message
            {"errorCode", errorCode});
        return;
    }
    ...
    YDB_LOG_NOTICE("MyFunction succeeded");    // The parameter sourceFilePath will be attached to the message
}
```

The parameter `sourceFilePath` will be attached to all messages logged in this execution thread in the function `MyFunction` and the functions it calls until exiting the function `MyFunction`.

{% endcut %}

Logging contexts are organized as a stack. If a logging context is configured, it is stored at the top of the stack. Each execution thread has its own stack of logging contexts.

When calling `YDB_LOG_CREATE_CONTEXT`, the following actions occur:

1. A new parameter set is created (as when calling `YDB_LOG_CREATE_MESSAGE`), containing all parameters from the existing context, as well as the parameters directly specified in the macro `YDB_LOG_CREATE_CONTEXT`.
2. The new parameter set is placed on the top of the stack and thus becomes the current logging context in this execution thread.
3. When writing a message to the journal (calling the macro `YDB_LOG_CTX_COMP` or similar), the parameters from the top of the logging context stack are automatically attached to the message, and the parameters specified in the logging macro are written on top of this set.
4. The top of the stack will be automatically removed when exiting the current block, thus returning to the previously configured context.

To add parameters to the current context or update their values, use the macro `YDB_LOG_UPDATE_CONTEXT`:

```cpp
YDB_LOG_UPDATE_CONTEXT({"requestId", requestId});
```

The macro `YDB_LOG_UPDATE_CONTEXT` can specify several parameters and their values at once. If the specified parameters already exist in the context, their values will be updated. If the specified parameters are absent from the context, their values will be added to the context.

To remove parameters from the current context, pass their names to `YDB_LOG_REMOVE_CONTEXT`:

```cpp
YDB_LOG_REMOVE_CONTEXT("requestId");
```

The macros `YDB_LOG_UPDATE_CONTEXT` and `YDB_LOG_REMOVE_CONTEXT` can be called any number of times. They modify only the current context (that is, the top of the context stack) but do not create new contexts and do not affect other contexts located lower in the context stack.

## Recommendations for log content

### Security

It is forbidden to write passwords, access tokens, encryption keys, connection strings, and personal data to the log. If a value is necessary for diagnostics, the sensitive part must be removed, masked, or replaced with an irreversible hash before passing it to the macro.

### Coding style

The following coding style is recommended when using logging macros:

1. All mandatory information (execution context, component code, message level, and message text) is placed on one line.
2. Each pair `{name, value}` is placed on a separate line.

Example of writing a message with several parameters:

```cpp
    YDB_LOG_DEBUG("Handle TEvNodeWardenNotifyConfigMismatch",
        {"clusterStateGeneration", ClusterStateGeneration},
        {"msgGeneration", msgGeneration},
        {"clusterStateGuid", ClusterStateGuid},
        {"msgGuid", msgGuid});
```

### Text message style

The following style for writing message texts is recommended:

1. The text is a meaningful (and preferably correct) sentence in English that speaks about some event inside the system, where:

   - the text begins with a capital letter;
   - if the text is a separate sentence, no period is placed at the end of the message;
   - if the text consists of several sentences, they are separated by periods, but no period is placed after the last sentence.

2. The text may contain class and function names. If an actor logs the fact of receiving a message, a good practice is to log with the text `Handle <event class name>`.
3. Messages are fixed text without dynamically formed fragments.
4. All dynamic information known only at runtime must be placed as attached parameters.

To align message texts and parameter names, there is a simple empirical rule:
> If you take the message text and add the parameter values to it in the form `(name=value, name=value, ... )`, you should get a meaningful, complete, unambiguous, and human-understandable text message.

{% cut "Example of aligned message text and parameter names" %}

```cpp
YDB_LOG_ERROR_CTX(ctx, "Unable to open file",
    {"sourceFilePath", filename},
    {"errorCode", err});
```

The expected event description looks like this (message parameters are listed in alphabetical order):

```text
Unable to open file (errorCode = ..., sourceFilePath = ...)
```

{% endcut %}

### General recommendations for naming individual parameters

The naming style for individual parameters is based on two basic principles:

1. In the context of a specific message, the parameter value should be interpreted simply and unambiguously.

{% list tabs %}

- Good

```cpp
YDB_LOG_ERROR("Response timeout elapsed",
    {"nodeHostName", hostName},
    {"requestNum", requestNum},
    {"waitAtPosixTime", startTime},
    {"timeoutMs", timeout});
```

- Bad

```cpp
YDB_LOG_ERROR("Response timeout elapsed",
    {"node", hostName},
    {"request", requestNum},
    {"wait", startTime},
    {"timeout", timeout});
```

If you use the previously mentioned rule for aligning message texts and parameter names, you get the sentence `Response timeout elapsed (node=<string>, request=<number>, wait=<number>, timeout=<number>)`, where it will be unclear: is the value `node` a host name or some internal node identifier name? Is the value `request` a sequence number or a numeric identifier? What is the value `wait` and how should it be interpreted? In what units is the value `wait` specified?

{% endlist %}

2. Semantically similar parameters should have the same names, since different naming of the same concepts makes it difficult to search for records in logs. For example, it is undesirable to denote a transaction identifier as `txId` in some messages and as `transactionId` in others.

{% list tabs %}

- Good

```cpp
YDB_LOG_INFO("Started transaction",
    {"txId", transactionId});
...
YDB_LOG_INFO("Transaction modifies table",
    {"queryId", queryId},
    {"txId", transactionId});
...
YDB_LOG_INFO("Query commits transaction",
    {"queryId", queryId},
    {"txId", transactionId});
...
```

- Bad

```cpp
YDB_LOG_INFO("Started transaction",
    {"id", transactionId});
...
YDB_LOG_INFO("Transaction modifies table",
    {"id", queryId},
    {"transactionId", transactionId});
...
YDB_LOG_INFO("Query commits transaction",
    {"id", queryId},
    {"txId", transactionId});
...
```

In this example, the transaction identifier is written to parameters with different names (`id`, `transactionId`, and `txId`), and at the same time, the parameter named `id` stores identifiers of completely different entities each time.

{% endlist %}

3. Parameter names are written in `camelCase`.

### Specific recommendations for naming individual parameters

The following recommendations are aimed at fulfilling the two principles mentioned above:

1. It is undesirable to use individual common words as parameter names (`id`, `item`, `value`, `path`, `child`, `parent`, `min`, `max`, and so on) — this complicates searching for records in logs. For example, the word `id` denotes an identifier, but it is completely unclear which entity it refers to, although in the context of a specific message, the meaning of this parameter may be extremely clear.

{% list tabs %}

- Good

```cpp
YDB_LOG_INFO("Started transaction",
    {"txId", ...});
...
YDB_LOG_INFO("Received query",
    {"queryId", ...});
...
YDB_LOG_INFO("Sent request to actor",
    {"actorId", ...});
```

- Bad

```cpp
YDB_LOG_INFO("Started transaction",
    {"id", ...});
...
YDB_LOG_INFO("Received query",
    {"id", ...});
...
YDB_LOG_INFO("Sent request to actor",
    {"id", ...});
```

{% endlist %}

2. Parameter names should consist of several words, with the last word being the main one, and each preceding word should clarify the meaning of the following one. For example:

- `actorId` — actor identifier;
- `operationId` — operation identifier;
- `operationName` — operation name;
- `operationSessionId` — identifier of the session within which the operation is performed.

{% list tabs %}

- Good

```cpp
YDB_LOG_INFO("Copy data to another shard",
    {"srcShardId", ...},
    {"dstShardId", ...});
```

- Bad

```cpp
YDB_LOG_INFO("Copy data to another shard",
    {"srcId", ...},
    {"dstShard", ...});
```

Here it is unclear what entity the parameter `srcId` is an identifier of, nor how to interpret the value `dstShard` (is it an identifier, a name, or something else).

{% endlist %}

3. Rules for denoting individual entities and collections:

- a separate mention of an entity name in the singular implies that the parameter value contains a description of that entity. For example, `table` is a description of a table (however, in this case, it is better to add a suffix, that is, `tableId` explicitly indicates that the parameter contains a table identifier, `tableName` — a table name, `tableDesc` — a table description, and so on);
- a separate mention of an entity name in the plural implies that the parameter value contains a description of a collection of entities. For example, `tables` is a description of several tables (but not their count);
- a separate mention of an entity name in the plural with the addition of `Count` implies that the parameter value contains the number of elements in the collection. For example, `tablesCount` is the number of tables;
- it is undesirable to add `Size` instead of `Count` to the parameter name to denote the number of elements in a collection. For example, by the parameter name `totalFilesSize`, it is unclear whether we are talking about the number of files or their total size.

{% list tabs %}

- Good

```cpp
YDB_LOG_INFO("Backup volumes progress",
    {"currentVolumeNum", ...},
    {"volumesCount", ...},
    {"volumesSizeBytes", ...});
```

- Bad

```cpp
YDB_LOG_INFO("Backup volumes progress",
    {"currentVolume", ...},
    {"volumes", ...});
```

{% endlist %}

4. If a parameter name uses an abbreviation or term generally accepted in {{ydb-short-name}}, it should be written in the case in which it is customary. Examples of such parameter names:

- `VDiskId` — VDisk identifier (written not as `vdiskId`);
- `PDiskId` — PDisk identifier (written not as `pdiskId`);
- and so on.

5. Popular abbreviations are allowed (`idx` instead of `index`, `msg` instead of `message`, `tx` instead of `transaction`, and so on).

6. If a parameter contains a numeric value, it is desirable to add a suffix to the parameter name:

- `Id` if the number is an identifier (but not as `ID`);
- the name of the units of measurement (for example, for memory volumes — `Byte`, `KByte`, `MByte`, for time — `Ms`, `Sec`, `Min`, for indicating percentage shares of something — `Percent`, and so on). If a non-standard unit of measurement is used, it is desirable to use the preposition `In` (for example, `bufferSizeInBlocks`);

{% list tabs %}

- Good

```cpp
YDB_LOG_INFO("Table dump progress",
    {"tableId", ...},
    {"dumpedRecordsCount", ...},
    {"dumpedSizeBytes", ...},
    {"totalSizeBytes", ...});
```

- Bad

```cpp
YDB_LOG_INFO("Table dump progress",
    {"table", ...},
    {"dumpedRecords", ...},
    {"dumpedSize", ...},
    {"totalSize", ...});
```

{% endlist %}

7. If a numeric parameter value is more convenient for a person to analyze not in decimal form, it is better to specify this value as a character string and supplement it with generally accepted characters (for example, a hexadecimal representation should have the prefix `0x`, and a binary one should have the prefix '0b', and so on). A good practice is to supplement the parameter name with the suffix `Hex` or similar.

{% list tabs %}

- Good

```cpp
unsigned flags;
...
YDB_LOG_INFO("Invalid access rights to file",
    {"filename", ...},
    {"aclFlags", flags},
    {"aclFlagsOct", ToOct(flags)});         // Make flags as "0XXXXX" string
```

- Bad

```cpp
unsigned flags;
...
YDB_LOG_INFO("Invalid access rights to file",
    {"filename", ...},
    {"aclFlags", flags});
```

{% endlist %}

8. If a parameter contains textual information, it is desirable to add a suffix to the parameter name that makes it easier to interpret the parameter value (for example, `Name`, `Desc`, `Json`, `Base64`, and so on).
9. Local variable naming rules can be applied to parameter names (for example, for boolean flags, add `is`, `has`, or similar to the beginning of the parameter name).

### Commonly accepted parameter names

There are commonly accepted parameter names that are desirable to use.

|Name|Parameter content|
|--|--|
|`actorClassName`|Name of the class in which the actor is implemented.|
|`actorStateName`|Actor state.|
|`backtrace`|Exception stack trace (result of the function `TBackTrace::FromCurrentException().PrintToString()`).|
|`ev`|Description of the message that the actor processes.|
|`evType`|Type of the message that the actor processes.|
|`exception`|Description of the C++ exception that occurred.|
|`selfId`|Identifier of the actor that processes the message.|

### Methodology for enriching logs with contextual information

The following methodology is proposed for enriching the events that a specific actor writes to the journal.

**Step 1.** The actor must implement a function that forms the current logging context as a set of parameters attached to logged messages.

{% cut "Implementation example" %}

```cpp
TStructuredMessage GetLogContext() const {
    return YDB_LOG_CREATE_MESSAGE(
        {"actorClassName", "TFileReader"},
        {"selfId", SelfId()},
        {"sourceFilePath", filename});
}
```

Instead of the name `GetLogContext`, `GetLogPrefix` or similar can be used. In principle, it may not even be a function but a class field created in the constructor and modified during the actor's operation.

{% endcut %}

**Step 2.** Configuring the context at "entry points". Each actor has a small set of functions that can be called by the actor system core. These include:

1. The function `Bootstrap`.
2. Message handling functions (found by the macro `STATEFN` or similar).
3. Other virtual functions that the actor overrides (they are easily found by the keyword `override`).

Then, in the first line of each such function, the logging context must be configured using the macro `YDB_LOG_CREATE_CONTEXT`.

{% cut "Context configuration example" %}

```cpp
STATEFN(StateWork) {
    YDB_LOG_CREATE_CONTEXT(GetLogContext());
    switch (ev->GetTypeRewrite()) {
        ...
    }
}
```

{% endcut %}
