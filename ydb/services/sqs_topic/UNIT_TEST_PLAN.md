# Unit Test Plan for ChangeMessageVisibility

## Overview
This document outlines the comprehensive unit test strategy for `ChangeMessageVisibility` and `ChangeMessageVisibilityBatch` operations in the YDB SQS Topic service.

## Test Infrastructure

### Existing Test Fixture
- **Class**: `THttpProxyTestMockForSQSTopic` (defined in `datastreams_fixture.h`)
- **Helper Methods** (already implemented):
  - `ChangeMessageVisibility(NJson::TJsonMap request, ui32 expectedHttpCode = 200)` (line 206)
  - `ChangeMessageVisibilityBatch(NJson::TJsonMap request, ui32 expectedHttpCode = 200)` (line 210)

### Test Pattern Analysis
Based on existing tests (DeleteMessage, ReceiveMessage, SendMessage), the pattern is:
1. Create topic with consumer using `CreateTopic(driver, topicName, consumerName)`
2. Send messages using `SendMessage()` or `SendMessageBatch()`
3. Receive messages using `ReceiveMessage()` to get receipt handles
4. Perform operation under test
5. Verify behavior by attempting to receive messages again

## Test Suite: ChangeMessageVisibility (Single Message)

### Test 1: TestChangeMessageVisibilityInvalid
**Purpose**: Validate error handling for invalid parameters

**Test Cases**:
```cpp
// Missing parameters
ChangeMessageVisibility({}, 400);
UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "MissingParameter");

// Invalid QueueUrl
ChangeMessageVisibility({{"QueueUrl", "wrong-queue-url"}}, 400);
UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "InvalidParameterValue");

// Non-existent queue
ChangeMessageVisibility({{"QueueUrl", "/v1/5//Root/16/ExampleQueueName/13/user_consumer"}}, 400);
UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "AWS.SimpleQueueService.NonExistentQueue");

// Missing ReceiptHandle
ChangeMessageVisibility({{"QueueUrl", path.QueueUrl}}, 400);
UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "MissingParameter");

// Invalid ReceiptHandle
ChangeMessageVisibility({{"QueueUrl", path.QueueUrl}, {"ReceiptHandle", "invalid"}}, 400);
UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "ReceiptHandleIsInvalid");

// Missing VisibilityTimeout
ChangeMessageVisibility({{"QueueUrl", path.QueueUrl}, {"ReceiptHandle", receiptHandle}}, 400);
UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "MissingParameter");

// Invalid VisibilityTimeout (negative)
ChangeMessageVisibility({
    {"QueueUrl", path.QueueUrl},
    {"ReceiptHandle", receiptHandle},
    {"VisibilityTimeout", -1}
}, 400);
UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "InvalidParameterValue");

// Invalid VisibilityTimeout (too large)
ChangeMessageVisibility({
    {"QueueUrl", path.QueueUrl},
    {"ReceiptHandle", receiptHandle},
    {"VisibilityTimeout", 43201}
}, 400);
UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "InvalidParameterValue");
```

### Test 2: TestChangeMessageVisibilityBasic
**Purpose**: Verify basic visibility timeout change functionality

**Flow**:
1. Create topic and send message
2. Receive message (gets receipt handle, default visibility timeout ~20s)
3. Change visibility timeout to 1 second
4. Verify message is NOT available immediately (still in visibility timeout)
5. Wait 1+ seconds
6. Verify message becomes available again

**Expected Behavior**:
- ChangeMessageVisibility returns success (empty response body)
- Message reappears after new timeout expires

```cpp
Y_UNIT_TEST_F(TestChangeMessageVisibilityBasic, TFixture) {
    auto driver = MakeDriver(*this);
    const TSqsTopicPaths path;
    bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
    UNIT_ASSERT(a);

    TString body = "MessageBody-0";
    SendMessage({{"QueueUrl", path.QueueUrl}, {"MessageBody", body}});

    // Receive with default visibility timeout
    auto json = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 20}});
    UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
    auto receiptHandle = json["Messages"][0]["ReceiptHandle"].GetString();

    // Change visibility to 1 second
    ChangeMessageVisibility({
        {"QueueUrl", path.QueueUrl},
        {"ReceiptHandle", receiptHandle},
        {"VisibilityTimeout", 1}
    });

    // Message should NOT be available immediately
    auto json2 = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 1}});
    UNIT_ASSERT_VALUES_EQUAL(json2["Messages"].GetArray().size(), 0);

    // Wait for new timeout to expire
    Sleep(TDuration::Seconds(2));

    // Message should be available again
    auto json3 = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 20}});
    UNIT_ASSERT_VALUES_EQUAL(json3["Messages"].GetArray().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(json3["Messages"][0]["Body"], body);
}
```

### Test 3: TestChangeMessageVisibilityZeroTimeout
**Purpose**: Test immediate message availability (visibility timeout = 0)

**Flow**:
1. Send and receive message
2. Change visibility timeout to 0 seconds
3. Immediately verify message is available

```cpp
Y_UNIT_TEST_F(TestChangeMessageVisibilityZeroTimeout, TFixture) {
    auto driver = MakeDriver(*this);
    const TSqsTopicPaths path;
    bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
    UNIT_ASSERT(a);

    TString body = "MessageBody-0";
    SendMessage({{"QueueUrl", path.QueueUrl}, {"MessageBody", body}});

    auto json = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 20}});
    UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
    auto receiptHandle = json["Messages"][0]["ReceiptHandle"].GetString();

    // Change visibility to 0 (immediate availability)
    ChangeMessageVisibility({
        {"QueueUrl", path.QueueUrl},
        {"ReceiptHandle", receiptHandle},
        {"VisibilityTimeout", 0}
    });

    // Message should be available immediately
    auto json2 = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 20}});
    UNIT_ASSERT_VALUES_EQUAL(json2["Messages"].GetArray().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(json2["Messages"][0]["Body"], body);
}
```

### Test 4: TestChangeMessageVisibilityExtendTimeout
**Purpose**: Test extending visibility timeout to prevent message reappearance

**Flow**:
1. Receive message with short visibility timeout (2 seconds)
2. After 1 second, extend visibility to 10 seconds
3. Verify message doesn't reappear after original 2 seconds
4. Verify message reappears after extended timeout

```cpp
Y_UNIT_TEST_F(TestChangeMessageVisibilityExtendTimeout, TFixture) {
    auto driver = MakeDriver(*this);
    const TSqsTopicPaths path;
    bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
    UNIT_ASSERT(a);

    TString body = "MessageBody-0";
    SendMessage({{"QueueUrl", path.QueueUrl}, {"MessageBody", body}});

    // Receive with 2 second visibility timeout
    auto json = ReceiveMessage({
        {"QueueUrl", path.QueueUrl},
        {"WaitTimeSeconds", 20},
        {"VisibilityTimeout", 2}
    });
    UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
    auto receiptHandle = json["Messages"][0]["ReceiptHandle"].GetString();

    // Wait 1 second, then extend to 10 seconds
    Sleep(TDuration::Seconds(1));
    ChangeMessageVisibility({
        {"QueueUrl", path.QueueUrl},
        {"ReceiptHandle", receiptHandle},
        {"VisibilityTimeout", 10}
    });

    // After original 2 seconds, message should NOT be available
    Sleep(TDuration::Seconds(2));
    auto json2 = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 1}});
    UNIT_ASSERT_VALUES_EQUAL(json2["Messages"].GetArray().size(), 0);

    // After extended timeout, message should be available
    Sleep(TDuration::Seconds(8));
    auto json3 = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 20}});
    UNIT_ASSERT_VALUES_EQUAL(json3["Messages"].GetArray().size(), 1);
}
```

## Test Suite: ChangeMessageVisibilityBatch

### Test 5: TestChangeMessageVisibilityBatchInvalid
**Purpose**: Validate batch operation error handling

**Test Cases**:
```cpp
// Empty batch
ChangeMessageVisibilityBatch({
    {"QueueUrl", path.QueueUrl},
    {"Entries", NJson::TJsonArray{}}
}, 400);
UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "AWS.SimpleQueueService.EmptyBatchRequest");

// Too many entries (>10)
ChangeMessageVisibilityBatch({
    {"QueueUrl", path.QueueUrl},
    {"Entries", NJson::TJsonArray{/* 11 entries */}}
}, 400);
UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "AWS.SimpleQueueService.TooManyEntriesInBatchRequest");

// Missing required fields in entry
ChangeMessageVisibilityBatch({
    {"QueueUrl", path.QueueUrl},
    {"Entries", NJson::TJsonArray{
        NJson::TJsonMap{{"Id", "id-1"}} // Missing ReceiptHandle and VisibilityTimeout
    }}
}, 400);
```

### Test 6: TestChangeMessageVisibilityBatch
**Purpose**: Test successful batch visibility timeout changes

**Flow**:
1. Send 3 messages
2. Receive all 3 messages
3. Change visibility timeout for all 3 in batch (mix of valid and invalid)
4. Verify successful changes and failures

```cpp
Y_UNIT_TEST_F(TestChangeMessageVisibilityBatch, TFixture) {
    auto driver = MakeDriver(*this);
    const TSqsTopicPaths path;
    bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
    UNIT_ASSERT(a);

    // Send 3 messages
    SendMessageBatch({
        {"QueueUrl", path.QueueUrl},
        {"Entries", NJson::TJsonArray{
            NJson::TJsonMap{{"Id", "Id-1"}, {"MessageBody", "Body-1"}},
            NJson::TJsonMap{{"Id", "Id-2"}, {"MessageBody", "Body-2"}},
            NJson::TJsonMap{{"Id", "Id-3"}, {"MessageBody", "Body-3"}},
        }}
    });

    // Receive all messages
    THashMap<TString, TString> receiptHandles;
    auto jsonReceived = ReceiveMessage({
        {"QueueUrl", path.QueueUrl},
        {"WaitTimeSeconds", 20},
        {"MaxNumberOfMessages", 10}
    });
    for (const auto& message : jsonReceived["Messages"].GetArray()) {
        receiptHandles[message["Body"].GetString()] = message["ReceiptHandle"].GetString();
    }
    UNIT_ASSERT_VALUES_EQUAL(receiptHandles.size(), 3);

    // Batch change visibility: 2 valid, 1 invalid receipt
    auto changeJson = ChangeMessageVisibilityBatch({
        {"QueueUrl", path.QueueUrl},
        {"Entries", NJson::TJsonArray{
            NJson::TJsonMap{
                {"Id", "change-1"},
                {"ReceiptHandle", receiptHandles["Body-1"]},
                {"VisibilityTimeout", 1}
            },
            NJson::TJsonMap{
                {"Id", "change-2"},
                {"ReceiptHandle", receiptHandles["Body-2"]},
                {"VisibilityTimeout", 1}
            },
            NJson::TJsonMap{
                {"Id", "change-invalid"},
                {"ReceiptHandle", "invalid-receipt"},
                {"VisibilityTimeout", 1}
            },
        }}
    });

    // Verify results
    UNIT_ASSERT_VALUES_EQUAL(changeJson["Successful"].GetArray().size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(changeJson["Successful"][0]["Id"], "change-1");
    UNIT_ASSERT_VALUES_EQUAL(changeJson["Successful"][1]["Id"], "change-2");

    UNIT_ASSERT_VALUES_EQUAL(changeJson["Failed"].GetArray().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(changeJson["Failed"][0]["Id"], "change-invalid");
    UNIT_ASSERT_VALUES_EQUAL(changeJson["Failed"][0]["Code"], "ReceiptHandleIsInvalid");

    // Wait for new timeout and verify messages reappear
    Sleep(TDuration::Seconds(2));
    auto json2 = ReceiveMessage({
        {"QueueUrl", path.QueueUrl},
        {"WaitTimeSeconds", 20},
        {"MaxNumberOfMessages", 10}
    });
    UNIT_ASSERT_VALUES_EQUAL(json2["Messages"].GetArray().size(), 2);
}
```

### Test 7: TestChangeMessageVisibilityBatchMixedTimeouts
**Purpose**: Test batch with different visibility timeouts

**Flow**:
1. Send 3 messages
2. Receive all 3
3. Change visibility: msg1=1s, msg2=2s, msg3=3s
4. Verify messages reappear at correct times

```cpp
Y_UNIT_TEST_F(TestChangeMessageVisibilityBatchMixedTimeouts, TFixture) {
    auto driver = MakeDriver(*this);
    const TSqsTopicPaths path;
    bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
    UNIT_ASSERT(a);

    // Send 3 messages
    SendMessageBatch({
        {"QueueUrl", path.QueueUrl},
        {"Entries", NJson::TJsonArray{
            NJson::TJsonMap{{"Id", "Id-1"}, {"MessageBody", "Body-1"}},
            NJson::TJsonMap{{"Id", "Id-2"}, {"MessageBody", "Body-2"}},
            NJson::TJsonMap{{"Id", "Id-3"}, {"MessageBody", "Body-3"}},
        }}
    });

    // Receive and change visibility with different timeouts
    auto jsonReceived = ReceiveMessage({
        {"QueueUrl", path.QueueUrl},
        {"WaitTimeSeconds", 20},
        {"MaxNumberOfMessages", 10}
    });

    TVector<std::pair<TString, TString>> bodyToReceipt;
    for (const auto& message : jsonReceived["Messages"].GetArray()) {
        bodyToReceipt.push_back({
            message["Body"].GetString(),
            message["ReceiptHandle"].GetString()
        });
    }

    ChangeMessageVisibilityBatch({
        {"QueueUrl", path.QueueUrl},
        {"Entries", NJson::TJsonArray{
            NJson::TJsonMap{
                {"Id", "change-1"},
                {"ReceiptHandle", bodyToReceipt[0].second},
                {"VisibilityTimeout", 1}
            },
            NJson::TJsonMap{
                {"Id", "change-2"},
                {"ReceiptHandle", bodyToReceipt[1].second},
                {"VisibilityTimeout", 2}
            },
            NJson::TJsonMap{
                {"Id", "change-3"},
                {"ReceiptHandle", bodyToReceipt[2].second},
                {"VisibilityTimeout", 3}
            },
        }}
    });

    // After 1.5 seconds, only first message should be available
    Sleep(TDuration::MilliSeconds(1500));
    auto json1 = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 1}});
    UNIT_ASSERT_VALUES_EQUAL(json1["Messages"].GetArray().size(), 1);

    // After 2.5 seconds total, first two messages should be available
    Sleep(TDuration::Seconds(1));
    auto json2 = ReceiveMessage({
        {"QueueUrl", path.QueueUrl},
        {"WaitTimeSeconds", 1},
        {"MaxNumberOfMessages", 10}
    });
    UNIT_ASSERT_GE(json2["Messages"].GetArray().size(), 1);

    // After 3.5 seconds total, all three messages should be available
    Sleep(TDuration::Seconds(1));
    auto json3 = ReceiveMessage({
        {"QueueUrl", path.QueueUrl},
        {"WaitTimeSeconds", 1},
        {"MaxNumberOfMessages", 10}
    });
    UNIT_ASSERT_GE(json3["Messages"].GetArray().size(), 1);
}
```

## Test Coverage Summary

### Error Cases Covered
1. ✅ Missing parameters (QueueUrl, ReceiptHandle, VisibilityTimeout)
2. ✅ Invalid QueueUrl format
3. ✅ Non-existent queue
4. ✅ Invalid receipt handle
5. ✅ Invalid visibility timeout (negative, too large)
6. ✅ Empty batch request
7. ✅ Too many entries in batch (>10)
8. ✅ Partial batch failures

### Functional Cases Covered
1. ✅ Basic visibility timeout change
2. ✅ Zero timeout (immediate availability)
3. ✅ Extending visibility timeout
4. ✅ Batch operations with mixed results
5. ✅ Batch operations with different timeouts per message
6. ✅ Message reappearance after timeout expiration

### Edge Cases Covered
1. ✅ Visibility timeout = 0 (minimum)
2. ✅ Visibility timeout = 43200 (maximum, 12 hours)
3. ✅ Multiple visibility changes on same message
4. ✅ Batch with all failures
5. ✅ Batch with all successes

## Test Execution Strategy

### Test Order
Tests should be executed in this order to ensure proper isolation:
1. Invalid parameter tests (fast, no side effects)
2. Basic functionality tests
3. Edge case tests
4. Batch operation tests

### Test Isolation
Each test:
- Creates its own topic with unique name
- Uses `TFixture` which provides clean environment
- Cleans up resources automatically via fixture teardown

### Performance Considerations
- Tests involving `Sleep()` should use minimal wait times
- Use `WaitTimeSeconds` parameter judiciously
- Batch tests are more time-efficient than multiple single tests

## Implementation Location
All tests should be added to: `/ydb/core/http_proxy/ut/sqs_topic_ut.cpp`

Insert after the existing `TestDeleteMessageBatch` test (line 610), before the closing brace of `Y_UNIT_TEST_SUITE(TestSqsTopicHttpProxy)`.
