# SDK

## What should I do if the SDK crashes when shutting down an application? {#what-to-do-if-the-sdk-crashes-urgently-when-the-app-is-shut-down}

Make sure not to wrap SDK components in a singleton, since their lifetime shouldn't exceed the execution time of the `main()` function. When a client is destroyed, session pools are emptied, so network navigation is required. But gRPC contains global static variables that might already be destroyed by this time. This disables gRPC. If you need to declare a driver as a global object, invoke the `Stop(true)` function on the driver before exiting the `main()` function.

## What should I do if, when using a `fork()` system call, a program does not work properly in a child process? {#program-does-not-work-correctly-when-calling-fork}

Using `fork()` in multithreaded applications is an antipattern. Since both the SDK and the gRPC library are multithreaded applications, their stability is not guaranteed.

## What do I do if I get the "Active sessions limit exceeded" error even though the current number of active sessions is within limits? {#active-sessions-does-not-exceed-the-limit}

The limit applies to the number of active sessions. An active session is a session passed to the client to be used in its code. A session is returned to the pool in a destructor. In this case, the session itself is a replicated object. You may have saved a copy of the session in the code.

## Is it possible to make queries to different databases from the same application? {#make-requests-to-different-databases-from-the-same-application}

Yes, the C++ SDK lets you override the DB parameters and token when creating a client. There is no need to create separate drivers.

## What should I do if a VM has failed and it's impossible to make a query? {#vms-failed-and-you-cant-make-a-request}

To detect that a VM is unavailable, set a client timeout. All queries contain the client timeout parameters. The timeout value should be an order of magnitude greater than the expected query execution time.

