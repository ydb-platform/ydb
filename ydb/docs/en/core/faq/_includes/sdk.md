# SDK

#### What may cause the error <q>Status: OVERLOADED Error: Pending previous query completion</q> in the C++ SDK?

Q: When running two queries, I try to get a response from the future method of the second one. It returns: ```Status: OVERLOADED Why: <main>: Error: Pending previous query completion```

A: Sessions in the SDK are single-threaded. To run multiple queries at once, you need to create multiple sessions.

#### What should I do if the SDK crashes when shutting down an application? {#what-to-do-if-the-sdk-crashes-urgently-when-the-app-is-shut-down}

Make sure not to wrap SDK components in a singleton, since their lifetime shouldn't exceed the execution time of the `main()` function. When a client is destroyed, session pools are emptied, so network navigation is required. But gRPC contains global static variables that might already be destroyed by this time. This disables gRPC.
If you need to declare a driver as a global object, invoke the `Stop(true)` function on the driver before exiting the `main()` function.

#### What should I do if, when using a `fork()` system call, a program does not work properly in a child process? {#program-does-not-work-correctly-when-calling-fork}

Using `fork()` in multithreaded applications is an antipattern. Since both the SDK and the gRPC library are multithreaded applications, their stability is not guaranteed.

#### What should I do if I get the error "Active sessions limit exceeded" even though the current number of active sessions is within the limit? {#active-sessions-does-not-exceed-the-limit}

The limit applies to the number of active sessions. An active session is a session passed to the client to be used in its code. A session is returned to the pool in a destructor. In this case, the session itself is a replicated object. You may have saved a copy of the session in the code.

#### Is it possible to make queries to different databases from the same application? {#make-requests-to-different-databases-from-the-same-application}

Yes, the C++ SDK lets you override the DB parameters and token when creating a client. There is no need to create separate drivers.

#### What should I do if a VM has failed and it's impossible to make a query? {#vms-failed-and-you-cant-make-a-request}

To detect the unavailability of a VM, set a client timeout. All queries contain the client timeout parameters. The timeout value should be an order of magnitude greater than the expected query execution time.

