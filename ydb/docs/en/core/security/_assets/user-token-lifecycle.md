```mermaid
sequenceDiagram
    actor user as User
    participant node as YDB node
    participant cache as Node cache
    participant auth as Authentication subsystem

    user->>node: Request with the same data
    node->>cache: Find record
    cache-->>node: Record found
    Note right of cache: life_time countdown restarts

    opt Time to update
        node->>auth: Re-verify the authentication token
        alt Successful verification
            auth-->>node: Verification result
            node->>node: Create a new user token
            node->>cache: Update record
        else Retryable error
            auth-->>node: Error
            Note right of node: Schedule a retry
        else Permanent error
            auth-->>node: Error
            node->>cache: Stop using the user token
        end
    end

    alt Record not used within life_time or expired
        node->>cache: Delete record
    end
```
