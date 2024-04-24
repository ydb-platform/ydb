## Multistep transactions {#multistep-transactions}

Multiple commands are executed within a single multistep transaction. The client-side code can be run between query executions. Using a transaction ensures that select queries made in its context are consistent with each other.

