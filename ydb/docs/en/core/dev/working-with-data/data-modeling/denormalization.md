# Data denormalization: balancing speed and structure

## Problem

Strict data normalization (especially beyond 3NF) in {{ ydb-short-name }} causes several critical problems:

* The number of `JOIN` operations in queries grows sharply — each extra normalization level requires joining tables, which significantly slows query execution.
* The load on the transaction system increases: multiple `JOIN`s cause frequent locks (`Transaction Locks Invalidated`), because transactions touch more tables and rows.
* Queries become more complex — they are harder to write, debug, and maintain, which raises the risk of errors.

## Solution: reasonable denormalization down to 3NF

To keep {{ ydb-short-name }} performant, it is recommended to:

1. Limit normalization to **3NF** — this is enough to remove redundancy without a critical hit to speed.
2. Combine related entities into one table when they are often queried together. For example, instead of separate `Users` and `Addresses` tables, create a single `Users_with_addresses` table if the address is almost always needed together with the user data.
3. Add duplicate fields (for example, `category_name` in the products table) to avoid a `JOIN` with the categories table.
4. Use materialized views for complex aggregates that are queried often.

### Key principle

Optimize the structure for real queries: if a certain set of data is often retrieved together, store it together. This reduces the number of `JOIN`s, decreases locking, and speeds up the system.
