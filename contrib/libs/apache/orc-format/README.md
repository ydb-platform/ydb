<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# [Apache ORC](https://orc.apache.org/)

ORC is a self-describing type-aware columnar file format designed for
Hadoop workloads. It is optimized for large streaming reads, but with
integrated support for finding required rows quickly. Storing data in
a columnar format lets the reader read, decompress, and process only
the values that are required for the current query. Because ORC files
are type-aware, the writer chooses the most appropriate encoding for
the type and builds an internal index as the file is written.
Predicate pushdown uses those indexes to determine which stripes in a
file need to be read for a particular query and the row indexes can
narrow the search to a particular set of 10,000 rows. ORC supports the
complete set of types in Hive, including the complex types: structs,
lists, maps, and unions.

## ORC Format

This project includes ORC specifications and the protobuf definition.
`Apache ORC Format 1.0.0` is desinged to be used for `Apache ORC 2.0+`.

Releases:
* Maven Central: <a href="https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.orc%22">![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.orc/orc/badge.svg)</a>
* Downloads: <a href="https://orc.apache.org/downloads">Apache ORC downloads</a>
* Release tags: <a href="https://github.com/apache/orc-format/releases">Apache ORC Format releases</a>
* Plan: <a href="https://github.com/apache/orc-format/milestones">Apache ORC Format future release plan</a>

The current build status:
* Main branch <a href="https://github.com/apache/orc-format/actions/workflows/build_and_test.yml?query=branch%3Amain">
  ![main build status](https://github.com/apache/orc-format/actions/workflows/build_and_test.yml/badge.svg?branch=main)</a>

Bug tracking: <a href="https://github.com/apache/orc-format/issues">Apache ORC Format Issues</a>

## Building

```
./mvnw install
```
