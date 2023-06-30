# TI — unified in-memory representation for Common Yandex Typesystem

Common Yandex Typesystem is a type system used across data storage and processing technologies developed by Yandex. A list of systems that support it includes YT, YDB, RTMR, YQL and others.

The Type Info library provides classes that represent types from Common Yandex Typesystem. It allows constructing in-memory representations of types, inspecting said representations, combining them to derive new types, as well as serializing and deserializing them.

For those familiar with Protobuf, Type Info implements the concept of message descriptors for Yandex type system.

Here you can find open issues: https://st.yandex-team.ru/YT/order:updated:false/filter?tags=type_info&resolution=empty()