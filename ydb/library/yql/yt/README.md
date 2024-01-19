### What is this?

This directory contains targets related to YTsaurus YQL plugin. [YTsaurus](https://github.com/ytsaurus/ytsaurus)
is an open-source big data storage system which employs YQL engine as one of its primary computation engines.
YT relies on a so-called YQL plugin shared library, which is built from target located in dynamic/ directory.

Changes of interfaces in this directory must be done very carefuly. In general, bridge/interface.h must be a
superset of 
[ytsaurus:yt/yql/plugin/bridge/interface.h](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yql/plugin/bridge/interface.h).
If a backwards incompatible change in interface is required, do not forget to promote the YQL plugin version
in this repository, and change the min/max supported YQL plugin 
[versions](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yql/plugin/bridge/plugin.cpp#L32-L41) in YTsaurus repository.

If you are not 100% sure about what you are doing, contact vvvv@ydb.tech and mpereskokova@ytsaurus.tech for help.

