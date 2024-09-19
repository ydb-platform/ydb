## For a docker image [`ydbplatform/local-ydb`](https://hub.docker.com/r/ydbplatform/local-ydb), the following naming rules apply for tags

* **latest** - corresponds to the *stable* version of YDB that was tested on well-known YDB production clusters. The **latest** tag is rebuilt when an entry about a new YDB release appears in [github.com/ydb-platform/ydb](https://github.com/ydb-platform/ydb/releases) project.
* **edge** - a candidate for the next *stable*, i.e. the version that is currently being tested. You can try out new YDB features in edge, but you should also not expect this build to be stable.
* **trunk,** **main**, **nightly** - the latest version of YDB from the code in the main YDB development branch. Contains the most recent changes. The image is reassembled every night.
* **XX.Y** - corresponds to the latest version of YDB in a major release **XX-Y** (with all patches included).
* **XX.Y.ZZ** - corresponds to the YDB release version **XX-Y-ZZ**.
* **XX.Y-slim** and **XX.Y.ZZ-slim** - corresponds to YDB versions with specially compressed binaries `ydbd` \+ `ydb` (*cli*) inside the image. The \***-slim** versions take a little longer to start, although they have a much smaller image size. For compression, the [upx](https://github.com/upx/upx) utility is used, which can cause problems in certain startup environments.
