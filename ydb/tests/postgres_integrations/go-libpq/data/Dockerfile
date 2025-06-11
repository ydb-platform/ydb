# For docker context at root git directory

FROM ghcr.io/ydb-platform/golang:1.20-amd64

WORKDIR /project/sources/

COPY patch.diff /patch.diff
COPY docker-init.bash /docker-init.bash
RUN /docker-init.bash

COPY common-go-scripts/go-run-separate-tests.bash /go-run-separate-tests.bash

COPY docker-start.bash /docker-start.bash

CMD [ "/docker-start.bash" ]
