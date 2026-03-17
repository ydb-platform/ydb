#!/bin/sh

if [ "$(uname)" = "Darwin" ]; then
    DEFAULT_PATH="/opt/homebrew/sbin"
else
    DEFAULT_PATH="/usr/lib/rabbitmq/bin"
fi

find_rabbitmq() {
    RABBITMQ_SERVER=
    for dir in "$TESTSUITE_RABBITMQ_BINDIR" "$DEFAULT_PATH"; do
        if [ -x "$dir/rabbitmq-server" ]; then
          RABBITMQ_SERVER="$dir/rabbitmq-server"
          break
        fi
    done
    if [ "x$RABBITMQ_SERVER" = "x" ]; then
        die "No rabbitmq-server script found.
Please install RabbitMQ following official instructions at
https://www.rabbitmq.com/download.html
or set TESTSUITE_RABBITMQ_BINDIR environment variable to the directory containing
actual rabbitmq scripts (it's usually /usr/lib/rabbitmq/bin/). Note that this directory
differs from \"which rabbitmq-server\",
actual location can be found via inspecting sbin/rabbitmq-server script.
"
    fi

    RABBITMQ_CTL=
    for dir in "$TESTSUITE_RABBITMQ_BINDIR" "$DEFAULT_PATH"; do
        if [ -x "$dir/rabbitmqctl" ]; then
          RABBITMQ_CTL="$dir/rabbitmqctl"
          break
        fi
    done
    if [ "x$RABBITMQ_CTL" = "x" ]; then
        die "No rabbitmqctl script found.
Please install RabbitMQ following official instructions at
https://www.rabbitmq.com/download.html
or set TESTSUITE_RABBITMQ_BINDIR environment variable to the directory containing
actual rabbitmq scripts (it's usually /usr/lib/rabbitmq/bin/). Note that this directory
differs from \"which rabbitmqctl\",
actual location can be found via inspecting sbin/rabbitmqctl script.
"
    fi

    echo "Rabbitmq server: $RABBITMQ_SERVER"
    echo "Rabbitmq ctl: $RABBITMQ_CTL"

    return 0
}

find_rabbitmq || die "RabbitMQ is not found."
