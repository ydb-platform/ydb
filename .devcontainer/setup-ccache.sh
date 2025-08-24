#!/bin/sh
WORKSPACE_FOLDER=$1

sudo ln -s /usr/local/bin/ccache /usr/local/bin/gcc
sudo ln -s /usr/local/bin/ccache /usr/local/bin/g++
ccache -o remote_storage="http://cachesrv.ydb.tech:8080|read-only|layout=bazel"
ccache -o sloppiness=locale
ccache -o base_dir=$WORKSPACE_FOLDER
