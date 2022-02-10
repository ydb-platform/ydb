#!/bin/sh -e
# $Id: trace.sh 2313967 2016-05-24 12:52:38Z serxa $
# $HeadURL: svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia/library/cpp/lwtrace/mon/trace.sh $

usage() {
    if [ -n "$*" ]; then
        echo "ERROR: $*" >&2
    else
        echo "Script for LWTrace tracing management" >&2
    fi
    echo "USAGE: `basename $0` COMMAND TRACEID HOST:PORT PARAM1=VALUE1 ..." >&2
    echo "COMMANDS:" >&2
    echo "     new  Start new tracing described by query from STDIN," >&2
    echo "          uniquely named with TRACEID string," >&2
    echo "          using HOST:PORT monitoring service." >&2
    echo "          query can contain \$params that are substituted with corresponding values from cmdline" >&2
    echo "          (alse \$\$ is replaced by \$, each param must be defined from cmdline)" >&2
    echo "  delete  Stop existing tracing with specified name TRACEID," >&2
    echo "          on HOST:PORT monitoring service." >&2
    exit 1
}

COMMAND=$1
ID="$2"
IDENC="$(perl -MURI::Escape -e '$id = $ARGV[0]; $id=uri_escape($id); $id =~ s{[+%]}{-}g; print $id;' "$ID")"
ADDRESS="$3"
if [ "$COMMAND" = "--help" ] || [ -z "$*" ]; then usage; fi
if [ -z "$ID" ]; then usage "TRACEID is not specified"; fi
if [ -z "$ADDRESS" ]; then usage "HOST:PORT is not specified"; fi

shift 3

STATUS=0
ERRFILE=/var/tmp/lwtrace.sh.$$

error() {
    echo "ERROR: $*" >&2
    [ ! -e $ERRFILE ] || cat $ERRFILE >&2
    exit 1
}

stop() { rm -f $ERRFILE; }
trap stop INT ABRT EXIT

case "$COMMAND" in

    new)
        QUERY="$(perl -e 'use MIME::Base64;
                         local $/;
                         $a = <STDIN>;
                         for $arg (@ARGV) {
                            ($k,$v) = split "=",$arg,2;
                            $a =~ s{\$$k}{$v}g;
                         }
                         if ($a =~ /\$([A-Za-z_][\w_]*)/) {
                            print STDERR "undefined param in lwtrace query: $1\n";
                            exit 0
                         }
                         $a =~ s{\$\$}{\$}g;
                         print encode_base64($a, "");
                         ' "$@" 2>$ERRFILE)"
        if [ -z "$QUERY" ]; then error "lwtrace query errors"; fi
        wget --post-data="id=$IDENC&query=$QUERY" \
            -O - http://$ADDRESS/trace?mode=new </dev/null 2>$ERRFILE || STATUS=$?
        if [ $STATUS -ne 0 ]; then error "wget failure"; fi
        ;;

    delete)
        wget --post-data="id=$IDENC" \
            -O - http://$ADDRESS/trace?mode=delete </dev/null 2>$ERRFILE || STATUS=$?
        if [ $STATUS -ne 0 ]; then error "wget failure"; fi
        ;;

    *)
        echo "usage: `basename $0` new TRACEID ADDRESS < query.txt" >&2
        echo "       `basename $0` delete TRACEID ADDRESS" >&2
        exit 1
        ;;

esac
echo "Done"
