#!/bin/bash
set -e

if [ -x /bin/systemctl ]; then
  systemctl kill --signal=SIGHUP rsyslog.service
else
  /sbin/reload rsyslog
fi
