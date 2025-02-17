#!/usr/bin/env bash

gperf -m10 aliases_nocjk.gperf | sed 's/register //g' >aliases_nocjk.h

cat aliases_nocjk.gperf | sed 1d | grep -v ^% | uniq -f1 | cut -d, -f1 | head -n -2 | xargs -n1 -IXXX grep '"XXX"' aliases_nocjk.h | grep -Eo "_str[0-9]+" | sed 's/^/  (int)(long)\&((struct stringpool_t *)0)->stringpool/;s/$/,/' >canonical_nocjk.h

cat aliases_nocjk.gperf | sed 1d | grep -v ^% | uniq -f1 | cut -d, -f1 | tail -n 2 | xargs -n1 -IXXX grep '"XXX"' aliases_nocjk.h | grep -Eo "_str[0-9]+" | sed 's/^/  (int)(long)\&((struct stringpool_t *)0)->stringpool/;s/$/,/' >canonical_local_nocjk.h
