#!/usr/bin/awk -f
# Filter perf script output by event type
# Sample usage: awk -f ./filter-perf-events.awk -v event=stalled-cycles-frontend <perf.data.dump

/^[^[:space:]]/ {
    match_event = match($0, event ":");
}

match_event {
    print
}

NF == 0 {
    match_event = 0
}
