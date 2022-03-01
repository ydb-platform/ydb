# Enabling/disabling Scrubbing

The Scrub settings let you adjust the interval from the beginning of the previous disk scrubbing cycle to that of the next one and the maximum number of disks that can be scrubbed simultaneously. The default value is 1 month.
`$ kikimr admin bs config invoke --proto 'Command { UpdateSettings { ScrubPeriodicitySeconds: 86400 MaxScrubbedDisksAtOnce: 1 } }'`

If ScrubPeriodicitySeconds is 0, Scrubbing is disabled.

