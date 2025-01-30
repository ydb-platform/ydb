The `dnsinfo.h` header was extracted from Apple's OpenSource repository:
[https://opensource.apple.com/source/configd/configd-453.19/dnsinfo/dnsinfo.h](https://opensource.apple.com/source/configd/configd-453.19/dnsinfo/dnsinfo.h)

We then had to make a few edits to this file:
1. Add `AvailabilityMacros.h` header file
2. conditionalize `reach_flags` in `dns_resolver_t` on MacOS 10.8 or higher, in
   order to maintain compatibility with the last MacOS PPC release, 10.6.
3. conditionalize `_dns_configuration_ack()` on MacOS 10.8 or higher.
4. Update parameter list to `(void)` for both `dns_configuration_notify_key()`
   and `dns_configuration_copy()` to sidestep compiler warnings in this old
   header.

We had tried initially to use the latest 1109.140.1 which only worked on
MacOS 11+, then downgraded to 963.50.8 for MacOS 10.8+ support, then finally
to 453.19 with additional patches.

This is needed to call into `dns_configuration_copy()` and
`dns_configuration_free()`.
