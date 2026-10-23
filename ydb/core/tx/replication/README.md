# Replication implementation notes

## CDC payload compression

CDC payload compression is disabled by default. To enable it, set the source
`DataShardConfig.CdcPayloadCompression` to `CDC_PAYLOAD_COMPRESSION_GZIP`.
A source sender uses GZIP only when all of these conditions hold:

- The stream uses the replication JSON format.
- Its private `__async_replication` attribute explicitly declares
  `"supported_payload_codecs": ["gzip"]`.
- The topic codec policy permits GZIP. An absent or empty allowlist is
  unrestricted; a nonempty allowlist must contain stored codec ID 1.

New replication streams advertise the destination reader capability when the
stream is created, before initial scan or consumer creation. The advertisement
is independent of `TransferInternalDataDecompression`: both destination decoding
modes support GZIP. Existing streams keep their existing attributes and remain
RAW without an explicit declaration; this change does not migrate them.

The source setting is sampled when sender actors resolve the stream/topic and
create partition senders. Recreate senders, or restart source nodes, when changing
this setting. Disabling compression changes future writes after recreation;
already stored GZIP remains readable.

Only serialized data bodies are eligible. JSON types, String base64 encoding,
Utf8 text, partition keys, source IDs, sequencing, timestamps and other metadata
retain their existing meanings. Schema changes and resolved heartbeats stay RAW.
Bodies smaller than 1024 bytes or larger than the signed 32-bit uncompressed-size
limit stay RAW. The encoder uses standard GZIP at quality 1 and accepts a result
only when it saves at least `max(64, original_bytes / 8)` bytes. Small,
incompressible or insufficiently compressible bodies therefore keep their
original RAW encoding. Ordinary codec failures also fall back to RAW.

A compressed topic envelope uses stored GZIP codec ID 1, corresponding to public
topic GZIP codec ID 2. `UncompressedSize` is the original serialized JSON byte
length. Existing RAW commands keep their previous accounting. Topic SDK readers
normally decode the body; readers with `.Decompress(false)` receive the standard
per-message codec and original length. Mixed RAW/GZIP history is supported.

Older sources ignore the private capability and continue writing RAW. New sources
write RAW for destinations that do not declare it.

Actual-binary compatibility tests in `ydb/tests/compatibility/topics` passed against
`stable-26-4-1` (commit `c7e4ce6c7ff3bc86e1f9368ed43cf686911cfa8c`, executable
SHA-256 `07d66274f2aa9ffd5fb30e9f5b5fea1da7d804758934dd242a6ddffce855bb94`).
Both SDK and internal destination decoders preserve exact rows, mixed RAW/GZIP
message bodies and metadata, and committed consumer offsets through source and
destination rollback and re-upgrade. The tests also verify current-source GZIP
production to an old destination and continued old-source RAW production on the
same topic. Existing streams keep their creation-time capability attributes.

The GLOBAL case applies a schema barrier while the destination is current, then
checks schema, GZIP consumption, checkpoints and replicated-table write rejection
through two old destination starts. It does not certify fresh schema alterations
applied by the old destination, which has a preexisting schema serialization
oneof defect, or every historical reader release.

The original compression implementation passed 35 component cases. The integration
snapshot passed the two decoder restart cases and one schema rollback case above
(three historical cases total). The standalone delivery snapshot has not been
separately recompiled, and final post-compatibility independent review remains
pending. These results establish the stated test scope, not blanket N/N−1
certification.

Compression reduces eligible payload and envelope bytes. It does not impose an
end-to-end disk-occupancy limit or remove all storage write amplification.
Device-I/O improvement requires a separate measured workload.

The compression unit/integration fixtures measured the serialized bytes below
with the implemented quality-1 encoder. These examples are repetitive inputs,
not an estimate for other workloads:

| Fixture | RAW body | GZIP body | RAW envelope | GZIP envelope |
| --- | ---: | ---: | ---: | ---: |
| 16 KiB repetitive source JSON | 16,417 | 145 | 16,424 | 151 |
| Typed JSON with Utf8, binary String and metadata | 16,677 | 351 | 16,684 | 357 |

The envelope totals include the existing topic message framing. Device writes
and disk occupancy require separate workload measurements.
