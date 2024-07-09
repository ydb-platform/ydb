#pragma once

// This feature enables VDisk SyncLog to write entry point in protobuf format
// We expect that stable-19-4 will understand entry point protobuf format,
// stable-19-6 will use protobuf format by default (i.e. write entry point
// in this format)
#ifndef KIKIMR_VDISK_SYNCLOG_ENTRY_POINT_PROTO_FORMAT
#define KIKIMR_VDISK_SYNCLOG_ENTRY_POINT_PROTO_FORMAT 0
#endif

// This feature flag enables use of flow controlled queue in statestorage lookup requests
#ifndef KIKIMR_ALLOW_FLOWCONTROLLED_QUEUE_FOR_SSLOOKUP
#define KIKIMR_ALLOW_FLOWCONTROLLED_QUEUE_FOR_SSLOOKUP 0
#endif

// This feature flag enables statestorage replica probes
#ifndef KIKIMR_ALLOW_SSREPLICA_PROBES
#define KIKIMR_ALLOW_SSREPLICA_PROBES 0
#endif
// This feature flag enables PDisk to use t1ha hash in sector footer checksums
#ifndef KIKIMR_PDISK_ENABLE_T1HA_HASH_WRITING
#define KIKIMR_PDISK_ENABLE_T1HA_HASH_WRITING true
#endif
