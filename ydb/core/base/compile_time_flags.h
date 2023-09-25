#pragma once

// This file is used for enabling or disabling dangerous kikimr features
// These flags may only be changed at compile time

// This feature enables tablet to serialize bundle deltas in part switches
// It may only be enabled after 19-2 or newer is deployed everywhere
#ifndef KIKIMR_TABLET_WRITE_BUNDLE_DELTAS
#define KIKIMR_TABLET_WRITE_BUNDLE_DELTAS 1
#endif

// This feature disables storing of meta blobs in borrow messages
// It may only be enabled after 19-4 or newer is deployed everywhere
#ifndef KIKIMR_TABLET_BORROW_WITHOUT_META
#define KIKIMR_TABLET_BORROW_WITHOUT_META 1
#endif

// This feature enables VDisk SyncLog to write entry point in protobuf format
// We expect that stable-19-4 will understand entry point protobuf format,
// stable-19-6 will use protobuf format by default (i.e. write entry point
// in this format)
#ifndef KIKIMR_VDISK_SYNCLOG_ENTRY_POINT_PROTO_FORMAT
#define KIKIMR_VDISK_SYNCLOG_ENTRY_POINT_PROTO_FORMAT 0
#endif

// This feature flag enables rope payload for protobuf events and may be switched
// on after 19-6
#ifndef KIKIMR_USE_PROTOBUF_WITH_PAYLOAD
#define KIKIMR_USE_PROTOBUF_WITH_PAYLOAD 1
#endif

// This feature flag enables use of column families in tables
// Runtime support is expected to ship in 19-6, may be enabled in 19-8
#ifndef KIKIMR_SCHEMESHARD_ALLOW_COLUMN_FAMILIES
#define KIKIMR_SCHEMESHARD_ALLOW_COLUMN_FAMILIES 1
#endif

// This feature flag enables use of flow controlled queue in statestorage lookup requests
#ifndef KIKIMR_ALLOW_FLOWCONTROLLED_QUEUE_FOR_SSLOOKUP
#define KIKIMR_ALLOW_FLOWCONTROLLED_QUEUE_FOR_SSLOOKUP 0
#endif

// This feature flag enables immediate ReadTable on datashard
// Runtime support shipped in 20-2, will be enabled in 20-4
#ifndef KIKIMR_ALLOW_READTABLE_IMMEDIATE
#define KIKIMR_ALLOW_READTABLE_IMMEDIATE 1
#endif

// This feature flag enables statestorage replica probes
#ifndef KIKIMR_ALLOW_SSREPLICA_PROBES
#define KIKIMR_ALLOW_SSREPLICA_PROBES 0
#endif

// This feature enables cutting PDisk's log from the middle of log chunks list
#ifndef KIKIMR_PDISK_ENABLE_CUT_LOG_FROM_THE_MIDDLE
#define KIKIMR_PDISK_ENABLE_CUT_LOG_FROM_THE_MIDDLE true
#endif

// This feature flag enables PDisk to use t1ha hash in sector footer checksums
#ifndef KIKIMR_PDISK_ENABLE_T1HA_HASH_WRITING
#define KIKIMR_PDISK_ENABLE_T1HA_HASH_WRITING true
#endif
