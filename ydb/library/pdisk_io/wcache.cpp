#include "wcache.h"
//#include <ydb/library/actors/core/log.h>
//#include <ydb/library/services/services.pb.h>

#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/strip.h>
#include <util/system/file.h>

#ifdef _linux_
#include <libgen.h>
#include <limits.h>
#include <linux/fs.h>
#include <linux/nvme_ioctl.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
//#include <linux/hdreg.h>
#define HDIO_GET_WCACHE         0x030e  /* get write cache mode on|off */
#define HDIO_SET_WCACHE         0x032b  /* change write cache enable-disable */
#endif

#include <regex>

namespace NKikimr {
namespace NPDisk {

#ifndef _linux_
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Compatibility version
EWriteCacheResult FlushWriteCache(FHANDLE file, const TString &path, TStringStream *outDetails) {
    Y_UNUSED(file);
    if (outDetails) {
        (*outDetails) << "FlushWriteCache is not implemented, path# \"" << path << "\"";
    }
    return WriteCacheResultErrorPersistent;
}

std::optional<TDriveData> GetDriveData(const TString &path, TStringStream *outDetails) {
    if (outDetails) {
        (*outDetails) << "GetDriveData is not implemented, path# \"" << path << "\"";
    }
    return std::nullopt;
}

EWriteCacheResult GetWriteCache(FHANDLE file, const TString &path, TDriveData *outDriveData, TStringStream *outDetails) {
    Y_ABORT_UNLESS(outDriveData);
    Y_UNUSED(file);
    if (outDetails) {
        (*outDetails) << "GetWriteCache is not implemented, path# \"" << path << "\"";
    }
    *outDriveData = TDriveData{};

    return WriteCacheResultErrorPersistent;
}

EWriteCacheResult SetWriteCache(FHANDLE file, const TString &path, bool isEnable, TStringStream *outDetails) {
    Y_UNUSED(file);
    if (outDetails) {
        (*outDetails) << "SetWriteCache is not implemented, path# \"" << path << "\" isEnable# " << isEnable;
    }
    return WriteCacheResultErrorPersistent;
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#else

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Linux version

////////////////////////////////////////////////////////////////////////////////
// form linux headers
typedef struct sg_io_hdr
{
  int interface_id;           /* [i] 'S' for SCSI generic (required) */
  int dxfer_direction;        /* [i] data transfer direction  */
  unsigned char cmd_len;      /* [i] SCSI command length ( <= 16 bytes) */
  unsigned char mx_sb_len;    /* [i] max length to write to sbp */
  unsigned short int iovec_count; /* [i] 0 implies no scatter gather */
  unsigned int dxfer_len;     /* [i] byte count of data transfer */
  void * dxferp;              /* [i], [*io] points to data transfer memory or scatter gather list */
  unsigned char * cmdp;       /* [i], [*i] points to command to perform */
  unsigned char * sbp;        /* [i], [*o] points to sense_buffer memory */
  unsigned int timeout;       /* [i] MAX_UINT->no timeout (unit: millisec) */
  unsigned int flags;         /* [i] 0 -> default, see SG_FLAG... */
  int pack_id;                /* [i->o] unused internally (normally) */
  void * usr_ptr;             /* [i->o] unused internally */
  unsigned char status;       /* [o] scsi status */
  unsigned char masked_status;/* [o] shifted, masked scsi status */
  unsigned char msg_status;   /* [o] messaging level data (optional) */
  unsigned char sb_len_wr;    /* [o] byte count actually written to sbp */
  unsigned short int host_status; /* [o] errors from host adapter */
  unsigned short int driver_status;/* [o] errors from software driver */
  int resid;                  /* [o] dxfer_len - actual_transferred */
  unsigned int duration;      /* [o] time taken by cmd (unit: millisec) */
  unsigned int info;          /* [o] auxiliary information */
} sg_io_hdr_t;

/* Use negative values to flag difference from original sg_header structure.  */
#define SG_DXFER_NONE -1        /* e.g. a SCSI Test Unit Ready command */
#define SG_DXFER_TO_DEV -2      /* e.g. a SCSI WRITE command */
#define SG_DXFER_FROM_DEV -3    /* e.g. a SCSI READ command */
#define SG_DXFER_TO_FROM_DEV -4 /* treated like SG_DXFER_FROM_DEV with the additional property than during indirect
                                   IO the user buffer is copied into the kernel buffers before the transfer */

#define SG_CHECK_CONDITION 0x02
#define SG_DRIVER_SENSE 0x08

//
////////////////////////////////////////////////////////////////////////////////

#define SG_IO 0x2285

enum EAtaOperation {
    ATA_OP_PIDENTIFY = 0xa1,
    ATA_OP_FLUSHCACHE = 0xe7,
    ATA_OP_FLUSHCACHE_EXT = 0xea,
    ATA_OP_IDENTIFY = 0xec,
    ATA_OP_SETFEATURES = 0xef
};

enum ESatlTLength {
    TL_NO_DATA_TRANSFERED = 0,
    TL_LENGTH_IN_FEATURE = 1,
    TL_LENGTH_IN_SECTOR_COUNT = 2,
    TL_LENGTH_IN_STPSIU = 3
};

enum EAtaProtocol {
    AP_NON_DATA = 3,
    AP_PIO_IN = 4,
    AP_PIO_OUT = 5,
    AP_DMA = 6,
    AP_UDMA_IN = 11,
    AP_UDMA_OUT = 12
};

struct TAtaPassThroughDevice {
    union {
        struct {
            ui8 CommandSpecific0 : 4;
            ui8 Dev : 1;
            ui8 Obsolete5 : 1;
            ui8 ComandSpecific6Lba : 1;
            ui8 Obsolete7 : 1;
        };
        ui8 Raw;
    };
};
static_assert(sizeof(TAtaPassThroughDevice) == sizeof(TAtaPassThroughDevice::Raw),
    "Size mismatch, see Ata Command Pass-Through documentation.");

struct TAtaStatusRegister {
    union {
        struct {
            ui8 ErrorCheckCondition : 1;  // ERR, 1 => Error occurred during execution of the previous command.
                                          // For ATAPI devices, this is called CHK and indicates a SCSI Status of
                                          // CHECK CONDITION.
            ui8 Unknown1 : 2;
            ui8 DataRequest : 1;  // 1 => DRQ, Device is ready for data transfer.
            ui8 Unknown4 : 1;
            ui8 DeviceFault : 1;  // 1 => Device had a major error processing a command.
            ui8 DeviceReady : 1;  // 0 => Device only accepts DEVICE RESET, EXECUTE DEVICE DIAGNOSTIC
                                  //      IDENTIFY PACKET DEVICE, PACKET.
                                  // 1 => Device is ready to accept all commands.
            ui8 Busy : 1;  // 0 => Host controls the task file, device is not busy.
                           // 1 => Device controls the task file, host can only write DEVICE RESET to task file.
                           //      If host reads the task file, only the Busy bit is valid.
        };
        ui8 Raw;
    };
};
static_assert(sizeof(TAtaStatusRegister) == sizeof(TAtaStatusRegister::Raw),
    "Size mismatch.");


struct TAtaPassThroughCdbHeader {
    // byte 0
    ui8 OperationCode;  // 0x85

    // byte 1
    ui8 Extend : 1;  // Reserved1 in Ata12
    ui8 Protocol : 4;
    ui8 Multiplecount : 3;

    // byte 2
    ui8 TLength : 2;
    ui8 BytBlock : 1;  // 0 => Bytes, 1 => Secors
    ui8 TDir : 1;  // 0 => To device, 1 => From device
    ui8 Reserved2 : 1;
    ui8 CkCond : 1;
    ui8 OffLine : 2;
};

struct TAtaPassThrough12Cdb {
    union {
        struct {
            // bytes 0:2
            TAtaPassThroughCdbHeader Header; // OperationCode = 0xA1

            ui8 Features;  // byte 3
            ui8 SectorCount;  // byte 4
            ui8 LbaLow;  // byte 5
            ui8 LbaMid;  // byte 6
            ui8 LbaHigh;  // byte 7
            TAtaPassThroughDevice Device;  // byte 8
            ui8 Command;  // byte 9
            ui8 Reserved10;  // byte 10
            ui8 Control;  // byte 11
        };
        ui8 Raw[12];
    };

    void Clear() {
        memset(Raw, 0, sizeof(Raw));
    }
};
static_assert(sizeof(TAtaPassThrough12Cdb) == sizeof(TAtaPassThrough12Cdb::Raw),
    "Size mismatch, see Ata Command Pass-Through documentation.");

struct TAtaPassThrough16Cdb {
    union {
        struct {
            // bytes 0:2
            TAtaPassThroughCdbHeader Header; // OperationCode = 0x85

            ui8 FeaturesHigh;  // byte 3
            ui8 Features;  // byte 4
            ui8 SectorCountHigh;  // byte 5
            ui8 SectorCount;  // byte 6
            ui8 LbaLowHigh;  // byte 7
            ui8 LbaLow;  // byte 8
            ui8 LbaMidHigh;  // byte 9
            ui8 LbaMid;  // byte 10
            ui8 LbaHighHigh;  // byte 11
            ui8 LbaHigh;  // byte 12
            TAtaPassThroughDevice Device;  // byte 13
            ui8 Command;  // byte 14
            ui8 Control;  // byte 15
        };
        ui8 Raw[16];
    };

    void Clear() {
        memset(Raw, 0, sizeof(Raw));
    }
};
static_assert(sizeof(TAtaPassThrough16Cdb) == sizeof(TAtaPassThrough16Cdb::Raw),
    "Size mismatch, see Ata Command Pass-Through documentation.");

struct TExtendedAtaStatusReturnDescriptor {
    union {
        struct {
            ui8 DescriptorCode;  // 0x09
            ui8 AdditionalDescriptorLength;  // 0x0c
            ui8 Reserved2 : 7;
            ui8 Extend : 1;
            ui8 Error;
            ui8 SectorCountHigh;
            ui8 SectorCount;
            ui8 LbaLowHigh;
            ui8 LbaLow;
            ui8 LbaMidHigh;
            ui8 LbaMid;
            ui8 LbaHighHigh;
            ui8 LbaHigh;
            ui8 Device;
            TAtaStatusRegister Status;
        };
        ui8 Raw[14];
    };

    void Clear() {
        memset(Raw, 0, sizeof(Raw));
    }
};
static_assert(sizeof(TExtendedAtaStatusReturnDescriptor) == sizeof(TExtendedAtaStatusReturnDescriptor::Raw),
    "Size mismatch, see Ata Command Pass-Through documentation.");

struct TSb {
    union {
        struct {
            ui8 SbHeadRaw[8];
            TExtendedAtaStatusReturnDescriptor Desc;
        };
        ui8 Raw[32];
    };

    void Clear() {
        memset(Raw, 0, sizeof(Raw));
    }
};
static_assert(sizeof(TSb) == sizeof(TSb::Raw), "Size mismatch.");

enum EAtaOperationCode {
    AOC_ATA_12 = 0xa1,
    AOC_ATA_16 = 0x85
};

EWriteCacheResult AtaPassThrough(FHANDLE file, ui8 command, ui8 features, ui8 dataCount, ui8 *data, bool preferAta12,
        ui32 timeoutMs, TStringStream *outDetails) {
    Y_ABORT_UNLESS(!dataCount || data);
    ui32 dataBytes = dataCount * 512;
    ui8 isLba48 = (command == ATA_OP_FLUSHCACHE_EXT || command == ATA_OP_PIDENTIFY);
    union {
        TAtaPassThrough12Cdb Ata12;
        TAtaPassThrough16Cdb Ata16;
        TAtaPassThroughCdbHeader Header;
    } cdb;
    cdb.Ata16.Clear();
    TSb sb;
    sb.Clear();
    sg_io_hdr_t ioHdr;
    memset(&ioHdr, 0, sizeof(ioHdr));
    if (preferAta12 && !isLba48) {
        cdb.Header.OperationCode = AOC_ATA_12;
        cdb.Ata12.SectorCount = dataCount;
        cdb.Ata12.Device.ComandSpecific6Lba = 1;
        cdb.Ata12.Command = command;
        cdb.Ata12.Features = features;
        ioHdr.cmd_len = sizeof(cdb.Ata12);
        ioHdr.cmdp = cdb.Ata12.Raw;
    } else {
        cdb.Header.OperationCode = AOC_ATA_16;
        cdb.Ata16.SectorCount = dataCount;
        cdb.Ata16.Device.ComandSpecific6Lba = 1;
        cdb.Ata16.Command = command;
        cdb.Ata16.Features = features;
        cdb.Header.Extend = (isLba48 ? 1 : 0);
        ioHdr.cmd_len = sizeof(cdb.Ata16);
        ioHdr.cmdp = cdb.Ata16.Raw;
    }

    if (data) {
        memset(data, 0, dataBytes);
        cdb.Header.Protocol = AP_PIO_IN;
        cdb.Header.TLength = TL_LENGTH_IN_SECTOR_COUNT;
        cdb.Header.BytBlock = 1;
        cdb.Header.TDir = 1;
        ioHdr.dxfer_direction = SG_DXFER_FROM_DEV;
    } else {
        cdb.Header.Protocol = AP_NON_DATA;
        cdb.Header.CkCond = 1;
        ioHdr.dxfer_direction = SG_DXFER_NONE;
    }

    ioHdr.interface_id = 'S';  // 'S' for SCSI generic
    ioHdr.mx_sb_len = sizeof(sb);
    ioHdr.dxfer_len = dataBytes;
    ioHdr.dxferp = data;
    ioHdr.sbp = sb.Raw;
    ioHdr.timeout = timeoutMs;


    if (ioctl(file, SG_IO, &ioHdr) == -1) {
        if (outDetails) {
            (*outDetails) << "SG_IO not supported (?)";
        }
        return WriteCacheResultErrorPersistent;
    }
    if (ioHdr.status && ioHdr.status != SG_CHECK_CONDITION) {
        if (outDetails) {
            (*outDetails) << "SG_IO: bad status# " << (ui32)ioHdr.status;
        }
        return WriteCacheResultErrorTemporary;
    }
    if (ioHdr.host_status) {
        if (outDetails) {
            (*outDetails) << "SG_IO: bad host status# " << (ui32)ioHdr.host_status;
        }
        return WriteCacheResultErrorTemporary;
    }
    if (ioHdr.driver_status && (ioHdr.driver_status != SG_DRIVER_SENSE)) {
        if (outDetails) {
            (*outDetails) << "SG_IO: bad driver_status# " << (ui32)ioHdr.driver_status;
        }
        return WriteCacheResultErrorPersistent;
    }
    if (sb.Desc.Status.ErrorCheckCondition || sb.Desc.Status.DataRequest) {
        if (ioHdr.driver_status != SG_DRIVER_SENSE) {
            if (sb.Raw[0] | sb.Raw[1] | sb.Raw[2] | sb.Raw[3] | sb.Raw[4] |
                    sb.Raw[5] | sb.Raw[6] | sb.Raw[7] | sb.Raw[8] | sb.Raw[9]) {
                if (outDetails) {
                    (*outDetails) << "SG_IO: questionable sense data, results may be incorrect.";
                }
                return WriteCacheResultErrorPersistent;
            }
        } else if (sb.Raw[0] != 0x72 || sb.Raw[7] < 14 ||
                sb.Desc.DescriptorCode != 0x09 || sb.Desc.AdditionalDescriptorLength < 0x0c) {
            if (outDetails) {
                (*outDetails) << "SG_IO: bad/missing sense data.";
            }
        }
        if (outDetails) {
            (*outDetails) << "I/O error, command# " << (ui32)command << " status# " << (ui32)sb.Desc.Status.Raw
                << "error# " << (ui32)sb.Desc.Error;
        }
        return WriteCacheResultErrorTemporary;
    }
    return WriteCacheResultOk;
}

enum EThreeValuedLogic {
    TVL_FALSE = 0,
    TVL_TRUE = 1,
    TVL_UNKNOWABLE = 2
};

struct TIdentifyData {
    static const ui32 IdentifySizeBytes = 512;
    bool IsGathered = false;
    ui8 Data[IdentifySizeBytes];
    // Offset in words, descirption, size bytes for strings
    // 10, serial number, 20 ASCII
    // 23, firmware revision, 8 ASCII
    // 27, Model number, 40 ASCII
    // 75, 4 bits: maximum queue depth - 1
    //
    // 82 bit 5  - write cache support.
    // 83 bit 12 - flush cache support.
    // 83 bit 13 - flush cache ext supported
    // 83 bit 14 - 1 and bitt 15 - 0 => words 82 and 83 are valid
    //
    // 84 bit 14 - 1 and bit 15 - 0 => word 84 is valid
    //
    // 85, bit 5  - write cache enabled.
    // 87 bit 14 - 1 and bit 15 - 0 => words 85:87 is valid
    ui16 *Id = nullptr;
    bool IsAta12 = true;

    bool IsKnowable(ui32 wordIdx) const {
        Y_ABORT_UNLESS(Id);
        return ((Id[wordIdx] & 0xc000) == 0x4000);
    }

    TString GetString(ui32 offsetWords, ui32 sizeBytes) const {
        Y_ABORT_UNLESS(Id);
        TString string;
        string.resize(sizeBytes);
        char *dst = string.Detach();
        char *src = reinterpret_cast<char*>(&Id[offsetWords]);
        Y_ABORT_UNLESS((sizeBytes & 1) == 0);
        for (ui32 i = 0; i < sizeBytes; i += 2) {
            dst[i + 0] = src[i + 1];
            dst[i + 1] = src[i + 0];
        }
        return StripString(string);
    }

    TString GetSerialNumber() const {
        return GetString(10, 20);
    }

    TString GetFirmwareRevision() const {
        return GetString(23, 8);
    }

    TString GetModelNumber() const {
        return GetString(27, 40);
    }

    EDeviceType GetDeviceType() const {
        // "nominal media rotation rate" of HDD devices, equals to 1 for SSD
        if (Id[217] > 0x401) {
            return DEVICE_TYPE_ROT;
        } else if (Id[217] == 1) {
            return DEVICE_TYPE_SSD;
        } else {
            return DEVICE_TYPE_UNKNOWN;
        }
    }

    EThreeValuedLogic Is3WriteCacheSuppored() const {
        if (IsKnowable(83)) {
            return ((Id[82] & (1 << 5)) ? TVL_TRUE : TVL_FALSE);
        }
        return TVL_UNKNOWABLE;
    }

    EThreeValuedLogic Is3FlushCacheSuppored() const {
        if (IsKnowable(83)) {
            return ((Id[83] & (1 << 12)) ? TVL_TRUE : TVL_FALSE);
        }
        return TVL_UNKNOWABLE;
    }

    EThreeValuedLogic Is3FlushCacheExtSuppored() const {
        if (IsKnowable(83)) {
            return ((Id[83] & (1 << 13)) ? TVL_TRUE : TVL_FALSE);
        }
        return TVL_UNKNOWABLE;
    }

    EThreeValuedLogic Is3WriteCacheEnabled() const {
        if (IsKnowable(87)) {
            return ((Id[85] & (1 << 5)) ? TVL_TRUE : TVL_FALSE);
        }
        return TVL_UNKNOWABLE;
    }

    void ToHostByteOrder() {
        ui16 *id = (ui16*)(void*)Data;
        const ui32 size = IdentifySizeBytes / sizeof(ui16);
        for (ui32 i = 82; i < size; ++i) {
            id[i] = le16toh(id[i]);
        }
    }

    EWriteCacheResult Gather(FHANDLE file, TStringStream *outDetails) {
        if (IsGathered) {
            return WriteCacheResultOk;
        }
        EWriteCacheResult res1 = AtaPassThrough(file, ATA_OP_IDENTIFY, 0, 1, Data, true, 60000, outDetails);
        if (res1 != WriteCacheResultOk) {
            IsAta12 = false;
            EWriteCacheResult res2 = AtaPassThrough(file, ATA_OP_PIDENTIFY, 0, 1, Data, false, 60000, outDetails);
            if (res2 != WriteCacheResultOk) {
                if (outDetails) {
                    (*outDetails) << "GetIdentifyData failed both ATA_OP_IDENTIFY and ATA_OP_PIDENTIFY.";
                }
                if (res1 == WriteCacheResultErrorPersistent && res2 == WriteCacheResultErrorPersistent) {
                    return WriteCacheResultErrorPersistent;
                } else {
                    return WriteCacheResultErrorTemporary;
                }
            }
        }
        ToHostByteOrder();
        IsGathered = true;
        Id = (ui16*)(void*)Data;
        return WriteCacheResultOk;
    }
};

EWriteCacheResult FlushWriteCache(FHANDLE file, const TString &path, TStringStream *outDetails) {
    TIdentifyData identify;
    EWriteCacheResult res = identify.Gather(file, outDetails);
    if (res != WriteCacheResultOk) {
        if (outDetails) {
            (*outDetails) << "FlushWriteCache failed, path# \"" << path << "\"";
        }
        return res;
    }
    Y_ABORT_UNLESS(identify.Id);
    bool useExt = (identify.Is3FlushCacheExtSuppored() == TVL_TRUE);
    res = AtaPassThrough(file, useExt ? ATA_OP_FLUSHCACHE_EXT : ATA_OP_FLUSHCACHE,
            0, 0, nullptr, useExt, 60000, outDetails);
    if (res != WriteCacheResultOk) {
        if (outDetails) {
            (*outDetails) << "FlushWriteCache failed, path# \"" << path << "\"";
            (*outDetails) << " op# " << (useExt ? "ATA_OP_FLUSHCACHE_EXT" : "ATA_OP_FLUSHCACHE");
            // TODO(cthulhu): output TVL result here
        }
        return res;
    }
    return WriteCacheResultOk;
}

////////////////////////////////////////////////////////////////////////////////
// NVMe admin command
////////////////////////////////////////////////////////////////////////////////

static constexpr ui64 NVME_ADMIN_IDENTIFY_OPCODE = 0x06;
static constexpr ui64 NVME_IDENTIFY_DATA_SIZE = 4096;

static constexpr ui64 NVME_ID_MODEL_NUMBER_OFFSET = 4;
static constexpr ui64 NVME_ID_MODEL_NUMBER_SIZE = 20;
static constexpr ui64 NVME_ID_SERIAL_NUMBER_OFFSET = 24;
static constexpr ui64 NVME_ID_SERIAL_NUMBER_SIZE = 40;
static constexpr ui64 NVME_ID_FIRMWARE_REVISION_OFFSET = 64;
static constexpr ui64 NVME_ID_FIRMWARE_REVISION_SIZE = 8;

static TArrayHolder<char> GetNvmeIdentifyStruct(int fd, TStringStream *outDetails)
{
    TArrayHolder<char> id_ctrl_buffer{new char[NVME_IDENTIFY_DATA_SIZE]};
    memset(id_ctrl_buffer.Get(), 0, NVME_IDENTIFY_DATA_SIZE);
    struct nvme_admin_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_admin_cmd));

    cmd.opcode = NVME_ADMIN_IDENTIFY_OPCODE;
    cmd.addr = reinterpret_cast<__u64>(id_ctrl_buffer.Get());
    cmd.data_len = NVME_IDENTIFY_DATA_SIZE;
    cmd.nsid = 0;
    //  bits 31:16 -- Controller Identifier (CNTID)
    //  bits 15:08 -- Reserved
    //  bits 07:00 -- Controller or Namespace Structure (CNS).
    cmd.cdw10 = 0x01; // CNS == 01h -- Identify Controller data structure for the controller processing the command.
    //  bits 31:16 -- Reserved
    //  bits 15:00 -- NVM Set Identifier (NVMSETID). This field is used for Identify operations with a CNS value of 04h
    cmd.cdw11 = 0;

    if (ioctl(fd, NVME_IOCTL_ADMIN_CMD, &cmd) == 0) {
        return id_ctrl_buffer;
    } else {
        if (outDetails) {
            *outDetails  << "error in NVME_IOCTL_ADMIN_CMD ioctl, errno# " << errno << " strerror# " << strerror(errno);
        }
        return nullptr;
    }
}

static TString RenderCharField(const char *s, size_t n) {
    TString str(s, n);
    if (size_t end = str.find('\0'); end != TString::npos) {
        str.resize(end);
    }
    return StripString(str);
}

static std::optional<TDriveData> GetNvmeDriveData(int fd, TStringStream *outDetails) {
    if (TArrayHolder<char> id_data = GetNvmeIdentifyStruct(fd, outDetails)) {
        TDriveData data;
        data.IsWriteCacheValid = false;
        const char *id = id_data.Get();
        data.SerialNumber = RenderCharField(id + NVME_ID_SERIAL_NUMBER_OFFSET, NVME_ID_SERIAL_NUMBER_SIZE);
        data.ModelNumber = RenderCharField(id + NVME_ID_MODEL_NUMBER_OFFSET, NVME_ID_MODEL_NUMBER_SIZE);
        data.FirmwareRevision = RenderCharField(id + NVME_ID_FIRMWARE_REVISION_OFFSET, NVME_ID_FIRMWARE_REVISION_SIZE);
        data.DeviceType = DEVICE_TYPE_NVME;
        return data;
    } else {
        return std::nullopt;
    }
}

static std::optional<TDriveData> GetSysfsDriveData(const TString &path, TStringStream *outDetails) {
    char realPath[PATH_MAX];
    char *res = realpath(path.Data(), realPath);
    if (res == NULL) {
        if (errno == ENOENT) {
            ythrow TFileError() << "no such file# " << path;
        }
        *outDetails << "erron in realpath(), details# " << strerror(errno);
        return std::nullopt;
    }
    std::regex name_regex(R"__(nvme\d+n\d+)__");
    std::cmatch match;
    if (!std::regex_search(realPath, match, name_regex)) {
        *outDetails << "regex_search failed, realPath# " << realPath;
        return std::nullopt;
    }
    if (match.size() != 1) {
        *outDetails << "regex_match size not equals to 1, realPath# " << realPath;
        return std::nullopt;
    }
    TString nvme_sysfs = TStringBuilder() << "/sys/block/" << match[0].str() << "/";

    auto readField = [&] (TStringBuf subpath) {
        return StripString(TFileInput(nvme_sysfs + subpath).ReadAll());
    };

    try {
        TDriveData data;
        data.IsWriteCacheValid = false;
        data.SerialNumber = readField("device/serial");
        data.ModelNumber = readField("device/model");
        data.FirmwareRevision = readField("device/firmware_rev");
        data.DeviceType = DEVICE_TYPE_NVME;
        return data;
    } catch (const TFileError& err) {
        *outDetails << "can't open sysfs files, caught TFileError, what# " << err.what();
        return std::nullopt;
    }
}

std::optional<TDriveData> GetDriveData(const TString &path, TStringStream *outDetails) {
    try {
        TFile f(path, OpenExisting | RdOnly);
        TDriveData data;
        long size = 0;
        if (ioctl(f.GetHandle(), BLKGETSIZE, &size) == 0) {
            data.Size = size << 9;
        }
        EWriteCacheResult res = GetWriteCache(f.GetHandle(), path, &data, outDetails);
        if (res == EWriteCacheResult::WriteCacheResultOk) {
            data.Path = path;
            return data;
        }
        *outDetails << "; ";
        if (std::optional<TDriveData> nvmeData = GetSysfsDriveData(path, outDetails)) {
            nvmeData->Path = path;
            nvmeData->Size = nvmeData->Size ? nvmeData->Size : data.Size;
            return nvmeData;
        }
        *outDetails << "; ";
        if (std::optional<TDriveData> nvmeData = GetNvmeDriveData(f.GetHandle(), outDetails)) {
            nvmeData->Path = path;
            nvmeData->Size = nvmeData->Size ? nvmeData->Size : data.Size;
            return nvmeData;
        }
        return std::nullopt;
    } catch (const TFileError& err) {
        *outDetails << "caught TFileError, what# " << err.what();
        return std::nullopt;
    }
}

EWriteCacheResult GetWriteCache(FHANDLE file, const TString &path, TDriveData *outDriveData,
        TStringStream *outDetails) {
    Y_ABORT_UNLESS(outDriveData);
    TIdentifyData identify;
    EWriteCacheResult res = identify.Gather(file, outDetails);
    if (res != WriteCacheResultOk) {
        if (outDetails) {
            (*outDetails) << "GetWriteCache failed, path# \"" << path << "\"";
        }
        return res;
    }
    Y_ABORT_UNLESS(identify.Id);
    EThreeValuedLogic wcache = identify.Is3WriteCacheEnabled();
    if (wcache == TVL_UNKNOWABLE) {
        (*outDetails) << "GetWriteCache failed, write cache state is unknowable, path# \"" << path << "\"";

        return WriteCacheResultErrorPersistent;
    }
    outDriveData->Path = path;
    outDriveData->IsWriteCacheValid = true;
    outDriveData->IsWriteCacheEnabled = (wcache == TVL_TRUE);
    outDriveData->SerialNumber = identify.GetSerialNumber();
    outDriveData->FirmwareRevision = identify.GetFirmwareRevision();
    outDriveData->ModelNumber = identify.GetModelNumber();
    outDriveData->DeviceType = identify.GetDeviceType();
    return WriteCacheResultOk;
}

EWriteCacheResult SetWriteCache(FHANDLE file, const TString &path, bool isEnable, TStringStream *outDetails) {
    ui8 features = (isEnable ? 0x02 : 0x82);  // 0x02 and 0x82 are magic numbers from ATA specificaton 6.49.8
    EWriteCacheResult res = AtaPassThrough(file, ATA_OP_SETFEATURES, features, 0, nullptr, false, 60000, outDetails);
    if (res != WriteCacheResultOk) {
        (*outDetails) << "SetWriteCache failed, path# \"" << path << "\" isEnable# " << isEnable;
        return res;
    }
    return WriteCacheResultOk;
}

#endif

/*
bool GetWriteCache(TFileHandle *file, TActorSystem *actorSystem, const TString &path, bool *outIsEnabled) {
    *outIsEnabled = false;
#ifdef _linux_
    int val = 0;
    errno = 0;
//    ui8 setcache[4] = {ATA_OP_SETFEATURES,0,0,0};
//    setcache[2] = wcache ? 0x02 : 0x82;
    if (ioctl((FHANDLE)*file, HDIO_GET_WCACHE, &val)) {
//    if (ioctl((FHANDLE)*file, HDIO_DRIVE_CMD, &val)) {
        int errorId = errno;
        if (errorId == EOPNOTSUPP) {
            if (actorSystem) {
                LOG_WARN_S(*actorSystem, NKikimrServices::BS_DEVICE,
                    "HDIO_GET_WCACHE failed, operation not supported path# \"" << path << "\"");
            } else {
                Cerr << "HDIO_GET_WCACHE failed, operation not supported path# \"" << path << "\"" << Endl;
            }
            return false;
        } else if (errorId == ENOTTY) {
            if (actorSystem) {
                LOG_WARN_S(*actorSystem, NKikimrServices::BS_DEVICE,
                    "HDIO_GET_WCACHE failed, device is not a typewriter! path# \"" << path << "\"");
            } else {
                Cerr << "HDIO_GET_WCACHE failed, device is not a typewriter! path# \"" << path << "\"" << Endl;
            }
            return false;
        } else {
            if (actorSystem) {
                LOG_WARN_S(*actorSystem, NKikimrServices::BS_DEVICE,
                    "HDIO_GET_WCACHE failed with errno# " << errorId << " path# \"" << path << "\"");
            } else {
                Cerr << "HDIO_GET_WCACHE failed with errno# " << errorId << " path# \"" << path << "\"" << Endl;
            }
            return false;
        }
    } else {
        if (actorSystem) {
            LOG_INFO_S(*actorSystem, NKikimrServices::BS_DEVICE, "HDIO_GET_WCACHE val# " << val
                << " path# \"" << path << "\"");
        }
        *outIsEnabled = bool(val);
        return true;
    }
#else
    Y_UNUSED(file);
    Y_UNUSED(actorSystem);
    Y_UNUSED(path);
    return false;
#endif
}

bool SetWriteCache(TFileHandle *file, bool isWriteCacheEnable, TActorSystem *actorSystem, const TString &path) {
#ifdef _linux_
    // TODO: Consider flushing the cache before disabling it
    errno = 0;
    int val = (isWriteCacheEnable ? 1 : 0);
    if (ioctl((FHANDLE)*file, HDIO_SET_WCACHE, val)) {
        int errorId = errno;
        if (errorId == EOPNOTSUPP) {
            if (actorSystem) {
                LOG_WARN_S(*actorSystem, NKikimrServices::BS_DEVICE,
                    "HDIO_SET_WCACHE failed, operation not supported path# \"" << path << "\"");
            } else {
                Cerr << "HDIO_SET_WCACHE failed, operation not supported path# \"" << path << "\"" << Endl;
            }
            return false;
        } else if (errorId == ENOTTY) {
            if (actorSystem) {
                LOG_WARN_S(*actorSystem, NKikimrServices::BS_DEVICE,
                    "HDIO_SET_WCACHE failed, device is not a typewriter! path# \"" << path << "\"");
            } else {
                Cerr << "HDIO_SET_WCACHE failed, device is not a typewriter! path# \"" << path << "\"" << Endl;
            }
            return false;
        } else if (errorId == EACCES) {
            if (actorSystem) {
                LOG_WARN_S(*actorSystem, NKikimrServices::BS_DEVICE,
                    "HDIO_SET_WCACHE failed, no access rights, CAP_SYS_ADMIN needed! path# \"" << path << "\"");
            } else {
                Cerr << "HDIO_SET_WCACHE failed, no access rights, CAP_SYS_ADMIN needed! path# \""
                    << path << "\"" << Endl;
            }
            return false;
        } else {
            if (actorSystem) {
                LOG_WARN_S(*actorSystem, NKikimrServices::BS_DEVICE,
                    "HDIO_SET_WCACHE failed with errno# " << errorId << " path# \"" << path << "\"");
            } else {
                Cerr << "HDIO_SET_WCACHE failed with errno# " << errorId << " path# \"" << path << "\"" << Endl;
            }
            return false;
        }
    } else {
        if (actorSystem) {
            LOG_INFO_S(*actorSystem, NKikimrServices::BS_DEVICE, "HDIO_SET_WCACHE setVal# " << val
                << " path# \"" << path << "\"");
        }
        return true;
    }
#else
    Y_UNUSED(file);
    Y_UNUSED(isWriteCacheEnabled);
    Y_UNUSED(actorSystem);
    Y_UNUSED(path);
    return false;
#endif
}
*/

} // NPDisk
} // NKikimr
