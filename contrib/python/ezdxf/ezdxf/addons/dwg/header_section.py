# Copyright (c) 2020-2021, Manfred Moitzi
# License: MIT License
from typing import Dict, Any, List, Tuple
from abc import abstractmethod
import struct

from ezdxf.lldxf.const import acad_release_to_dxf_version
from ezdxf.tools.binarydata import BitStream

from .const import *
from .crc import crc8
from .fileheader import FileHeader


def load_header_section(specs: FileHeader, data: Bytes, crc_check=False):
    if specs.version <= ACAD_2000:
        return DwgHeaderSectionR2000(specs, data, crc_check)
    else:
        return DwgHeaderSectionR2004(specs, data, crc_check)


class DwgSectionLoader:
    def __init__(self, specs: FileHeader, data: Bytes, crc_check=False):
        self.specs = specs
        self.crc_check = crc_check
        self.data = self.load_data_section(data)

    @abstractmethod
    def load_data_section(self, data: Bytes) -> Bytes:
        ...


class DwgHeaderSectionR2000(DwgSectionLoader):
    def load_data_section(self, data: Bytes) -> Bytes:
        if self.specs.version > ACAD_2000:
            raise DwgVersionError(self.specs.version)
        seeker, section_size = self.specs.sections[HEADER_ID]
        return data[seeker : seeker + section_size]

    def load_header_vars(self) -> Dict:
        data = self.data
        sentinel = data[:16]
        if (
            sentinel
            != b"\xCF\x7B\x1F\x23\xFD\xDE\x38\xA9\x5F\x7C\x68\xB8\x4E\x6D\x33\x5F"
        ):
            raise DwgCorruptedHeaderSection(
                "Sentinel for start of HEADER section not found."
            )
        index = 16
        size = struct.unpack_from("<L", data, index)[0]
        index += 4
        bs = BitStream(
            data[index : index + size],
            dxfversion=self.specs.version,
            encoding=self.specs.encoding,
        )
        hdr_vars = parse_header(bs)
        index += size
        if self.crc_check:
            check = struct.unpack_from("<H", data, index)[0]
            # CRC of data from end of sentinel until start of crc value
            crc = crc8(data[16:-18], seed=0xC0C1)
            if check != crc:
                raise CRCError("CRC error in header section.")
        sentinel = data[-16:]
        if (
            sentinel
            != b"\x30\x84\xE0\xDC\x02\x21\xC7\x56\xA0\x83\x97\x47\xB1\x92\xCC\xA0"
        ):
            raise DwgCorruptedHeaderSection(
                "Sentinel for end of HEADER section not found."
            )
        return hdr_vars


class DwgHeaderSectionR2004(DwgHeaderSectionR2000):
    def load_data(self, data: Bytes) -> Bytes:
        raise NotImplementedError()


CMD_SET_VERSION = "ver"
CMD_SKIP_BITS = "skip_bits"
CMD_SKIP_NEXT_IF = "skip_next_if"
CMD_SET_VAR = "var"


def _min_max_versions(version: str) -> Tuple[str, str]:
    min_ver = ACAD_13
    max_ver = ACAD_LATEST
    if version != "all":
        v = version.split("-")
        if len(v) > 1:
            min_ver = acad_release_to_dxf_version[v[0].strip()]
            max_ver = acad_release_to_dxf_version[v[1].strip()]
        else:
            v_str: str = v[0].strip()
            if v_str[-1] == "+":
                min_ver = acad_release_to_dxf_version[v_str[:-1]]
            else:
                min_ver = max_ver = acad_release_to_dxf_version[v_str]
    return min_ver, max_ver


def load_commands(desc: str) -> List[Tuple[str, Any]]:
    commands = []
    lines = desc.split("\n")
    for line in lines:
        line = line.strip()
        if not line or line[0] == "#":
            continue
        try:
            command, param = line.split(":")
        except ValueError:
            raise ValueError(f"Unpack Error in line: {line}")
        command = command.strip()
        param = param.split("#")[0].strip()
        if command == CMD_SET_VERSION:
            commands.append((CMD_SET_VERSION, _min_max_versions(param)))
        elif command in {CMD_SKIP_BITS, CMD_SKIP_NEXT_IF}:
            commands.append((command, param))  # type: ignore
        elif command[0] == "$":
            commands.append((CMD_SET_VAR, (command, param)))
        else:
            raise ValueError(f"Unknown command: {command}")
    return commands


def parse_bitstream(
    bs: BitStream, commands: List[Tuple[str, Any]]
) -> Dict[str, Any]:
    version = bs.dxfversion
    min_ver = ACAD_13
    max_ver = ACAD_LATEST
    hdr_vars: Dict[str, Any] = dict()
    skip_next_cmd = False
    for cmd, params in commands:
        if skip_next_cmd:
            skip_next_cmd = False
            continue

        if cmd == CMD_SET_VERSION:
            min_ver, max_ver = params
        elif cmd == CMD_SKIP_BITS:
            bs.skip(int(params))
        elif cmd == CMD_SKIP_NEXT_IF:
            skip_next_cmd = eval(params, None, {"header": hdr_vars})
        elif cmd == CMD_SET_VAR:
            if min_ver <= version <= max_ver:
                name, code = params
                hdr_vars[name] = bs.read_code(code)
        else:
            raise ValueError(f"Unknown command: {cmd}")
    return hdr_vars


def parse_header(bs: BitStream) -> Dict[str, Any]:
    commands = load_commands(HEADER_DESCRIPTION)
    return parse_bitstream(bs, commands)


HEADER_DESCRIPTION = """
ver: R2007
$SIZE_IN_BITS: RL # Size in bits 

ver: R2013+ 
$REQUIREDVERSIONS: BLL # default value 0, read only 

ver: all
$UNKNOWN: BD # Unknown, default value 412148564080.0 
$UNKNOWN: BD # Unknown, default value 1.0 
$UNKNOWN: BD # Unknown, default value 1.0 
$UNKNOWN: BD # Unknown, default value 1.0 
$UNKNOWN: TV # Unknown text string, default "" 
$UNKNOWN: TV # Unknown text string, default "" 
$UNKNOWN: TV # Unknown text string, default "" 
$UNKNOWN: TV # Unknown text string, default "" 
$UNKNOWN: BL # Unknown long, default value 24L 
$UNKNOWN: BL # Unknown long, default value 0L; 

ver: R13-R14 
$UNKNOWN: BS # Unknown short, default value 0 

ver: R13-R2000 
$CURRENT_VIEWPORT_ENTITY_HEADER: H # Handle of the current viewport entity header (hard pointer) 

ver: all
$DIMASO: B 
$DIMSHO: B 

ver: R13-R14 
$DIMSAV: B # Undocumented 

ver: all
$PLINEGEN: B 
$ORTHOMODE: B 
$REGENMODE: B 
$FILLMODE: B 
$QTEXTMODE: B 
$PSLTSCALE: B 
$LIMCHECK: B 

ver: R13-R14 
$BLIPMODE: B 

ver: R2004+ 
$UNKNOWN: B #  Undocumented 

ver: all 
$USRTIMER: B #  (User timer on/off) 
$SKPOLY: B 
$ANGDIR: B 
$SPLFRAME: B 

ver: R13-R14 
$ATTREQ: B
$ATTDIA: B

ver: all 
$MIRRTEXT: B 
$WORLDVIEW: B

ver: R13-R14 
$WIREFRAME: B # Undocumented. 

ver: all
$TILEMODE: B 
$PLIMCHECK: B 
$VISRETAIN: B 

ver: R13-R14 
$DELOBJ: B

ver: all
$DISPSILH: B 
$PELLIPSE: B # (not present in DXF) 
$PROXYGRAPHICS: BS 

ver: R13-R14 
$DRAGMODE: BS 

ver: all 
$TREEDEPTH: BS 
$LUNITS: BS 
$LUPREC: BS 
$AUNITS: BS 
$AUPREC: BS 

ver: R13-R14 
$OSMODE: BS 

ver: all 
$ATTMODE: BS 

ver: R13-R14 
$COORDS: BS 

ver: all 
$PDMODE: BS 

ver: R13-R14 
$PICKSTYLE: BS 

ver: R2004+
$UNKNOWN: BL 
$UNKNOWN: BL 
$UNKNOWN: BL 

ver: all 
$USERI1: BS 
$USERI2: BS 
$USERI3: BS 
$USERI4: BS 
$USERI5: BS 
$SPLINESEGS: BS 
$SURFU: BS 
$SURFV: BS 
$SURFTYPE: BS 
$SURFTAB1: BS 
$SURFTAB2: BS 
$SPLINETYPE: BS 
$SHADEDGE: BS 
$SHADEDIF: BS 
$UNITMODE: BS 
$MAXACTVP: BS 
$ISOLINES: BS 
$CMLJUST: BS 
$TEXTQLTY: BS 
$LTSCALE: BD 
$TEXTSIZE: BD 
$TRACEWID: BD 
$SKETCHINC: BD 
$FILLETRAD: BD 
$THICKNESS: BD 
$ANGBASE: BD 
$PDSIZE: BD 
$PLINEWID: BD 
$USERR1: BD 
$USERR2: BD 
$USERR3: BD 
$USERR4: BD 
$USERR5: BD 
$CHAMFERA: BD 
$CHAMFERB: BD 
$CHAMFERC: BD 
$CHAMFERD: BD 
$FACETRES: BD 
$CMLSCALE: BD 
$CELTSCALE: BD 

ver: R13-R2004 
$MENUNAME: TV

ver: all 
$TDCREATE: BL # (Julian day) 
$TDCREATE: BL # (Milliseconds into the day) 
$TDUPDATE: BL # (Julian day) 
$TDUPDATE: BL # (Milliseconds into the day) 

ver: R2004+
$UNKNOWN: BL 
$UNKNOWN: BL 
$UNKNOWN: BL 

ver: all 
$TDINDWG: BL # (Days) 
$TDINDWG: BL # (Milliseconds into the day) 
$TDUSRTIMER: BL # (Days) 
$TDUSRTIMER: BL # (Milliseconds into the day) 
$CECOLOR: CMC

# with an 8-bit length specifier preceding the handle bytes (standard hex handle form) (code 0). 
# The HANDSEED is not part of the handle stream, but of the normal data stream (relevant for R21 and later). 

$HANDSEED: H # The next handle 
$CLAYER: H # (hard pointer) 
$TEXTSTYLE: H # (hard pointer) 
$CELTYPE: H # (hard pointer) 

ver: R2007+
$CMATERIAL: H # (hard pointer) 

ver: all 
$DIMSTYLE: H # (hard pointer) 
$CMLSTYLE: H # (hard pointer) 

ver: R2000+ 
$PSVPSCALE: BD 

ver: all 
$PINSBASE: 3BD # (PSPACE) 
$PEXTMIN: 3BD # (PSPACE) 
$PEXTMAX: 3BD # (PSPACE) 
$PLIMMIN: 2RD # (PSPACE) 
$PLIMMAX: 2RD # (PSPACE) 
$PELEVATION: BD # (PSPACE) 
$PUCSORG: 3BD # (PSPACE) 
$PUCSXDIR: 3BD # (PSPACE) 
$PUCSYDIR: 3BD # (PSPACE) 
$PUCSNAME: H # (PSPACE) (hard pointer) 

ver: R2000+
$PUCSORTHOREF: H # (hard pointer) 
$PUCSORTHOVIEW: BS 
$PUCSBASE: H # (hard pointer) 
$PUCSORGTOP: 3BD 
$PUCSORGBOTTOM: 3BD 
$PUCSORGLEFT: 3BD 
$PUCSORGRIGHT: 3BD 
$PUCSORGFRONT: 3BD 
$PUCSORGBACK: 3BD 

ver: all 
$INSBASE: 3BD # (MSPACE) 
$EXTMIN: 3BD # (MSPACE) 
$EXTMAX: 3BD # (MSPACE)
$LIMMIN: 2RD # (MSPACE) 
$LIMMAX: 2RD # (MSPACE) 
$ELEVATION: BD # (MSPACE) 
$UCSORG: 3BD # (MSPACE) 
$UCSXDIR: 3BD # (MSPACE) 
$UCSYDIR: 3BD # (MSPACE) 
$UCSNAME: H # (MSPACE) (hard pointer) 

ver: R2000+ 
$UCSORTHOREF: H # (hard pointer) 
$UCSORTHOVIEW: BS 
$UCSBASE: H # (hard pointer) 
$UCSORGTOP: 3BD 
$UCSORGBOTTOM: 3BD 
$UCSORGLEFT: 3BD 
$UCSORGRIGHT: 3BD 
$UCSORGFRONT: 3BD 
$UCSORGBACK: 3BD 
$DIMPOST: TV 
$DIMAPOST: TV 

ver: R13-R14
$DIMTOL: B 
$DIMLIM: B 
$DIMTIH: B 
$DIMTOH: B 
$DIMSE1: B 
$DIMSE2: B 
$DIMALT: B 
$DIMTOFL: B 
$DIMSAH: B 
$DIMTIX: B 
$DIMSOXD: B 
$DIMALTD: RC 
$DIMZIN: RC 
$DIMSD1: B 
$DIMSD2: B 
$DIMTOLJ: RC 
$DIMJUST: RC 
$DIMFIT: RC 
$DIMUPT: B 
$DIMTZIN: RC  
$DIMALTZ: RC 
$DIMALTTZ: RC 
$DIMTAD: RC 
$DIMUNIT: BS 
$DIMAUNIT: BS 
$DIMDEC: BS 
$DIMTDEC: BS 
$DIMALTU: BS 
$DIMALTTD: BS 
$DIMTXSTY: H # (hard pointer) 

ver: all 
$DIMSCALE: BD 
$DIMASZ: BD 
$DIMEXO: BD 
$DIMDLI: BD 
$DIMEXE: BD 
$DIMRND: BD 
$DIMDLE: BD 
$DIMTP: BD 
$DIMTM: BD 

ver: R2007+ 
$DIMFXL: BD 
$DIMJOGANG: BD 
$DIMTFILL: BS 
$DIMTFILLCLR: CMC 

ver: R2000+
$DIMTOL: B 
$DIMLIM: B 
$DIMTIH: B 
$DIMTOH: B 
$DIMSE1: B 
$DIMSE2: B 
$DIMTAD: BS 
$DIMZIN: BS 
$DIMAZIN: BS 

ver: R2007+ 
$DIMARCSYM: BS 

ver: all 
$DIMTXT: BD 
$DIMCEN: BD 
$DIMTSZ: BD 
$DIMALTF: BD 
$DIMLFAC: BD 
$DIMTVP: BD 
$DIMTFAC: BD 
$DIMGAP: BD 

ver: R13-R14 
$DIMPOST: T 
$DIMAPOST: T 
$DIMBLK: T 
$DIMBLK1: T 
$DIMBLK2: T 

ver: R2000+ 
$DIMALTRND: BD 
$DIMALT: B 
$DIMALTD: BS 
$DIMTOFL: B 
$DIMSAH: B 
$DIMTIX: B 
$DIMSOXD: B 

ver: all 
$DIMCLRD: CMC 
$DIMCLRE: CMC 
$DIMCLRT: CMC 

ver: R2000+
$DIMADEC: BS 
$DIMDEC: BS 
$DIMTDEC: BS 
$DIMALTU: BS 
$DIMALTTD: BS 
$DIMAUNIT: BS 
$DIMFRAC: BS 
$DIMLUNIT: BS 
$DIMDSEP: BS 
$DIMTMOVE: BS 
$DIMJUST: BS 
$DIMSD1: B 
$DIMSD2: B 
$DIMTOLJ: BS 
$DIMTZIN: BS 
$DIMALTZ: BS 
$DIMALTTZ: BS 
$DIMUPT: B 
$DIMATFIT: BS 

ver: R2007+
$DIMFXLON: B 

ver: R2010+ 
$DIMTXTDIRECTION: B 
$DIMALTMZF: BD 
$DIMALTMZS: T 
$DIMMZF: BD 
$DIMMZS: T 

ver: R2000+ 
$DIMTXSTY: H # (hard pointer) 
$DIMLDRBLK: H # (hard pointer) 
$DIMBLK: H # (hard pointer) 
$DIMBLK1: H # (hard pointer) 
$DIMBLK2: H # (hard pointer) 

ver: R2007+ 
$DIMLTYPE: H # (hard pointer) 
$DIMLTEX1: H # (hard pointer) 
$DIMLTEX2: H # (hard pointer) 

ver: R2000+ 
$DIMLWD: BS 
$DIMLWE: BS 

ver: all 
$BLOCK_CONTROL_OBJECT: H # (hard owner) Block Record Table
$LAYER_CONTROL_OBJECT: H # (hard owner) Layer Table
$STYLE_CONTROL_OBJECT: H # (hard owner) Style Table
$LINETYPE_CONTROL_OBJECT: H # (hard owner) Linetype Table
$VIEW_CONTROL_OBJECT: H # (hard owner) View table
$UCS_CONTROL_OBJECT: H # (hard owner)  UCS Table
$VPORT_CONTROL_OBJECT: H # (hard owner)  Viewport table
$APPID_CONTROL_OBJECT: H # (hard owner)  AppID Table
$DIMSTYLE_CONTROL_OBJECT: H # (hard owner)  Dimstyle Table

ver: R13-R2000 
$VIEWPORT_ENTITY_HEADER_CONTROL_OBJECT: H # (hard owner) 

ver: all 
$ACAD_GROUP_DICTIONARY: H # (hard pointer) 
$ACAD_MLINESTYLE_DICTIONARY: H # (hard pointer) 
$ROOT_DICTIONARY: H # (NAMED OBJECTS) (hard owner) 

ver: R2000+ 
$TSTACKALIGN: BS # default = 1 (not present in DXF) 
$TSTACKSIZE: BS #  default = 70 (not present in DXF) 
$HYPERLINKBASE: TV 
$STYLESHEET: TV 
$LAYOUTS_DICTIONARY: H # (hard pointer) 
$PLOTSETTINGS_DICTIONARY: H # (hard pointer) 
$PLOTSTYLES_DICTIONARY: H # (hard pointer) 

ver: R2004+ 
$MATERIALS_DICTIONARY: H # (hard pointer) 
$COLORS_DICTIONARY: H # (hard pointer) 

ver: R2007+ 
$VISUALSTYLE_DICTIONARY: H # (hard pointer) 

ver: R2013+
$UNKNOWN: H # (hard pointer) 

ver: R2000+
$R2000_PLUS_FLAGS: BL
#    CELWEIGHT Flags & 0x001F 
#    ENDCAPS Flags & 0x0060 
#    JOINSTYLE Flags & 0x0180 
#    LWDISPLAY !(Flags & 0x0200) 
#    XEDIT !(Flags & 0x0400) 
#    EXTNAMES Flags & 0x0800 
#    PSTYLEMODE Flags & 0x2000 
#    OLESTARTUP Flags & 0x4000 
$INSUNITS: BS 
$CEPSNTYPE: BS

skip_next_if: header['$CEPSNTYPE'] != 3
$CPSNID: H # (present only if CEPSNTYPE == 3) (hard pointer)

$FINGERPRINTGUID: TV 
$VERSIONGUID: TV 

ver: R2004+ 
$SORTENTS: RC 
$INDEXCTL: RC 
$HIDETEXT: RC 
$XCLIPFRAME: RC # before R2010 the value can be 0 or 1 only. 
$DIMASSOC: RC 
$HALOGAP: RC 
$OBSCUREDCOLOR: BS 
$INTERSECTIONCOLOR: BS 
$OBSCUREDLTYPE: RC 
$INTERSECTIONDISPLAY: RC 
$PROJECTNAME: TV 

ver: all 
$PAPER_SPACE_BLOCK_RECORD: H # (hard pointer) 
$MODEL_SPACE_BLOCK_RECORD: H #  (hard pointer) 
$BYLAYER_LTYPE: H # (hard pointer) 
$BYBLOCK_LTYPE: H # (hard pointer) 
$CONTINUOUS_LTYPE: H # (hard pointer) 

ver: R2007+ 
$CAMERADISPLAY: B 
$UNKNOWN: BL 
$UNKNOWN: BL 
$UNKNOWN: BD 
$STEPSPERSEC: BD 
$STEPSIZE: BD 
$3DDWFPREC: BD 
$LENSLENGTH: BD 
$CAMERAHEIGHT: BD 
$SOLIDHIST: RC 
$SHOWHIST: RC 
$PSOLWIDTH: BD 
$PSOLHEIGHT: BD 
$LOFTANG1: BD 
$LOFTANG2: BD 
$LOFTMAG1: BD 
$LOFTMAG2: BD 
$LOFTPARAM: BS 
$LOFTNORMALS: RC 
$LATITUDE: BD 
$LONGITUDE: BD 
$NORTHDIRECTION: BD 
$TIMEZONE: BL 
$LIGHTGLYPHDISPLAY: RC 
$TILEMODELIGHTSYNCH: RC 
$DWFFRAME: RC 
$DGNFRAME: RC 
$UNKNOWN: B 
$INTERFERECOLOR: CMC 
$INTERFEREOBJVS: H # (hard pointer) 
$INTERFEREVPVS: H # (hard pointer) 
$CSHADOW: RC 
$UNKNOWN: BD 

ver: R14+ 
$UNKNOWN: BS # short (type 5/6 only) these do not seem to be required, 
$UNKNOWN: BS # short (type 5/6 only) even for type 5. 
$UNKNOWN: BS # short (type 5/6 only) 
$UNKNOWN: BS # short (type 5/6 only) 

"""
