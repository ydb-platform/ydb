#include "fake_llvm_symbolizer.h"

#include <contrib/libs/llvm12/lib/DebugInfo/Symbolize/SymbolizableObjectFile.h>
#include <util/string/builder.h>
#include <llvm/DebugInfo/Symbolize/DIPrinter.h>
#include <llvm/DebugInfo/Symbolize/SymbolizableModule.h>
#include <llvm/DebugInfo/DWARF/DWARFContext.h>
#include <llvm/Object/Binary.h>
#include <llvm/Object/ObjectFile.h>
#include <llvm/Object/ELFObjectFile.h>
#include <llvm/Object/COFF.h>
#include <llvm/Object/MachO.h>
#include <llvm/Object/MachOUniversal.h>
#include <llvm/Demangle/Demangle.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/CRC.h>
#include <llvm/Support/raw_ostream.h>
#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <sstream>
#include <utility>
#include <vector>

namespace NYql {
namespace NBacktrace {
namespace {

using ObjectPair = std::pair<const llvm::object::ObjectFile *, const llvm::object::ObjectFile *>;
std::map<std::string, std::unique_ptr<llvm::symbolize::SymbolizableModule>, std::less<>>
    Modules;
std::map<std::pair<std::string, std::string>, ObjectPair>
    ObjectPairForPathArch;
std::map<std::string, llvm::object::OwningBinary<llvm::object::Binary>> BinaryForPath;
std::map<std::pair<std::string, std::string>, std::unique_ptr<llvm::object::ObjectFile>>
      ObjectForUBPathAndArch;

// Fuctions are from llvm's Symbolize.cpp
llvm::Expected<llvm::object::ObjectFile *> GetOrCreateObject(const std::string &Path, const std::string &ArchName) {
    llvm::object::Binary *Bin;
    auto Pair = BinaryForPath.emplace(Path, llvm::object::OwningBinary<llvm::object::Binary>());
    if (!Pair.second) {
        Bin = Pair.first->second.getBinary();
    } else {
        llvm::Expected<llvm::object::OwningBinary<llvm::object::Binary>> BinOrErr = llvm::object::createBinary(Path);
        if (!BinOrErr)
        return BinOrErr.takeError();
        Pair.first->second = std::move(BinOrErr.get());
        Bin = Pair.first->second.getBinary();
    }

    if (!Bin)
        return static_cast<llvm::object::ObjectFile *>(nullptr);

    if (llvm::object::MachOUniversalBinary *UB = llvm::dyn_cast_or_null<llvm::object::MachOUniversalBinary>(Bin)) {
        auto I = ObjectForUBPathAndArch.find(std::make_pair(Path, ArchName));
        if (I != ObjectForUBPathAndArch.end())
        return I->second.get();

        llvm::Expected<std::unique_ptr<llvm::object::ObjectFile>> ObjOrErr =
            UB->getMachOObjectForArch(ArchName);
        if (!ObjOrErr) {
        ObjectForUBPathAndArch.emplace(std::make_pair(Path, ArchName),
                                        std::unique_ptr<llvm::object::ObjectFile>());
        return ObjOrErr.takeError();
        }
        llvm::object::ObjectFile *Res = ObjOrErr->get();
        ObjectForUBPathAndArch.emplace(std::make_pair(Path, ArchName),
                                    std::move(ObjOrErr.get()));
        return Res;
    }
    if (Bin->isObject()) {
        return llvm::cast<llvm::object::ObjectFile>(Bin);
    }
    return llvm::errorCodeToError(llvm::object::object_error::arch_not_found);
}

bool CheckFileCRC(llvm::StringRef Path, uint32_t CRCHash) {
    llvm::ErrorOr<std::unique_ptr<llvm::MemoryBuffer>> MB =
        llvm::MemoryBuffer::getFileOrSTDIN(Path);
    if (!MB)
        return false;
    return CRCHash == llvm::crc32(arrayRefFromStringRef(MB.get()->getBuffer()));
}

bool FindDebugBinary(const std::string &OrigPath, const std::string &DebuglinkName, uint32_t CRCHash, const std::string &FallbackDebugPath, std::string &Result) {
    llvm::SmallString<16> OrigDir(OrigPath);
    llvm::sys::path::remove_filename(OrigDir);
    llvm::SmallString<16> DebugPath = OrigDir;
    // Try relative/path/to/original_binary/debuglink_name
    llvm::sys::path::append(DebugPath, DebuglinkName);
    if (CheckFileCRC(DebugPath, CRCHash)) {
        Result = std::string(DebugPath.str());
        return true;
    }
    // Try relative/path/to/original_binary/.debug/debuglink_name
    DebugPath = OrigDir;
    llvm::sys::path::append(DebugPath, ".debug", DebuglinkName);
    if (CheckFileCRC(DebugPath, CRCHash)) {
        Result = std::string(DebugPath.str());
        return true;
    }
    // Make the path absolute so that lookups will go to
    // "/usr/lib/debug/full/path/to/debug", not
    // "/usr/lib/debug/to/debug"
    llvm::sys::fs::make_absolute(OrigDir);
    if (!FallbackDebugPath.empty()) {
        // Try <FallbackDebugPath>/absolute/path/to/original_binary/debuglink_name
        DebugPath = FallbackDebugPath;
    } else {
    #if defined(__NetBSD__)
        // Try /usr/libdata/debug/absolute/path/to/original_binary/debuglink_name
        DebugPath = "/usr/libdata/debug";
    #else
        // Try /usr/lib/debug/absolute/path/to/original_binary/debuglink_name
        DebugPath = "/usr/lib/debug";
    #endif
    }
    llvm::sys::path::append(DebugPath, llvm::sys::path::relative_path(OrigDir),
                            DebuglinkName);
    if (CheckFileCRC(DebugPath, CRCHash)) {
        Result = std::string(DebugPath.str());
        return true;
    }
    return false;
}

template <typename ELFT>
llvm::Optional<llvm::ArrayRef<uint8_t>> GetBuildID(const llvm::object::ELFFile<ELFT> &Obj) {
    auto PhdrsOrErr = Obj.program_headers();
    if (!PhdrsOrErr) {
        consumeError(PhdrsOrErr.takeError());
        return {};
    }
    for (const auto &P : *PhdrsOrErr) {
        if (P.p_type != llvm::ELF::PT_NOTE)
        continue;
        llvm::Error Err = llvm::Error::success();
        for (auto N : Obj.notes(P, Err))
        if (N.getType() == llvm::ELF::NT_GNU_BUILD_ID && N.getName() == llvm::ELF::ELF_NOTE_GNU)
            return N.getDesc();
        consumeError(std::move(Err));
    }
    return {};
}

llvm::Optional<llvm::ArrayRef<uint8_t>> GetBuildID(const llvm::object::ELFObjectFileBase *Obj) {
    llvm::Optional<llvm::ArrayRef<uint8_t>> BuildID;
    if (auto *O = llvm::dyn_cast<llvm::object::ELFObjectFile<llvm::object::ELF32LE>>(Obj))
        BuildID = GetBuildID(O->getELFFile());
    else if (auto *O = llvm::dyn_cast<llvm::object::ELFObjectFile<llvm::object::ELF32BE>>(Obj))
        BuildID = GetBuildID(O->getELFFile());
    else if (auto *O = llvm::dyn_cast<llvm::object::ELFObjectFile<llvm::object::ELF64LE>>(Obj))
        BuildID = GetBuildID(O->getELFFile());
    else if (auto *O = llvm::dyn_cast<llvm::object::ELFObjectFile<llvm::object::ELF64BE>>(Obj))
        BuildID = GetBuildID(O->getELFFile());
    else
        llvm_unreachable("unsupported file format");
    return BuildID;
}

std::string GetDarwinDWARFResourceForPath(const std::string &Path, const std::string &Basename) {
    llvm::SmallString<16> ResourceName = llvm::StringRef(Path);
    if (llvm::sys::path::extension(Path) != ".dSYM") {
        ResourceName += ".dSYM";
    }
    llvm::sys::path::append(ResourceName, "Contents", "Resources", "DWARF");
    llvm::sys::path::append(ResourceName, Basename);
    return std::string(ResourceName.str());
}

bool DarwinDsymMatchesBinary(const llvm::object::MachOObjectFile *DbgObj, const llvm::object::MachOObjectFile *Obj) {
    llvm::ArrayRef<uint8_t> dbg_uuid = DbgObj->getUuid();
    llvm::ArrayRef<uint8_t> bin_uuid = Obj->getUuid();
    if (dbg_uuid.empty() || bin_uuid.empty())
        return false;
    return !memcmp(dbg_uuid.data(), bin_uuid.data(), dbg_uuid.size());
}

llvm::object::ObjectFile* LookUpDsymFile(const std::string &ExePath, const llvm::object::MachOObjectFile *MachExeObj, const std::string &ArchName) {
    // On Darwin we may find DWARF in separate object file in
    // resource directory.
    std::vector<std::string> DsymPaths;
    llvm::StringRef Filename = llvm::sys::path::filename(ExePath);
    DsymPaths.push_back(
        GetDarwinDWARFResourceForPath(ExePath, std::string(Filename)));
    for (const auto &Path : DsymPaths) {
        auto DbgObjOrErr = GetOrCreateObject(Path, ArchName);
        if (!DbgObjOrErr) {
        // Ignore errors, the file might not exist.
        consumeError(DbgObjOrErr.takeError());
        continue;
        }
        llvm::object::ObjectFile *DbgObj = DbgObjOrErr.get();
        if (!DbgObj)
        continue;
        const llvm::object::MachOObjectFile *MachDbgObj = llvm::dyn_cast<const llvm::object::MachOObjectFile>(DbgObj);
        if (!MachDbgObj)
        continue;
        if (DarwinDsymMatchesBinary(MachDbgObj, MachExeObj))
        return DbgObj;
    }
    return nullptr;
}

bool GetGNUDebuglinkContents(const llvm::object::ObjectFile *Obj, std::string &DebugName, uint32_t &CRCHash) {
    if (!Obj)
        return false;
    for (const llvm::object::SectionRef &Section : Obj->sections()) {
        llvm::StringRef Name;
        if (llvm::Expected<llvm::StringRef> NameOrErr = Section.getName())
        Name = *NameOrErr;
        else
        consumeError(NameOrErr.takeError());

        Name = Name.substr(Name.find_first_not_of("._"));
        if (Name == "gnu_debuglink") {
        llvm::Expected<llvm::StringRef> ContentsOrErr = Section.getContents();
        if (!ContentsOrErr) {
            consumeError(ContentsOrErr.takeError());
            return false;
        }
        llvm::DataExtractor DE(*ContentsOrErr, Obj->isLittleEndian(), 0);
        uint64_t Offset = 0;
        if (const char *DebugNameStr = DE.getCStr(&Offset)) {
            // 4-byte align the offset.
            Offset = (Offset + 3) & ~0x3;
            if (DE.isValidOffsetForDataOfSize(Offset, 4)) {
            DebugName = DebugNameStr;
            CRCHash = DE.getU32(&Offset);
            return true;
            }
        }
        break;
        }
    }
    return false;
}

llvm::object::ObjectFile* LookUpDebuglinkObject(const std::string &Path, const llvm::object::ObjectFile *Obj, const std::string &ArchName) {
    std::string DebuglinkName;
    uint32_t CRCHash;
    std::string DebugBinaryPath;
    if (!GetGNUDebuglinkContents(Obj, DebuglinkName, CRCHash))
        return nullptr;
    if (!FindDebugBinary(Path, DebuglinkName, CRCHash, "", DebugBinaryPath))
        return nullptr;
    auto DbgObjOrErr = GetOrCreateObject(DebugBinaryPath, ArchName);
    if (!DbgObjOrErr) {
        // Ignore errors, the file might not exist.
        consumeError(DbgObjOrErr.takeError());
        return nullptr;
    }
    return DbgObjOrErr.get();
}

bool FindDebugBinary(const std::vector<std::string> &DebugFileDirectory, const llvm::ArrayRef<uint8_t> BuildID, std::string &Result) {
    auto getDebugPath = [&](llvm::StringRef Directory) {
        llvm::SmallString<128> Path{Directory};
        llvm::sys::path::append(Path, ".build-id",
                        llvm::toHex(BuildID[0], /*LowerCase=*/true),
                        llvm::toHex(BuildID.slice(1), /*LowerCase=*/true));
        Path += ".debug";
        return Path;
    };
    if (DebugFileDirectory.empty()) {
        llvm::SmallString<128> Path = getDebugPath(
    #if defined(__NetBSD__)
        // Try /usr/libdata/debug/.build-id/../...
        "/usr/libdata/debug"
    #else
        // Try /usr/lib/debug/.build-id/../...
        "/usr/lib/debug"
    #endif
        );
        if (llvm::sys::fs::exists(Path)) {
        Result = std::string(Path.str());
        return true;
        }
    } else {
        for (const auto &Directory : DebugFileDirectory) {
        // Try <debug-file-directory>/.build-id/../...
        llvm::SmallString<128> Path = getDebugPath(Directory);
        if (llvm::sys::fs::exists(Path)) {
            Result = std::string(Path.str());
            return true;
        }
        }
    }
    return false;
}

llvm::object::ObjectFile* LookUpBuildIDObject(const std::string &Path, const llvm::object::ELFObjectFileBase *Obj, const std::string &ArchName) {
    Y_UNUSED(Path);
    auto BuildID = GetBuildID(Obj);
    if (!BuildID)
        return nullptr;
    if (BuildID->size() < 2)
        return nullptr;
    std::string DebugBinaryPath;
    if (!FindDebugBinary({}, *BuildID, DebugBinaryPath))
        return nullptr;
    auto DbgObjOrErr = GetOrCreateObject(DebugBinaryPath, ArchName);
    if (!DbgObjOrErr) {
        consumeError(DbgObjOrErr.takeError());
        return nullptr;
    }
    return DbgObjOrErr.get();
}

llvm::Expected<ObjectPair> GetOrCreateObjectPair(const std::string &Path, const std::string &ArchName) {
    auto I = ObjectPairForPathArch.find(std::make_pair(Path, ArchName));
    if (I != ObjectPairForPathArch.end())
        return I->second;

    auto ObjOrErr = GetOrCreateObject(Path, ArchName);
    if (!ObjOrErr) {
        ObjectPairForPathArch.emplace(std::make_pair(Path, ArchName),
                                    ObjectPair(nullptr, nullptr));
        return ObjOrErr.takeError();
    }

    llvm::object::ObjectFile *Obj = ObjOrErr.get();
    assert(Obj != nullptr);
    llvm::object::ObjectFile *DbgObj = nullptr;

    if (auto MachObj = llvm::dyn_cast<const llvm::object::MachOObjectFile>(Obj))
        DbgObj = LookUpDsymFile(Path, MachObj, ArchName);
    else if (auto ELFObj = llvm::dyn_cast<const llvm::object::ELFObjectFileBase>(Obj))
        DbgObj = LookUpBuildIDObject(Path, ELFObj, ArchName);
    if (!DbgObj)
        DbgObj = LookUpDebuglinkObject(Path, Obj, ArchName);
    if (!DbgObj)
        DbgObj = Obj;
    ObjectPair Res = std::make_pair(Obj, DbgObj);
    ObjectPairForPathArch.emplace(std::make_pair(Path, ArchName), Res);
    return Res;
}

llvm::Expected<llvm::symbolize::SymbolizableModule *> CreateModuleInfo(const llvm::object::ObjectFile *Obj, std::unique_ptr<llvm::DIContext> Context, llvm::StringRef ModuleName) {
    auto InfoOrErr = llvm::symbolize::SymbolizableObjectFile::create(Obj, std::move(Context), false);
    std::unique_ptr<llvm::symbolize::SymbolizableModule> SymMod;
    if (InfoOrErr)
        SymMod = std::move(*InfoOrErr);
    auto InsertResult = Modules.insert(
        std::make_pair(std::string(ModuleName), std::move(SymMod)));
    assert(InsertResult.second);
    if (!InfoOrErr)
        return InfoOrErr.takeError();
    return InsertResult.first->second.get();
}

void DoNothing(llvm::Error) {
};

llvm::Expected<llvm::symbolize::SymbolizableModule*> GetOrCreateModuleInfo(const std::string &ModuleName) {
    auto I = Modules.find(ModuleName);
    if (I != Modules.end())
        return I->second.get();

    std::string BinaryName = ModuleName;
    std::string ArchName = ""; // (!) is set to empty
    size_t ColonPos = ModuleName.find_last_of(':');
    // Verify that substring after colon form a valid arch name.
    if (ColonPos != std::string::npos) {
        std::string ArchStr = ModuleName.substr(ColonPos + 1);
        if (llvm::Triple(ArchStr).getArch() != llvm::Triple::UnknownArch) {
            BinaryName = ModuleName.substr(0, ColonPos);
            ArchName = ArchStr;
        }
    }

    auto ObjectsOrErr = GetOrCreateObjectPair(BinaryName, ArchName);
    if (!ObjectsOrErr) {
        // Failed to find valid object file.
        Modules.emplace(ModuleName, std::unique_ptr<llvm::symbolize::SymbolizableModule>());
        return ObjectsOrErr.takeError();
    }

    ObjectPair Objects = ObjectsOrErr.get();

    // Only DWARF available on linux
    std::unique_ptr<llvm::DIContext> Context = llvm::DWARFContext::create(*Objects.second, nullptr, "", DoNothing, DoNothing);
    return CreateModuleInfo(Objects.first, std::move(Context), ModuleName);
}

llvm::StringRef DemanglePE32ExternCFunc(llvm::StringRef SymbolName) {
    // Remove any '_' or '@' prefix.
    char Front = SymbolName.empty() ? '\0' : SymbolName[0];
    if (Front == '_' || Front == '@')
        SymbolName = SymbolName.drop_front();

    // Remove any '@[0-9]+' suffix.
    if (Front != '?') {
        size_t AtPos = SymbolName.rfind('@');
        if (AtPos != llvm::StringRef::npos &&
            all_of(drop_begin(SymbolName, AtPos + 1), llvm::isDigit))
        SymbolName = SymbolName.substr(0, AtPos);
    }

    // Remove any ending '@' for vectorcall.
    if (SymbolName.endswith("@"))
        SymbolName = SymbolName.drop_back();

    return SymbolName;
}

std::string DemangleName(const std::string &Name, const llvm::symbolize::SymbolizableModule *DbiModuleDescriptor) {
    // We can spoil names of symbols with C linkage, so use an heuristic
    // approach to check if the name should be demangled.
    if (Name.substr(0, 2) == "_Z") {
        int status = 0;
        char *DemangledName = llvm::itaniumDemangle(Name.c_str(), nullptr, nullptr, &status);
        if (status != 0)
        return Name;
        std::string Result = DemangledName;
        free(DemangledName);
        return Result;
    }

    if (!Name.empty() && Name.front() == '?') {
        // Only do MSVC C++ demangling on symbols starting with '?'.
        int status = 0;
        char *DemangledName = microsoftDemangle(
            Name.c_str(), nullptr, nullptr, nullptr, &status,
            llvm::MSDemangleFlags(llvm::MSDF_NoAccessSpecifier | llvm::MSDF_NoCallingConvention |
                            llvm::MSDF_NoMemberType | llvm::MSDF_NoReturnType));
        if (status != 0)
        return Name;
        std::string Result = DemangledName;
        free(DemangledName);
        return Result;
    }

    if (DbiModuleDescriptor && DbiModuleDescriptor->isWin32Module())
        return std::string(DemanglePE32ExternCFunc(Name));
    return Name;
}

llvm::Expected<llvm::DILineInfo> SymbolizeCodeCommon(llvm::symbolize::SymbolizableModule *Info, llvm::object::SectionedAddress ModuleOffset) {
  // A null module means an error has already been reported. Return an empty
  // result.
  if (!Info)
    return llvm::DILineInfo();

  llvm::DILineInfo LineInfo = Info->symbolizeCode(
      ModuleOffset, llvm::DILineInfoSpecifier(llvm::DILineInfoSpecifier::FileLineInfoKind::AbsoluteFilePath, llvm::DILineInfoSpecifier::FunctionNameKind::LinkageName),
      true);
    LineInfo.FunctionName = DemangleName(LineInfo.FunctionName, Info);
  return LineInfo;
}

class TRawOStreamProxy: public llvm::raw_ostream {
public:
    TRawOStreamProxy(std::ostream& out)
        : llvm::raw_ostream(true) // unbuffered
        , Slave_(out)
    {
    }
    void write_impl(const char* ptr, size_t size) override {
        Slave_.write(ptr, size);
    }
    uint64_t current_pos() const override {
        return 0;
    }
    size_t preferred_buffer_size() const override {
        return 0;
    }
private:
    std::ostream& Slave_;
};

}

TString SymbolizeAndDumpToString(const std::string &moduleName, llvm::object::SectionedAddress moduleOffset, ui64 offset) {
    llvm::Expected<llvm::symbolize::SymbolizableModule *> InfoOrErr = GetOrCreateModuleInfo(moduleName);
    if (!InfoOrErr)
        return TStringBuilder() << "Can't create module info for '" << moduleName << "'";
    auto resOrErr = SymbolizeCodeCommon(*InfoOrErr, moduleOffset);
    if (resOrErr) {
        auto value = resOrErr.get();
        if (value.FileName == "<invalid>" && offset > 0) {
            value.FileName = moduleName;
        }

        std::stringstream ss;
        TRawOStreamProxy stream_proxy(ss);
        llvm::symbolize::DIPrinter printer(stream_proxy, true, true, false);
        printer << value;
        ss.flush();

        return ss.str();
    } else {
        return TStringBuilder() << "LLVMSymbolizer: error reading file of module '" << moduleName << "'";
    }
}
}
}
