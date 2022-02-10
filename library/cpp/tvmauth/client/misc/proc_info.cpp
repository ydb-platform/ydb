#include "proc_info.h" 
 
#include <library/cpp/tvmauth/version.h> 
 
#include <library/cpp/string_utils/quote/quote.h> 
 
#include <util/stream/file.h> 
#include <util/string/cast.h> 
#include <util/system/getpid.h> 
 
namespace NTvmAuth::NUtils { 
    void TProcInfo::AddToRequest(IOutputStream& out) const { 
        out << "_pid=" << Pid; 
        if (ProcessName) { 
            out << "&_procces_name=" << *ProcessName; 
        } 
        out << "&lib_version=client_" << VersionPrefix << LibVersion(); 
    } 
 
    TProcInfo TProcInfo::Create(const TString& versionPrefix) { 
        TProcInfo res; 
        res.Pid = IntToString<10>(GetPID()); 
        res.ProcessName = GetProcessName(); 
        res.VersionPrefix = versionPrefix; 
        return res; 
    } 
 
    std::optional<TString> TProcInfo::GetProcessName() { 
        try { 
            // works only for linux 
            TFileInput proc("/proc/self/status"); 
 
            TString line; 
            while (proc.ReadLine(line)) { 
                TStringBuf buf(line); 
                if (!buf.SkipPrefix("Name:")) { 
                    continue; 
                } 
 
                while (buf && isspace(buf.front())) { 
                    buf.Skip(1); 
                } 
 
                TString res(buf); 
                CGIEscape(res); 
                return res; 
            } 
        } catch (...) { 
        } 
 
        return {}; 
    } 
} 
