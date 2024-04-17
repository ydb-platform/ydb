/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @file CpuInfoUtil.cc is from Apache Arrow as of 2023-03-21
 */

#include "CpuInfoUtil.hh"

#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

#ifndef _MSC_VER
#include <unistd.h>
#endif

#ifdef _WIN32
#define NOMINMAX
#include <Windows.h>
#include <intrin.h>
#endif

#include <algorithm>
#include <array>
#include <bitset>
#include <cstdint>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "orc/Exceptions.hh"

#undef CPUINFO_ARCH_X86
#undef CPUINFO_ARCH_ARM
#undef CPUINFO_ARCH_PPC

#if defined(__i386) || defined(_M_IX86) || defined(__x86_64__) || defined(_M_X64)
#define CPUINFO_ARCH_X86
#ifndef ORC_HAVE_RUNTIME_AVX512
#define UNUSED(x) (void)(x)
#endif
#elif defined(_M_ARM64) || defined(__aarch64__) || defined(__arm64__)
#define CPUINFO_ARCH_ARM
#elif defined(__PPC64__) || defined(__PPC64LE__) || defined(__ppc64__) || defined(__powerpc64__)
#define CPUINFO_ARCH_PPC
#endif

namespace orc {

  namespace {

    constexpr int kCacheLevels = static_cast<int>(CpuInfo::CacheLevel::Last) + 1;

    //============================== OS Dependent ==============================//

#if defined(_WIN32)
    //------------------------------ WINDOWS ------------------------------//
    void OsRetrieveCacheSize(std::array<int64_t, kCacheLevels>* cache_sizes) {
      PSYSTEM_LOGICAL_PROCESSOR_INFORMATION buffer = nullptr;
      PSYSTEM_LOGICAL_PROCESSOR_INFORMATION buffer_position = nullptr;
      DWORD buffer_size = 0;
      size_t offset = 0;
      typedef BOOL(WINAPI * GetLogicalProcessorInformationFuncPointer)(void*, void*);
      GetLogicalProcessorInformationFuncPointer func_pointer =
          (GetLogicalProcessorInformationFuncPointer)GetProcAddress(
              GetModuleHandle("kernel32"), "GetLogicalProcessorInformation");

      if (!func_pointer) {
        throw ParseError("Failed to find procedure GetLogicalProcessorInformation");
      }

      // Get buffer size
      if (func_pointer(buffer, &buffer_size) && GetLastError() != ERROR_INSUFFICIENT_BUFFER) {
        throw ParseError("Failed to get size of processor information buffer");
      }

      buffer = (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION)malloc(buffer_size);
      if (!buffer) {
        return;
      }

      if (!func_pointer(buffer, &buffer_size)) {
        free(buffer);
        throw ParseError("Failed to get processor information");
      }

      buffer_position = buffer;
      while (offset + sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION) <= buffer_size) {
        if (RelationCache == buffer_position->Relationship) {
          PCACHE_DESCRIPTOR cache = &buffer_position->Cache;
          if (cache->Level >= 1 && cache->Level <= kCacheLevels) {
            const int64_t current = (*cache_sizes)[cache->Level - 1];
            (*cache_sizes)[cache->Level - 1] = std::max<int64_t>(current, cache->Size);
          }
        }
        offset += sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION);
        buffer_position++;
      }

      free(buffer);
    }

#if defined(CPUINFO_ARCH_X86)
    // On x86, get CPU features by cpuid, https://en.wikipedia.org/wiki/CPUID

#if defined(__MINGW64_VERSION_MAJOR) && __MINGW64_VERSION_MAJOR < 5
    void __cpuidex(int CPUInfo[4], int function_id, int subfunction_id) {
      __asm__ __volatile__("cpuid"
                           : "=a"(CPUInfo[0]), "=b"(CPUInfo[1]), "=c"(CPUInfo[2]), "=d"(CPUInfo[3])
                           : "a"(function_id), "c"(subfunction_id));
    }

    int64_t _xgetbv(int xcr) {
      int out = 0;
      __asm__ __volatile__("xgetbv" : "=a"(out) : "c"(xcr) : "%edx");
      return out;
    }
#endif  // MINGW

    void OsRetrieveCpuInfo(int64_t* hardware_flags, CpuInfo::Vendor* vendor,
                           std::string* model_name) {
      int register_EAX_id = 1;
      int highest_valid_id = 0;
      int highest_extended_valid_id = 0;
      std::bitset<32> features_ECX;
      std::array<int, 4> cpu_info;

      // Get highest valid id
      __cpuid(cpu_info.data(), 0);
      highest_valid_id = cpu_info[0];
      // HEX of "GenuineIntel": 47656E75 696E6549 6E74656C
      // HEX of "AuthenticAMD": 41757468 656E7469 63414D44
      if (cpu_info[1] == 0x756e6547 && cpu_info[3] == 0x49656e69 && cpu_info[2] == 0x6c65746e) {
        *vendor = CpuInfo::Vendor::Intel;
      } else if (cpu_info[1] == 0x68747541 && cpu_info[3] == 0x69746e65 &&
                 cpu_info[2] == 0x444d4163) {
        *vendor = CpuInfo::Vendor::AMD;
      }

      if (highest_valid_id <= register_EAX_id) {
        return;
      }

      // EAX=1: Processor Info and Feature Bits
      __cpuidex(cpu_info.data(), register_EAX_id, 0);
      features_ECX = cpu_info[2];

      // Get highest extended id
      __cpuid(cpu_info.data(), 0x80000000);
      highest_extended_valid_id = cpu_info[0];

      // Retrieve CPU model name
      if (highest_extended_valid_id >= static_cast<int>(0x80000004)) {
        model_name->clear();
        for (int i = 0x80000002; i <= static_cast<int>(0x80000004); ++i) {
          __cpuidex(cpu_info.data(), i, 0);
          *model_name += std::string(reinterpret_cast<char*>(cpu_info.data()), sizeof(cpu_info));
        }
      }

      bool zmm_enabled = false;
      if (features_ECX[27]) {  // OSXSAVE
        // Query if the OS supports saving ZMM registers when switching contexts
        int64_t xcr0 = _xgetbv(0);
        zmm_enabled = (xcr0 & 0xE0) == 0xE0;
      }

      if (features_ECX[9]) *hardware_flags |= CpuInfo::SSSE3;
      if (features_ECX[19]) *hardware_flags |= CpuInfo::SSE4_1;
      if (features_ECX[20]) *hardware_flags |= CpuInfo::SSE4_2;
      if (features_ECX[23]) *hardware_flags |= CpuInfo::POPCNT;
      if (features_ECX[28]) *hardware_flags |= CpuInfo::AVX;

      // cpuid with EAX=7, ECX=0: Extended Features
      register_EAX_id = 7;
      if (highest_valid_id > register_EAX_id) {
        __cpuidex(cpu_info.data(), register_EAX_id, 0);
        std::bitset<32> features_EBX = cpu_info[1];

        if (features_EBX[3]) *hardware_flags |= CpuInfo::BMI1;
        if (features_EBX[5]) *hardware_flags |= CpuInfo::AVX2;
        if (features_EBX[8]) *hardware_flags |= CpuInfo::BMI2;
        if (zmm_enabled) {
          if (features_EBX[16]) *hardware_flags |= CpuInfo::AVX512F;
          if (features_EBX[17]) *hardware_flags |= CpuInfo::AVX512DQ;
          if (features_EBX[28]) *hardware_flags |= CpuInfo::AVX512CD;
          if (features_EBX[30]) *hardware_flags |= CpuInfo::AVX512BW;
          if (features_EBX[31]) *hardware_flags |= CpuInfo::AVX512VL;
        }
      }
    }

#elif defined(CPUINFO_ARCH_ARM)
    // Windows on Arm
    void OsRetrieveCpuInfo(int64_t* hardware_flags, CpuInfo::Vendor* vendor,
                           std::string* model_name) {
      *hardware_flags |= CpuInfo::ASIMD;
      // TODO: vendor, model_name
    }
#endif

#elif defined(__APPLE__)
    //------------------------------ MACOS ------------------------------//
    std::optional<int64_t> IntegerSysCtlByName(const char* name) {
      size_t len = sizeof(int64_t);
      int64_t data = 0;
      if (sysctlbyname(name, &data, &len, nullptr, 0) == 0) {
        return data;
      }
      // ENOENT is the official errno value for non-existing sysctl's,
      // but EINVAL and ENOTSUP have been seen in the wild.
      if (errno != ENOENT && errno != EINVAL && errno != ENOTSUP) {
        std::ostringstream ss;
        ss << "sysctlbyname failed for '" << name << "'";
        throw ParseError(ss.str());
      }
      return std::nullopt;
    }

    void OsRetrieveCacheSize(std::array<int64_t, kCacheLevels>* cache_sizes) {
      static_assert(kCacheLevels >= 3, "");
      auto c = IntegerSysCtlByName("hw.l1dcachesize");
      if (c.has_value()) {
        (*cache_sizes)[0] = *c;
      }
      c = IntegerSysCtlByName("hw.l2cachesize");
      if (c.has_value()) {
        (*cache_sizes)[1] = *c;
      }
      c = IntegerSysCtlByName("hw.l3cachesize");
      if (c.has_value()) {
        (*cache_sizes)[2] = *c;
      }
    }

    void OsRetrieveCpuInfo(int64_t* hardware_flags, CpuInfo::Vendor* vendor,
                           std::string* model_name) {
      // hardware_flags
      struct SysCtlCpuFeature {
        const char* name;
        int64_t flag;
      };
      std::vector<SysCtlCpuFeature> features = {
#if defined(CPUINFO_ARCH_X86)
        {"hw.optional.sse4_2",
         CpuInfo::SSSE3 | CpuInfo::SSE4_1 | CpuInfo::SSE4_2 | CpuInfo::POPCNT},
        {"hw.optional.avx1_0", CpuInfo::AVX},
        {"hw.optional.avx2_0", CpuInfo::AVX2},
        {"hw.optional.bmi1", CpuInfo::BMI1},
        {"hw.optional.bmi2", CpuInfo::BMI2},
        {"hw.optional.avx512f", CpuInfo::AVX512F},
        {"hw.optional.avx512cd", CpuInfo::AVX512CD},
        {"hw.optional.avx512dq", CpuInfo::AVX512DQ},
        {"hw.optional.avx512bw", CpuInfo::AVX512BW},
        {"hw.optional.avx512vl", CpuInfo::AVX512VL},
#elif defined(CPUINFO_ARCH_ARM)
        // ARM64 (note that this is exposed under Rosetta as well)
        {"hw.optional.neon", CpuInfo::ASIMD},
#endif
      };
      for (const auto& feature : features) {
        auto v = IntegerSysCtlByName(feature.name);
        if (v.value_or(0)) {
          *hardware_flags |= feature.flag;
        }
      }

      // TODO: vendor, model_name
      *vendor = CpuInfo::Vendor::Unknown;
      *model_name = "Unknown";
    }

#else
    //------------------------------ LINUX ------------------------------//
    // Get cache size, return 0 on error
    int64_t LinuxGetCacheSize(int level) {
      // get cache size by sysconf()
#ifdef _SC_LEVEL1_DCACHE_SIZE
      const int kCacheSizeConf[] = {
          _SC_LEVEL1_DCACHE_SIZE,
          _SC_LEVEL2_CACHE_SIZE,
          _SC_LEVEL3_CACHE_SIZE,
      };
      static_assert(sizeof(kCacheSizeConf) / sizeof(kCacheSizeConf[0]) == kCacheLevels, "");

      errno = 0;
      const int64_t cache_size = sysconf(kCacheSizeConf[level]);
      if (errno == 0 && cache_size > 0) {
        return cache_size;
      }
#endif

      // get cache size from sysfs if sysconf() fails or not supported
      const char* kCacheSizeSysfs[] = {
          "/sys/devices/system/cpu/cpu0/cache/index0/size",  // l1d (index1 is l1i)
          "/sys/devices/system/cpu/cpu0/cache/index2/size",  // l2
          "/sys/devices/system/cpu/cpu0/cache/index3/size",  // l3
      };
      static_assert(sizeof(kCacheSizeSysfs) / sizeof(kCacheSizeSysfs[0]) == kCacheLevels, "");

      std::ifstream cacheinfo(kCacheSizeSysfs[level], std::ios::in);
      if (!cacheinfo) {
        return 0;
      }
      // cacheinfo is one line like: 65536, 64K, 1M, etc.
      uint64_t size = 0;
      char unit = '\0';
      cacheinfo >> size >> unit;
      if (unit == 'K') {
        size <<= 10;
      } else if (unit == 'M') {
        size <<= 20;
      } else if (unit == 'G') {
        size <<= 30;
      } else if (unit != '\0') {
        return 0;
      }
      return static_cast<int64_t>(size);
    }

    // Helper function to parse for hardware flags from /proc/cpuinfo
    // values contains a list of space-separated flags.  check to see if the flags we
    // care about are present.
    // Returns a bitmap of flags.
    int64_t LinuxParseCpuFlags(const std::string& values) {
      const struct {
        std::string name;
        int64_t flag;
      } flag_mappings[] = {
#if defined(CPUINFO_ARCH_X86)
        {"ssse3", CpuInfo::SSSE3},
        {"sse4_1", CpuInfo::SSE4_1},
        {"sse4_2", CpuInfo::SSE4_2},
        {"popcnt", CpuInfo::POPCNT},
        {"avx", CpuInfo::AVX},
        {"avx2", CpuInfo::AVX2},
        {"avx512f", CpuInfo::AVX512F},
        {"avx512cd", CpuInfo::AVX512CD},
        {"avx512vl", CpuInfo::AVX512VL},
        {"avx512dq", CpuInfo::AVX512DQ},
        {"avx512bw", CpuInfo::AVX512BW},
        {"bmi1", CpuInfo::BMI1},
        {"bmi2", CpuInfo::BMI2},
#elif defined(CPUINFO_ARCH_ARM)
        {"asimd", CpuInfo::ASIMD},
#endif
      };
      const int64_t num_flags = sizeof(flag_mappings) / sizeof(flag_mappings[0]);

      int64_t flags = 0;
      for (int i = 0; i < num_flags; ++i) {
        if (values.find(flag_mappings[i].name) != std::string::npos) {
          flags |= flag_mappings[i].flag;
        }
      }
      return flags;
    }

    void OsRetrieveCacheSize(std::array<int64_t, kCacheLevels>* cache_sizes) {
      for (int i = 0; i < kCacheLevels; ++i) {
        const int64_t cache_size = LinuxGetCacheSize(i);
        if (cache_size > 0) {
          (*cache_sizes)[i] = cache_size;
        }
      }
    }

    static constexpr bool IsWhitespace(char c) {
      return c == ' ' || c == '\t';
    }

    std::string TrimString(std::string value) {
      size_t ltrim_chars = 0;
      while (ltrim_chars < value.size() && IsWhitespace(value[ltrim_chars])) {
        ++ltrim_chars;
      }
      value.erase(0, ltrim_chars);
      size_t rtrim_chars = 0;
      while (rtrim_chars < value.size() && IsWhitespace(value[value.size() - 1 - rtrim_chars])) {
        ++rtrim_chars;
      }
      value.erase(value.size() - rtrim_chars, rtrim_chars);
      return value;
    }

    // Read from /proc/cpuinfo
    void OsRetrieveCpuInfo(int64_t* hardware_flags, CpuInfo::Vendor* vendor,
                           std::string* model_name) {
      std::ifstream cpuinfo("/proc/cpuinfo", std::ios::in);
      while (cpuinfo) {
        std::string line;
        std::getline(cpuinfo, line);
        const size_t colon = line.find(':');
        if (colon != std::string::npos) {
          const std::string name = TrimString(line.substr(0, colon - 1));
          const std::string value = TrimString(line.substr(colon + 1, std::string::npos));
          if (name.compare("flags") == 0 || name.compare("Features") == 0) {
            *hardware_flags |= LinuxParseCpuFlags(value);
          } else if (name.compare("model name") == 0) {
            *model_name = value;
          } else if (name.compare("vendor_id") == 0) {
            if (value.compare("GenuineIntel") == 0) {
              *vendor = CpuInfo::Vendor::Intel;
            } else if (value.compare("AuthenticAMD") == 0) {
              *vendor = CpuInfo::Vendor::AMD;
            }
          }
        }
      }
    }
#endif  // WINDOWS, MACOS, LINUX

    //============================== Arch Dependent ==============================//

#if defined(CPUINFO_ARCH_X86)
    //------------------------------ X86_64 ------------------------------//
    bool ArchParseUserSimdLevel(const std::string& simd_level, int64_t* hardware_flags) {
      enum {
        USER_SIMD_NONE,
        USER_SIMD_AVX512,
        USER_SIMD_MAX,
      };

      int level = USER_SIMD_MAX;
      // Parse the level
      if (simd_level == "AVX512") {
        level = USER_SIMD_AVX512;
      } else if (simd_level == "NONE") {
        level = USER_SIMD_NONE;
      } else {
        return false;
      }

      // Disable feature as the level
      if (level < USER_SIMD_AVX512) {
        *hardware_flags &= ~CpuInfo::AVX512;
      }
      return true;
    }

    void ArchVerifyCpuRequirements(const CpuInfo* ci) {
#if defined(ORC_HAVE_RUNTIME_AVX512)
      if (!ci->isDetected(CpuInfo::AVX512)) {
        throw ParseError("CPU does not support the Supplemental AVX512 instruction set");
      }
#else
      UNUSED(ci);
#endif
    }

#elif defined(CPUINFO_ARCH_ARM)
    //------------------------------ AARCH64 ------------------------------//
    bool ArchParseUserSimdLevel(const std::string& simd_level, int64_t* hardware_flags) {
      if (simd_level == "NONE") {
        *hardware_flags &= ~CpuInfo::ASIMD;
        return true;
      }
      return false;
    }

    void ArchVerifyCpuRequirements(const CpuInfo* ci) {
      if (!ci->isDetected(CpuInfo::ASIMD)) {
        throw ParseError("CPU does not support the Armv8 Neon instruction set");
      }
    }

#else
    //------------------------------ PPC, ... ------------------------------//
    bool ArchParseUserSimdLevel(const std::string& simd_level, int64_t* hardware_flags) {
      return true;
    }

    void ArchVerifyCpuRequirements(const CpuInfo* ci) {}

#endif  // X86, ARM, PPC

  }  // namespace

  struct CpuInfo::Impl {
    int64_t hardware_flags = 0;
    int numCores = 0;
    int64_t original_hardware_flags = 0;
    Vendor vendor = Vendor::Unknown;
    std::string model_name = "Unknown";
    std::array<int64_t, kCacheLevels> cache_sizes{};

    Impl() {
      OsRetrieveCacheSize(&cache_sizes);
      OsRetrieveCpuInfo(&hardware_flags, &vendor, &model_name);
      original_hardware_flags = hardware_flags;
      numCores = std::max(static_cast<int>(std::thread::hardware_concurrency()), 1);

      // parse user simd level
      const auto maybe_env_var = std::getenv("ORC_USER_SIMD_LEVEL");
      std::string userSimdLevel = maybe_env_var == nullptr ? "NONE" : std::string(maybe_env_var);
      std::transform(userSimdLevel.begin(), userSimdLevel.end(), userSimdLevel.begin(),
                     [](unsigned char c) { return std::toupper(c); });
      if (!ArchParseUserSimdLevel(userSimdLevel, &hardware_flags)) {
        throw ParseError("Invalid value for ORC_USER_SIMD_LEVEL: " + userSimdLevel);
      }
    }
  };

  CpuInfo::~CpuInfo() = default;

  CpuInfo::CpuInfo() : impl_(new Impl) {}

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wexit-time-destructors"
#endif

  const CpuInfo* CpuInfo::getInstance() {
    static CpuInfo cpu_info;
    return &cpu_info;
  }

#ifdef __clang__
#pragma clang diagnostic pop
#endif

  int64_t CpuInfo::hardwareFlags() const {
    return impl_->hardware_flags;
  }

  int CpuInfo::numCores() const {
    return impl_->numCores <= 0 ? 1 : impl_->numCores;
  }

  CpuInfo::Vendor CpuInfo::vendor() const {
    return impl_->vendor;
  }

  const std::string& CpuInfo::modelName() const {
    return impl_->model_name;
  }

  int64_t CpuInfo::cacheSize(CacheLevel level) const {
    constexpr int64_t kDefaultCacheSizes[] = {
        32 * 1024,    // Level 1: 32K
        256 * 1024,   // Level 2: 256K
        3072 * 1024,  // Level 3: 3M
    };
    static_assert(sizeof(kDefaultCacheSizes) / sizeof(kDefaultCacheSizes[0]) == kCacheLevels, "");

    static_assert(static_cast<int>(CacheLevel::L1) == 0, "");
    const int i = static_cast<int>(level);
    if (impl_->cache_sizes[i] > 0) return impl_->cache_sizes[i];
    if (i == 0) return kDefaultCacheSizes[0];
    // l3 may be not available, return maximum of l2 or default size
    return std::max(kDefaultCacheSizes[i], impl_->cache_sizes[i - 1]);
  }

  bool CpuInfo::isSupported(int64_t flags) const {
    return (impl_->hardware_flags & flags) == flags;
  }

  bool CpuInfo::isDetected(int64_t flags) const {
    return (impl_->original_hardware_flags & flags) == flags;
  }

  void CpuInfo::verifyCpuRequirements() const {
    return ArchVerifyCpuRequirements(this);
  }

}  // namespace orc

#undef CPUINFO_ARCH_X86
#undef CPUINFO_ARCH_ARM
#undef CPUINFO_ARCH_PPC
