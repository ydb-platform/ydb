#include "discover.h"

#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/providers/common/proto/udf_resolver.pb.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/utils/sys/become_user.h>
#include <ydb/library/yql/utils/sys/linux_version.h>

#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_utils.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/yexception.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/hash_set.h>
#include <util/generic/hash.h>
#include <util/system/env.h>
#include <util/system/fs.h>
#include <util/stream/output.h>
#include <util/string/builder.h>

#ifdef _linux_
#include <sys/types.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <sys/socket.h>
#ifndef GRND_RANDOM
#include <sys/random.h>
#endif

#include <linux/filter.h>
#include <linux/seccomp.h>
#include <linux/audit.h>
#ifndef GRND_RANDOM
#include <linux/random.h>
#endif

#ifndef __SI_MAX_SIZE
#define __SI_MAX_SIZE        128
#endif

#ifndef __SI_PAD_SIZE
#if __WORDSIZE == 64
# define __SI_PAD_SIZE        ((__SI_MAX_SIZE / sizeof (int)) - 4)
#else
# define __SI_PAD_SIZE        ((__SI_MAX_SIZE / sizeof (int)) - 3)
#endif
#endif

#endif

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

void ResolveUDFs() {
    NYql::TResolve inMsg;
    if (!inMsg.ParseFromArcadiaStream(&Cin)) {
        throw yexception() << "Bad input TResolve proto message";
    }

    NYql::TResolveResult outMsg;

    auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
    auto newRegistry = functionRegistry->Clone();
    newRegistry->SetBackTraceCallback(&NYql::NBacktrace::KikimrBackTrace);
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);

    TSet<TString> loadedPaths;
    // Load system imports first
    for (auto& import : inMsg.GetImports()) {
        if (import.GetSystem()) {
            if (!loadedPaths.emplace(import.GetPath()).second) {
                continue;
            }

            newRegistry->LoadUdfs(import.GetPath(), {}, NUdf::IRegistrator::TFlags::TypesOnly);
        }
    }

    THashMap<std::pair<TString, TString>, THashSet<TString>> cachedModules;
    for (auto& import : inMsg.GetImports()) {
        if (!import.GetSystem()) {
            auto importRes = outMsg.AddImports();
            importRes->SetFileAlias(import.GetFileAlias());
            importRes->SetCustomUdfPrefix(import.GetCustomUdfPrefix());
            auto [it, inserted] = cachedModules.emplace(std::make_pair(import.GetPath(), import.GetCustomUdfPrefix()), THashSet<TString>());
            if (inserted) {
                THashSet<TString> modules;
                newRegistry->LoadUdfs(import.GetPath(),
                                    {},
                                    NUdf::IRegistrator::TFlags::TypesOnly,
                                    import.GetCustomUdfPrefix(),
                                    &modules);

                NUdfResolver::FillImportResultModules(modules, *importRes);
                it->second = modules;
            } else {
                NUdfResolver::FillImportResultModules(it->second, *importRes);
            }
        }
    }

    for (size_t i = 0; i < inMsg.UdfsSize(); ++i) {
        auto& udf = inMsg.GetUdfs(i);
        auto udfRes = outMsg.AddUdfs();
        try {
            TProgramBuilder pgmBuilder(env, *newRegistry);
            TType* mkqlUserType = nullptr;
            if (udf.HasUserType()) {
                TStringStream err;
                mkqlUserType = NYql::NCommon::ParseTypeFromYson(TStringBuf{udf.GetUserType()}, pgmBuilder, err);
                if (!mkqlUserType) {
                    udfRes->SetError(TStringBuilder() << "Invalid user type for function: "
                        << udf.GetName() << ", error: " << err.Str());
                    continue;
                }
            }

            TFunctionTypeInfo funcInfo;
            auto status = newRegistry->FindFunctionTypeInfo(env, typeInfoHelper, nullptr,
                udf.GetName(), mkqlUserType, udf.GetTypeConfig(), NUdf::IUdfModule::TFlags::TypesOnly, {}, nullptr, &funcInfo);
            if (!status.IsOk()) {
                udfRes->SetError(TStringBuilder() << "Failed to find UDF function: " << udf.GetName()
                    << ", reason: " << status.GetError());
                continue;
            }

            udfRes->SetCallableType(NYql::NCommon::WriteTypeToYson(funcInfo.FunctionType));

            if (funcInfo.RunConfigType) {
                udfRes->SetRunConfigType(NYql::NCommon::WriteTypeToYson(funcInfo.RunConfigType));
            }

            if (funcInfo.UserType) {
                udfRes->SetNormalizedUserType(NYql::NCommon::WriteTypeToYson(funcInfo.UserType));
            }

            udfRes->SetSupportsBlocks(funcInfo.SupportsBlocks);
            udfRes->SetIsStrict(funcInfo.IsStrict);
        } catch (yexception& e) {
            udfRes->SetError(TStringBuilder()
                << "Internal error was found when udf metadata is loading for function: " << udf.GetName()
                << ", reason: " << e.what());
        }
    }

    outMsg.SerializeToArcadiaStream(&Cout);
}

void ListModules(const TString& dir) {
    TVector<TString> udfPaths;
    NMiniKQL::FindUdfsInDir(dir, &udfPaths);
    auto funcRegistry = CreateFunctionRegistry(&NYql::NBacktrace::KikimrBackTrace, IBuiltinFunctionRegistry::TPtr(), false, udfPaths,
       NUdf::IRegistrator::TFlags::TypesOnly);

    for (auto& m : funcRegistry->GetAllModuleNames()) {
        auto path = *funcRegistry->FindUdfPath(m);
        Cout << m << '\t' << path << Endl;
    }
}

// NOLINTBEGIN(readability-identifier-naming)
#ifdef _linux_
struct my_siginfo_t
  {
    int si_signo;                /* Signal number.  */
#if __SI_ERRNO_THEN_CODE
    int si_errno;                /* If non-zero, an errno value associated with
                                   this signal, as defined in <errno.h>.  */
    int si_code;                /* Signal code.  */
#else
    int si_code;
    int si_errno;
#endif
#if __WORDSIZE == 64
    int __pad0;                        /* Explicit padding.  */
#endif
    union
      {
        int _pad[__SI_PAD_SIZE];
        struct
          {
            void *_call_addr;        /* Calling user insn.  */
            int _syscall;        /* Triggering system call number.  */
            unsigned int _arch; /* AUDIT_ARCH_* of syscall.  */
          } _sigsys;

      } _sifields;
  };
// NOLINTEND(readability-identifier-naming)

void SigSysHandler(int sig, my_siginfo_t *info, void *) {
    Cerr << "SigSysHandler: " << sig << ", code: " << info->si_code << ", errno: " <<
        info->si_errno << ", call: " << info->_sifields._sigsys._syscall << ", arch:" << info->_sifields._sigsys._arch << "\n";
    // repeat SIGSYS signal (this will kill current process)
    raise(sig);
}
#endif

int main(int argc, char **argv) {
    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    try {
#ifdef _linux_
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sa.sa_flags = SA_RESETHAND | SA_SIGINFO;
        typedef void (*TSigSysHandler)(int, siginfo_t *, void *);
        sa.sa_sigaction = (TSigSysHandler)SigSysHandler;
        sigfillset(&sa.sa_mask);
        if (sigaction(SIGSYS, &sa, nullptr) == -1) {
           ythrow TSystemError() << "Cannot set handler for signal " << strsignal(SIGSYS);
        }
#endif

        TString path;
        TString user;
        TString group;
        bool printAsProto = true;

        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.AddLongOption('L', "list", "List UDF modules in specified directory")
            .Optional()
            .RequiredArgument("DIR")
            .StoreResult(&path);

        opts.AddLongOption('D', "discover-path", "Discover UDFs in folder or single file")
            .Optional()
            .RequiredArgument("DIR")
            .StoreResult(&path);

        opts.AddLongOption('P', "discover-proto", "Discover UDFs according to TResolve proto from cin")
            .Optional()
            .NoArgument();

        opts.AddLongOption('A', "as-proto", "Print result in protobuf format")
            .Optional()
            .DefaultValue(true)
            .StoreResult(&printAsProto);

        opts.AddLongOption('U', "user", "Run as user")
            .Optional()
            .StoreResult(&user);

        opts.AddLongOption('G', "group", "Run as group")
            .Optional()
            .StoreResult(&group);

        opts.AddLongOption('F', "filter-syscalls", "Filter syscalls")
            .Optional()
            .NoArgument();

        opts.SetFreeArgsNum(0);

        NLastGetopt::TOptsParseResult res(&opts, argc, argv);

        SetEnv("USER", user ? user : "udf_resolver");
        if (user && !NYql::IsLinuxKernelBelow4_3()) {
            // since we are going to load untrusted modules
            // we have to switch into another user and drop privileges (capabilities)
            NYql::BecomeUser(user, group);
            NYql::TurnOffBecomeUserAbility();
        }

        NYql::SendSignalOnParentThreadExit(SIGTERM);

#ifdef _linux_
        if (rlimit limit = {0, 0}; setrlimit(RLIMIT_CORE, &limit) != 0) {
            ythrow TSystemError() << "Failed to set RLIMIT_CORE";
        }
#endif

        if (res.Has("filter-syscalls")) {
#ifdef _linux_

#define ArchField offsetof(struct seccomp_data, arch) // NOLINT(readability-identifier-naming)

// NOLINTNEXTLINE(readability-identifier-naming)
#define Allow(syscall) \
    BPF_JUMP(BPF_JMP+BPF_JEQ+BPF_K, SYS_##syscall, 0, 1), \
    BPF_STMT(BPF_RET+BPF_K, SECCOMP_RET_ALLOW)

            struct sock_filter filter[] = {
                /* validate arch */
                BPF_STMT(BPF_LD+BPF_W+BPF_ABS, ArchField),
                BPF_JUMP( BPF_JMP+BPF_JEQ+BPF_K, AUDIT_ARCH_X86_64, 1, 0),
                BPF_STMT(BPF_RET+BPF_K, SECCOMP_RET_TRAP),

                /* load syscall */
                BPF_STMT(BPF_LD+BPF_W+BPF_ABS, offsetof(struct seccomp_data, nr)),

                /* list of allowed syscalls */
#ifndef _arm64_
                Allow(access),
#endif
                Allow(brk),
                Allow(chdir),
                Allow(clock_gettime),
                Allow(clock_nanosleep),
                Allow(clone),
                Allow(close),
#ifndef _arm64_
                Allow(creat),
#endif
                Allow(dup),
#ifndef _arm64_
                Allow(dup2),
#endif
                Allow(dup3),
                Allow(eventfd2),
                Allow(exit),
                Allow(exit_group),
                Allow(fadvise64),
                Allow(fallocate),
                Allow(flock),
                Allow(fstat),
                Allow(fsync),
                Allow(ftruncate),
                Allow(futex),
                Allow(get_robust_list),
                Allow(getcwd),
#ifndef _arm64_
                Allow(getdents),
#endif
                Allow(getdents64),
                Allow(getegid),
                Allow(geteuid),
                Allow(getgid),
                Allow(getgroups),
                Allow(getpgid),
#ifndef _arm64_
                Allow(getpgrp),
#endif
                Allow(getpid),
                Allow(getppid),
                Allow(getpriority),
                Allow(getrandom),
                Allow(getrlimit),
                Allow(getrusage),
                Allow(getsid),
                Allow(gettid),
                Allow(gettimeofday),
                Allow(getuid),
                Allow(getxattr),
                Allow(ioctl),
                Allow(lgetxattr),
#ifndef _arm64_
                Allow(link),
#endif
                Allow(listxattr),
                Allow(llistxattr),
                Allow(lremovexattr),
                Allow(lseek),
                Allow(lsetxattr),
#ifndef _arm64_
                Allow(lstat),
#endif
                Allow(madvise),
#ifndef _arm64_
                Allow(mkdir),
#endif
                Allow(mkdirat),
                Allow(mlock),
                Allow(mlockall),
                Allow(mmap),
                Allow(mprotect),
                Allow(munlock),
                Allow(munlockall),
                Allow(munmap),
                Allow(nanosleep),
#ifndef _arm64_
                Allow(open),
#endif
                Allow(openat),
                Allow(pipe2),
                Allow(prctl),
                Allow(pread64),
                Allow(pwrite64),
                Allow(read),
#ifndef _arm64_
                Allow(readlink),
#endif
                Allow(readv),
                Allow(removexattr),
#ifndef _arm64_
                Allow(rename),
#endif
                Allow(renameat),
#ifndef _arm64_
                Allow(rmdir),
#endif
                Allow(rt_sigaction),
                Allow(rt_sigpending),
                Allow(rt_sigprocmask),
                Allow(rt_sigqueueinfo),
                Allow(rt_sigreturn),
                Allow(rt_sigsuspend),
                Allow(rt_sigtimedwait),
                Allow(rt_tgsigqueueinfo),
                Allow(sched_getaffinity),
                Allow(sched_setaffinity),
                Allow(set_robust_list),
                Allow(setxattr),
#ifndef _arm64_
                Allow(stat),
#endif
                Allow(sysinfo),
                Allow(uname),
#ifndef _arm64_
                Allow(unlink),
#endif
                Allow(unlinkat),
                Allow(write),
                Allow(writev),

                /* and if we don't match above, die */
                BPF_STMT(BPF_RET+BPF_K, SECCOMP_RET_TRAP),
            };
            struct sock_fprog filterprog = {
                .len = sizeof(filter)/sizeof(filter[0]),
                .filter = filter
            };

            if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) == -1) {
                ythrow yexception() << "prctl(PR_SET_NO_NEW_PRIVS, 1, ...) failed with: " << LastSystemErrorText();
            }
            if (prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &filterprog) == -1) {
                ythrow yexception() << "prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, ...) failed with: " << LastSystemErrorText();
            }
#endif
        }

        Y_UNUSED(NUdf::GetStaticSymbols());
        if (res.Has("list")) {
            ListModules(path);
            return 0;
        }

        if (res.Has("discover-path")) {
            NFs::EnsureExists(path);
            TFileStat fstat(path);
            if (fstat.IsDir()) {
                NUdfResolver::DiscoverInDir(path, Cout, printAsProto);
            } else {
                NUdfResolver::DiscoverInFile(path, Cout, printAsProto);
            }
            return 0;
        }

        if (res.Has("discover-proto")) {
            NUdfResolver::Discover(Cin, Cout, printAsProto);
            return 0;
        }

        ResolveUDFs();
        return 0;
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
