use std::env;
#[cfg(target_os = "linux")]
use std::fs;
#[cfg(unix)]
use std::path::Path;

#[cfg(target_os = "macos")]
use temporalio_common::protos::temporal::api::worker::v1::environment_info::MacOsPlatform;
#[cfg(target_os = "linux")]
use temporalio_common::protos::temporal::api::worker::v1::environment_info::{
    LinuxPlatform, linux_platform::Libc,
};
#[cfg(target_os = "windows")]
use temporalio_common::protos::temporal::api::worker::v1::environment_info::{
    WindowsPlatform, windows_platform::Crt,
};
use temporalio_common::protos::temporal::api::worker::v1::{
    EnvironmentInfo,
    environment_info::{
        Architecture, HostingEnvironment, Platform, Runtime,
        hosting_environment::HostingEnvironmentType, platform::Variant, runtime::RuntimeType,
    },
};

pub(crate) fn native_runtime() -> Runtime {
    Runtime {
        r#type: RuntimeType::Native as i32,
        version: String::new(),
    }
}

pub(crate) fn detect(runtimes: Vec<Runtime>) -> EnvironmentInfo {
    EnvironmentInfo {
        runtimes: runtimes
            .into_iter()
            .filter(|runtime| runtime.r#type != RuntimeType::Unspecified as i32)
            .collect(),
        hosting_environments: detect_hosting_environments(),
        platform: detect_platform(),
    }
}

fn detect_hosting_environments() -> Vec<HostingEnvironment> {
    let mut environments = Vec::new();

    // Note it is intentional that some environments may be detected simultaneously.
    // EX: AzureFunctions inside AzureAppService or Docker inside Kubernetes.

    if is_docker() {
        environments.push(hosting_environment(HostingEnvironmentType::Docker, None));
    }
    if has_env("KUBERNETES_SERVICE_HOST") {
        environments.push(hosting_environment(HostingEnvironmentType::K8s, None));
    }
    if has_env("AWS_LAMBDA_FUNCTION_NAME") {
        environments.push(hosting_environment(HostingEnvironmentType::AwsLambda, None));
    }
    if has_any_env(&[
        "ECS_CONTAINER_METADATA_URI_V4",
        "ECS_CONTAINER_METADATA_URI",
    ]) {
        environments.push(hosting_environment(HostingEnvironmentType::AwsEcs, None));
    }
    if has_any_env(&["K_SERVICE", "CLOUD_RUN_JOB", "CLOUD_RUN_WORKER_POOL"]) {
        environments.push(hosting_environment(
            HostingEnvironmentType::GoogleCloudRun,
            None,
        ));
    }
    if has_env("GAE_SERVICE") {
        environments.push(hosting_environment(
            HostingEnvironmentType::GoogleAppEngine,
            None,
        ));
    }
    if has_env("WEBSITE_SITE_NAME") {
        environments.push(hosting_environment(
            HostingEnvironmentType::AzureAppService,
            env_value("WEBSITE_PLATFORM_VERSION"),
        ));
    }
    if let Some(version) = env_value("FUNCTIONS_EXTENSION_VERSION") {
        environments.push(hosting_environment(
            HostingEnvironmentType::AzureFunctions,
            Some(version),
        ));
    }
    if has_any_env(&["CONTAINER_APP_NAME", "CONTAINER_APP_JOB_NAME"]) {
        environments.push(hosting_environment(
            HostingEnvironmentType::AzureContainerApps,
            None,
        ));
    }

    environments
}

fn hosting_environment(
    environment_type: HostingEnvironmentType,
    version: Option<String>,
) -> HostingEnvironment {
    HostingEnvironment {
        r#type: environment_type as i32,
        version: version.unwrap_or_default(),
    }
}

fn env_value(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn has_env(name: &str) -> bool {
    env_value(name).is_some()
}

fn has_any_env(names: &[&str]) -> bool {
    names.iter().any(|name| has_env(name))
}

fn is_docker() -> bool {
    #[cfg(unix)]
    if Path::new("/.dockerenv").exists() {
        return true;
    }

    #[cfg(target_os = "linux")]
    if let Ok(cgroups) = fs::read_to_string("/proc/self/cgroup") {
        return cgroups.lines().any(|line| {
            let path = line.rsplit_once(':').map_or(line, |(_, path)| path);
            path.split('/').any(|component| {
                component == "docker"
                    || component
                        .strip_prefix("docker-")
                        .is_some_and(|id| id.ends_with(".scope"))
            })
        });
    }

    false
}

fn detect_platform() -> Option<Platform> {
    let version = sysinfo::System::os_version()
        .or_else(sysinfo::System::kernel_version)
        .unwrap_or_default();
    let architecture = architecture() as i32;

    #[cfg(target_os = "linux")]
    return Some(Platform {
        variant: Some(Variant::Linux(LinuxPlatform {
            version,
            architecture,
            libc: linux_libc() as i32,
        })),
    });

    #[cfg(target_os = "macos")]
    return Some(Platform {
        variant: Some(Variant::Macos(MacOsPlatform {
            version,
            architecture,
        })),
    });

    #[cfg(target_os = "windows")]
    return Some(Platform {
        variant: Some(Variant::Windows(WindowsPlatform {
            version,
            architecture,
            crt: windows_crt() as i32,
        })),
    });

    #[allow(unreachable_code)]
    None
}

fn architecture() -> Architecture {
    #[cfg(target_arch = "x86_64")]
    return Architecture::Amd64;

    #[cfg(target_arch = "aarch64")]
    return Architecture::Arm64;

    #[allow(unreachable_code)]
    Architecture::Unspecified
}

#[cfg(target_os = "linux")]
fn linux_libc() -> Libc {
    #[cfg(target_env = "gnu")]
    return Libc::Glibc;

    #[cfg(target_env = "musl")]
    return Libc::Musl;

    #[allow(unreachable_code)]
    Libc::Unspecified
}

#[cfg(target_os = "windows")]
fn windows_crt() -> Crt {
    #[cfg(target_env = "msvc")]
    return Crt::Ucrt;

    #[cfg(target_env = "gnu")]
    return Crt::Mingw;

    #[allow(unreachable_code)]
    Crt::Unspecified
}
