use std::time::Duration;

use crate::ByteArray;
use crate::ByteArrayRef;
use crate::UserDataHandle;
use crate::runtime::Runtime;

use temporal_sdk_core::ephemeral_server;

pub struct EphemeralServer {
    runtime: Runtime,
    server: Option<ephemeral_server::EphemeralServer>,
}

#[repr(C)]
pub struct DevServerOptions {
    /// Must always be present
    pub test_server: *const TestServerOptions,
    pub namespace: ByteArrayRef,
    pub ip: ByteArrayRef,
    /// Empty means default behavior
    pub database_filename: ByteArrayRef,
    pub ui: bool,
    pub ui_port: u16,
    pub log_format: ByteArrayRef,
    pub log_level: ByteArrayRef,
}

#[repr(C)]
pub struct TestServerOptions {
    /// Empty means default behavior
    pub existing_path: ByteArrayRef,
    pub sdk_name: ByteArrayRef,
    pub sdk_version: ByteArrayRef,
    pub download_version: ByteArrayRef,
    /// Empty means default behavior
    pub download_dest_dir: ByteArrayRef,
    /// 0 means default behavior
    pub port: u16,
    /// Newline delimited
    pub extra_args: ByteArrayRef,
    /// 0 means no TTL
    pub download_ttl_seconds: u64,
}

/// Anything besides user data must be freed if non-null.
pub type EphemeralServerStartCallback = unsafe extern "C" fn(
    user_data: *mut libc::c_void,
    success: *mut EphemeralServer,
    success_target: *const ByteArray,
    fail: *const ByteArray,
);

/// Runtime must live as long as server. Options and user data must live through
/// callback.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_ephemeral_server_start_dev_server(
    runtime: *mut Runtime,
    options: *const DevServerOptions,
    user_data: *mut libc::c_void,
    callback: EphemeralServerStartCallback,
) {
    let runtime = unsafe { &mut *runtime };
    // Convert opts
    let options = unsafe { &*options };
    let config: ephemeral_server::TemporalDevServerConfig = match options.try_into() {
        Ok(v) => v,
        Err(err) => {
            unsafe {
                callback(
                    user_data,
                    std::ptr::null_mut(),
                    std::ptr::null(),
                    runtime
                        .alloc_utf8(&format!("Invalid options: {err}"))
                        .into_raw(),
                );
            }
            return;
        }
    };
    // Spawn async call
    let user_data = UserDataHandle(user_data);
    runtime.core.tokio_handle().spawn(async move {
        match config.start_server().await {
            Ok(server) => {
                let target = runtime.alloc_utf8(&server.target).into_raw();
                let owned_server = Box::into_raw(Box::new(EphemeralServer {
                    runtime: runtime.clone(),
                    server: Some(server),
                }));
                unsafe {
                    callback(user_data.into(), owned_server, target, std::ptr::null());
                }
            }
            Err(err) => unsafe {
                callback(
                    user_data.into(),
                    std::ptr::null_mut(),
                    std::ptr::null(),
                    runtime
                        .alloc_utf8(&format!("Connection failed: {err}"))
                        .into_raw(),
                );
            },
        }
    });
}

/// Runtime must live as long as server. Options and user data must live through
/// callback.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_ephemeral_server_start_test_server(
    runtime: *mut Runtime,
    options: *const TestServerOptions,
    user_data: *mut libc::c_void,
    callback: EphemeralServerStartCallback,
) {
    let runtime = unsafe { &mut *runtime };
    // Convert opts
    let options = unsafe { &*options };
    let config: ephemeral_server::TestServerConfig = match options.try_into() {
        Ok(v) => v,
        Err(err) => {
            unsafe {
                callback(
                    user_data,
                    std::ptr::null_mut(),
                    std::ptr::null(),
                    runtime
                        .alloc_utf8(&format!("Invalid options: {err}"))
                        .into_raw(),
                );
            }
            return;
        }
    };
    // Spawn async call
    let user_data = UserDataHandle(user_data);
    runtime.core.tokio_handle().spawn(async move {
        match config.start_server().await {
            Ok(server) => {
                let target = runtime.alloc_utf8(&server.target).into_raw();
                let owned_server = Box::into_raw(Box::new(EphemeralServer {
                    runtime: runtime.clone(),
                    server: Some(server),
                }));
                unsafe {
                    callback(user_data.into(), owned_server, target, std::ptr::null());
                }
            }
            Err(err) => unsafe {
                callback(
                    user_data.into(),
                    std::ptr::null_mut(),
                    std::ptr::null(),
                    runtime
                        .alloc_utf8(&format!("Connection failed: {err}"))
                        .into_raw(),
                );
            },
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_ephemeral_server_free(server: *mut EphemeralServer) {
    unsafe {
        let _ = Box::from_raw(server);
    }
}

pub type EphemeralServerShutdownCallback =
    unsafe extern "C" fn(user_data: *mut libc::c_void, fail: *const ByteArray);

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_ephemeral_server_shutdown(
    server: *mut EphemeralServer,
    user_data: *mut libc::c_void,
    callback: EphemeralServerShutdownCallback,
) {
    let server = unsafe { &mut *server };
    let eph_server = server.server.take();
    let user_data = UserDataHandle(user_data);
    server.runtime.core.tokio_handle().spawn(async move {
        let fail = if let Some(mut eph_server) = eph_server {
            if let Err(err) = eph_server.shutdown().await {
                server
                    .runtime
                    .alloc_utf8(&format!("Failed shutting down server: {err}"))
                    .into_raw()
            } else {
                std::ptr::null_mut()
            }
        } else {
            std::ptr::null_mut()
        };
        unsafe {
            callback(user_data.into(), fail);
        }
    });
}

impl TryFrom<&DevServerOptions> for ephemeral_server::TemporalDevServerConfig {
    type Error = anyhow::Error;

    fn try_from(options: &DevServerOptions) -> anyhow::Result<Self> {
        let test_server_options = unsafe { &*options.test_server };
        Ok(ephemeral_server::TemporalDevServerConfigBuilder::default()
            .exe(test_server_options.exe())
            .namespace(options.namespace.to_string())
            .ip(options.ip.to_string())
            .port(test_server_options.port())
            .db_filename(options.database_filename.to_option_string())
            .ui(options.ui)
            .ui_port(if options.ui_port == 0 || !options.ui {
                None
            } else {
                Some(options.ui_port)
            })
            .log((
                options.log_format.to_string(),
                options.log_level.to_string(),
            ))
            .extra_args(test_server_options.extra_args())
            .build()?)
    }
}

impl TryFrom<&TestServerOptions> for ephemeral_server::TestServerConfig {
    type Error = anyhow::Error;

    fn try_from(options: &TestServerOptions) -> anyhow::Result<Self> {
        Ok(ephemeral_server::TestServerConfigBuilder::default()
            .exe(options.exe())
            .port(options.port())
            .extra_args(options.extra_args())
            .build()?)
    }
}

impl TestServerOptions {
    fn exe(&self) -> ephemeral_server::EphemeralExe {
        if let Some(existing_path) = self.existing_path.to_option_string() {
            ephemeral_server::EphemeralExe::ExistingPath(existing_path)
        } else {
            ephemeral_server::EphemeralExe::CachedDownload {
                version: match self.download_version.to_str() {
                    "default" => ephemeral_server::EphemeralExeVersion::SDKDefault {
                        sdk_name: self.sdk_name.to_string(),
                        sdk_version: self.sdk_version.to_string(),
                    },
                    download_version => {
                        ephemeral_server::EphemeralExeVersion::Fixed(download_version.to_string())
                    }
                },
                dest_dir: self.download_dest_dir.to_option_string(),
                ttl: if self.download_ttl_seconds == 0 {
                    None
                } else {
                    Some(Duration::from_secs(self.download_ttl_seconds))
                },
            }
        }
    }

    fn port(&self) -> Option<u16> {
        if self.port == 0 {
            None
        } else {
            Some(self.port)
        }
    }

    fn extra_args(&self) -> Vec<String> {
        if let Some(extra_args) = self.extra_args.to_option_str() {
            extra_args.split('\n').map(str::to_string).collect()
        } else {
            Vec::new()
        }
    }
}
