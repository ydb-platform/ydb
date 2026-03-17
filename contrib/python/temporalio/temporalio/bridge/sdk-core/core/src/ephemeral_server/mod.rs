//! This module implements support for downloading and running ephemeral test
//! servers useful for testing.

use anyhow::anyhow;
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use serde::Deserialize;
use std::{
    fs::OpenOptions,
    io,
    path::{Path, PathBuf},
};
use temporal_client::ClientOptionsBuilder;
use tokio::{
    task::spawn_blocking,
    time::{Duration, sleep},
};
use tokio_util::io::{StreamReader, SyncIoBridge};
use url::Url;
use zip::read::read_zipfile_from_stream;

#[cfg(target_family = "unix")]
use std::os::unix::fs::OpenOptionsExt;
use std::process::Stdio;

/// Configuration for Temporal CLI dev server.
#[derive(Debug, Clone, derive_builder::Builder)]
pub struct TemporalDevServerConfig {
    /// Required path to executable or download info.
    pub exe: EphemeralExe,
    /// Namespace to use.
    #[builder(default = "\"default\".to_owned()")]
    pub namespace: String,
    /// IP to bind to.
    #[builder(default = "\"127.0.0.1\".to_owned()")]
    pub ip: String,
    /// Port to use or obtains a free one if none given.
    #[builder(default)]
    pub port: Option<u16>,
    /// Port to use for the UI server or obtains a free one if none given.
    #[builder(default)]
    pub ui_port: Option<u16>,
    /// Sqlite DB filename if persisting or non-persistent if none.
    #[builder(default)]
    pub db_filename: Option<String>,
    /// Whether to enable the UI. If ui_port is set, assumes true.
    #[builder(default)]
    pub ui: bool,
    /// Log format and level
    #[builder(default = "(\"pretty\".to_owned(), \"warn\".to_owned())")]
    pub log: (String, String),
    /// Additional arguments to Temporal dev server.
    #[builder(default)]
    pub extra_args: Vec<String>,
}

impl TemporalDevServerConfig {
    /// Start a Temporal CLI dev server.
    pub async fn start_server(&self) -> anyhow::Result<EphemeralServer> {
        self.start_server_with_output(Stdio::inherit(), Stdio::inherit())
            .await
    }

    /// Start a Temporal CLI dev server with configurable stdout destination.
    pub async fn start_server_with_output(
        &self,
        output: Stdio,
        err_output: Stdio,
    ) -> anyhow::Result<EphemeralServer> {
        // Get exe path
        let exe_path = self
            .exe
            .get_or_download("cli", "temporal", Some("tar.gz"))
            .await?;

        // Get free port if not already given
        let port = match self.port {
            Some(p) => p,
            None => get_free_port(&self.ip)?,
        };

        // Build arg set
        let mut args = vec![
            "server".to_owned(),
            "start-dev".to_owned(),
            "--port".to_owned(),
            port.to_string(),
            "--namespace".to_owned(),
            self.namespace.clone(),
            "--ip".to_owned(),
            self.ip.clone(),
            "--log-format".to_owned(),
            self.log.0.clone(),
            "--log-level".to_owned(),
            self.log.1.clone(),
            "--dynamic-config-value".to_owned(),
            "frontend.enableServerVersionCheck=false".to_owned(),
            "--dynamic-config-value".to_owned(),
            "frontend.enableUpdateWorkflowExecution=true".to_owned(),
            "--dynamic-config-value".to_owned(),
            "frontend.enableUpdateWorkflowExecutionAsyncAccepted=true".to_owned(),
        ];
        if let Some(db_filename) = &self.db_filename {
            args.push("--db-filename".to_owned());
            args.push(db_filename.clone());
        }
        if let Some(ui_port) = self.ui_port {
            args.push("--ui-port".to_owned());
            args.push(ui_port.to_string());
        } else if self.ui {
            // Caps at u16 max which is also max port value
            let port = port.saturating_add(1000);
            args.push("--ui-port".to_owned());
            args.push(port.to_string());
        } else {
            args.push("--headless".to_owned());
        }
        args.extend(self.extra_args.clone());

        // Start
        EphemeralServer::start(EphemeralServerConfig {
            exe_path,
            port,
            args,
            has_test_service: false,
            output,
            err_output,
        })
        .await
    }
}

/// Configuration for the test server.
#[derive(Debug, Clone, derive_builder::Builder)]
pub struct TestServerConfig {
    /// Required path to executable or download info.
    pub exe: EphemeralExe,
    /// Port to use or obtains a free one if none given.
    #[builder(default)]
    pub port: Option<u16>,
    /// Additional arguments to the test server.
    #[builder(default)]
    pub extra_args: Vec<String>,
}

impl TestServerConfig {
    /// Start a test server.
    pub async fn start_server(&self) -> anyhow::Result<EphemeralServer> {
        self.start_server_with_output(Stdio::inherit(), Stdio::inherit())
            .await
    }

    /// Start a test server with configurable stdout.
    pub async fn start_server_with_output(
        &self,
        output: Stdio,
        err_output: Stdio,
    ) -> anyhow::Result<EphemeralServer> {
        // Get exe path
        let exe_path = self
            .exe
            .get_or_download("temporal-test-server", "temporal-test-server", None)
            .await?;

        // Get free port if not already given
        let port = match self.port {
            Some(p) => p,
            None => get_free_port("0.0.0.0")?,
        };

        // Build arg set
        let mut args = vec![port.to_string()];
        args.extend(self.extra_args.clone());

        // Start
        EphemeralServer::start(EphemeralServerConfig {
            exe_path,
            port,
            args,
            has_test_service: true,
            output,
            err_output,
        })
        .await
    }
}

struct EphemeralServerConfig {
    exe_path: PathBuf,
    port: u16,
    args: Vec<String>,
    has_test_service: bool,
    output: Stdio,
    err_output: Stdio,
}

/// Server that will be stopped when dropped.
#[derive(Debug)]
pub struct EphemeralServer {
    /// gRPC target host:port for the server frontend.
    pub target: String,
    /// Whether the target implements the gRPC TestService
    pub has_test_service: bool,
    child: tokio::process::Child,
}

impl EphemeralServer {
    async fn start(config: EphemeralServerConfig) -> anyhow::Result<EphemeralServer> {
        // Start process
        let child = tokio::process::Command::new(config.exe_path)
            .args(config.args)
            .stdin(Stdio::null())
            .stdout(config.output)
            .stderr(config.err_output)
            .spawn()?;
        let target = format!("127.0.0.1:{}", config.port);
        let target_url = format!("http://{target}");
        let success = Ok(EphemeralServer {
            target,
            has_test_service: config.has_test_service,
            child,
        });

        // Try to connect every 100ms for 5s
        // TODO(cretz): Some other way, e.g. via stdout, to know whether the
        // server is up?
        let client_options = ClientOptionsBuilder::default()
            .identity("online_checker".to_owned())
            .target_url(Url::parse(&target_url)?)
            .client_name("online-checker".to_owned())
            .client_version("0.1.0".to_owned())
            .build()?;
        let mut last_error = None;
        for _ in 0..50 {
            sleep(Duration::from_millis(100)).await;
            let connect_res = client_options.connect_no_namespace(None).await;
            if let Err(err) = connect_res {
                last_error = Some(err);
            } else {
                return success;
            }
        }
        Err(anyhow!(
            "Failed connecting to test server after 5 seconds, last error: {:?}",
            last_error
        ))
    }

    /// Shutdown the server (i.e. kill the child process). This does not attempt
    /// a kill if the child process appears completed, but such a check is not
    /// atomic so a kill could still fail as completed if completed just before
    /// kill.
    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        // Only kill if there is a PID
        if self.child.id().is_some() {
            Ok(self.child.kill().await?)
        } else {
            Ok(())
        }
    }

    /// Get the process ID of the child. This will be None if the process is
    /// considered to be complete.
    pub fn child_process_id(&self) -> Option<u32> {
        self.child.id()
    }
}

/// Where to find an executable. Can be a path or download.
#[derive(Debug, Clone)]
pub enum EphemeralExe {
    /// Existing path on the filesystem for the executable.
    ExistingPath(String),
    /// Download the executable if not already there.
    CachedDownload {
        /// Which version to download.
        version: EphemeralExeVersion,
        /// Destination directory or the user temp directory if none set.
        dest_dir: Option<String>,
        /// How long to cache the download for. None means forever.
        ttl: Option<Duration>,
    },
}

/// Which version of the exe to download.
#[derive(Debug, Clone)]
pub enum EphemeralExeVersion {
    /// Use a default version for the given SDK name and version.
    SDKDefault {
        /// Name of the SDK to get the default for.
        sdk_name: String,
        /// Version of the SDK to get the default for.
        sdk_version: String,
    },
    /// Specific version.
    Fixed(String),
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct DownloadInfo {
    archive_url: String,
    file_to_extract: String,
}

impl EphemeralExe {
    async fn get_or_download(
        &self,
        artifact_name: &str,
        downloaded_name_prefix: &str,
        preferred_format: Option<&str>,
    ) -> anyhow::Result<PathBuf> {
        match self {
            EphemeralExe::ExistingPath(exe_path) => {
                let path = PathBuf::from(exe_path);
                if !path.exists() {
                    return Err(anyhow!("Exe path does not exist"));
                }
                Ok(path)
            }
            EphemeralExe::CachedDownload {
                version,
                dest_dir,
                ttl,
            } => {
                let dest_dir = dest_dir
                    .as_ref()
                    .map(PathBuf::from)
                    .unwrap_or_else(std::env::temp_dir);
                let (platform, out_ext) = match std::env::consts::OS {
                    "windows" => ("windows", ".exe"),
                    "macos" => ("darwin", ""),
                    _ => ("linux", ""),
                };
                // Create dest file based on SDK name/version or fixed version
                let dest = dest_dir.join(match version {
                    EphemeralExeVersion::SDKDefault {
                        sdk_name,
                        sdk_version,
                    } => format!("{downloaded_name_prefix}-{sdk_name}-{sdk_version}{out_ext}"),
                    EphemeralExeVersion::Fixed(version) => {
                        format!("{downloaded_name_prefix}-{version}{out_ext}")
                    }
                });
                debug!(
                    "Lazily downloading or using existing exe at {}",
                    dest.display()
                );

                if dest.exists() && remove_file_past_ttl(ttl, &dest)? {
                    return Ok(dest);
                }

                // Get info about the proper archive and in-archive file
                let arch = match std::env::consts::ARCH {
                    "x86_64" => "amd64",
                    "arm" | "aarch64" => "arm64",
                    other => return Err(anyhow!("Unsupported arch: {}", other)),
                };
                let mut get_info_params = vec![("arch", arch), ("platform", platform)];
                if let Some(format) = preferred_format {
                    get_info_params.push(("format", format));
                }
                let version_name = match version {
                    EphemeralExeVersion::SDKDefault {
                        sdk_name,
                        sdk_version,
                    } => {
                        get_info_params.push(("sdk-name", sdk_name.as_str()));
                        get_info_params.push(("sdk-version", sdk_version.as_str()));
                        "default"
                    }
                    EphemeralExeVersion::Fixed(version) => version,
                };
                let client = reqwest::Client::new();
                let resp = client
                    .get(format!(
                        "https://temporal.download/{artifact_name}/{version_name}"
                    ))
                    .query(&get_info_params)
                    .send()
                    .await?
                    .error_for_status()?;
                let info: DownloadInfo = resp.json().await?;

                // Attempt download, looping because it could have waited for
                // concurrent one to finish
                loop {
                    if lazy_download_exe(
                        &client,
                        &info.archive_url,
                        Path::new(&info.file_to_extract),
                        &dest,
                        false,
                    )
                    .await?
                    {
                        return Ok(dest);
                    }
                }
            }
        }
    }
}

/// Returns a TCP port that is available to listen on for the given local host.
///
/// This works by binding a new TCP socket on port 0, which requests the OS to
/// allocate a free port. There is no strict guarantee that the port will remain
/// available after this function returns, but it should be safe to assume that
/// a given port will not be allocated again to any process on this machine
/// within a few seconds.
///
/// On Unix-based systems, binding to the port returned by this function
/// requires setting the `SO_REUSEADDR` socket option (Rust already does that by
/// default, but other languages may not); otherwise, the OS may fail with a
/// message such as "address already in use". Windows default behavior is
/// already appropriate in this regard; on that platform, `SO_REUSEADDR` has a
/// different meaning and should not be set (setting it may have unpredictable
/// consequences).
fn get_free_port(bind_ip: &str) -> io::Result<u16> {
    let listen = std::net::TcpListener::bind((bind_ip, 0))?;
    let addr = listen.local_addr()?;

    // On Linux and some BSD variants, ephemeral ports are randomized, and may
    // consequently repeat within a short time frame after the listening end
    // has been closed. To avoid this, we make a connection to the port, then
    // close that connection from the server's side (this is very important),
    // which puts the connection in TIME_WAIT state for some time (by default,
    // 60s on Linux). While it remains in that state, the OS will not reallocate
    // that port number for bind(:0) syscalls, yet we are not prevented from
    // explicitly binding to it (thanks to SO_REUSEADDR).
    //
    // On macOS and Windows, the above technique is not necessary, as the OS
    // allocates ephemeral ports sequentially, meaning a port number will only
    // be reused after the entire range has been exhausted. Quite the opposite,
    // given that these OSes use a significantly smaller range for ephemeral
    // ports, making an extra connection just to reserve a port might actually
    // be harmful (by hastening ephemeral port exhaustion).
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        // Establish a connection to the bind_ip:port
        let _stream = std::net::TcpStream::connect(addr)?;

        // Accept the connection from the listening side
        let (socket, _addr) = listen.accept()?;

        // Explicitly drop the socket to close the connection from the listening side first
        drop(socket);
    }

    Ok(addr.port())
}

/// Returns false if we successfully waited for another download to complete, or
/// true if the destination is known to exist. Should call again if false is
/// returned.
async fn lazy_download_exe(
    client: &reqwest::Client,
    uri: &str,
    file_to_extract: &Path,
    dest: &Path,
    already_tried_cleaning_old: bool,
) -> anyhow::Result<bool> {
    // If it already exists, do not extract
    if dest.exists() {
        return Ok(true);
    }

    // We only want to download if we're not already downloading. To avoid some
    // kind of global lock, we'll just create the file eagerly w/ a temp
    // filename and delete it on failure or move it on success. If the temp file
    // already exists, we'll wait a bit and re-run this.
    let temp_dest_str = format!("{}{}", dest.to_str().unwrap(), ".downloading");
    let temp_dest = Path::new(&temp_dest_str);
    // Try to open file, using a file mode on unix families
    #[cfg(target_family = "unix")]
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o755)
        .open(temp_dest);
    #[cfg(not(target_family = "unix"))]
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(temp_dest);
    // This match only gets Ok if the file was downloaded and extracted to the
    // temporary path
    match file {
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            // If the download lock file exists but is old, delete it and try again, since it may
            // have been left by an abandoned process.
            if !already_tried_cleaning_old
                && temp_dest.metadata()?.modified()?.elapsed()?.as_secs() > 90
            {
                std::fs::remove_file(temp_dest)?;
                return Box::pin(lazy_download_exe(client, uri, file_to_extract, dest, true)).await;
            }

            // Since it already exists, we'll try once a second for 20 seconds
            // to wait for it to be done, then return false so the caller can
            // try again.
            for _ in 0..20 {
                sleep(Duration::from_secs(1)).await;
                if !temp_dest.exists() {
                    return Ok(false);
                }
            }
            Err(anyhow!(
                "Temp download file at {} not complete after 20 seconds. \
                Make sure another download isn't running for too long and delete the temp file.",
                temp_dest.display()
            ))
        }
        Err(err) => Err(err.into()),
        // If the dest was added since, just remove temp file
        Ok(_) if dest.exists() => {
            std::fs::remove_file(temp_dest)?;
            return Ok(true);
        }
        // Download and extract the binary
        Ok(mut temp_file) => {
            info!("Downloading {} to {}", uri, dest.display());
            download_and_extract(client, uri, file_to_extract, &mut temp_file)
                .await
                .inspect_err(|_| {
                    // Failed to download, just remove file
                    if let Err(err) = std::fs::remove_file(temp_dest) {
                        warn!(
                            "Failed removing temp file at {}: {:?}",
                            temp_dest.display(),
                            err
                        );
                    }
                })
        }
    }?;
    // Now that file should be dropped, we can rename
    std::fs::rename(temp_dest, dest)?;
    Ok(true)
}

async fn download_and_extract(
    client: &reqwest::Client,
    uri: &str,
    file_to_extract: &Path,
    dest: &mut std::fs::File,
) -> anyhow::Result<()> {
    // Start download. We are using streaming here to extract the file from the
    // tarball or zip instead of loading into memory for Cursor/Seek.
    let resp = client.get(uri).send().await?.error_for_status()?;
    // We have to map the error type to an io error
    let stream = resp
        .bytes_stream()
        .map(|item| item.map_err(io::Error::other));

    // Since our tar/zip impls use sync IO, we have to create a bridge and run
    // in a blocking closure.
    let mut reader = SyncIoBridge::new(StreamReader::new(stream));
    let tarball = if uri.ends_with(".tar.gz") {
        true
    } else if uri.ends_with(".zip") {
        false
    } else {
        return Err(anyhow!("URI not .tar.gz or .zip"));
    };
    let file_to_extract = file_to_extract.to_path_buf();
    let mut dest = dest.try_clone()?;

    spawn_blocking(move || {
        if tarball {
            for entry in tar::Archive::new(GzDecoder::new(reader)).entries()? {
                let mut entry = entry?;
                if entry.path()? == file_to_extract {
                    std::io::copy(&mut entry, &mut dest)?;
                    return Ok(());
                }
            }
            Err(anyhow!("Unable to find file in tarball"))
        } else {
            loop {
                // This is the way to stream a zip file without creating an archive
                // that requires Seek.
                if let Some(mut file) = read_zipfile_from_stream(&mut reader)? {
                    // If this is the file we're expecting, extract it
                    if file.enclosed_name().as_ref() == Some(&file_to_extract) {
                        std::io::copy(&mut file, &mut dest)?;
                        return Ok(());
                    }
                } else {
                    return Err(anyhow!("Unable to find file in zip"));
                }
            }
        }
    })
    .await?
}

/// Remove the file if it's older than the TTL. Returns true if the current file can be re-used,
/// returns false if it was removed or should otherwise be re-downloaded.
fn remove_file_past_ttl(ttl: &Option<Duration>, dest: &PathBuf) -> Result<bool, anyhow::Error> {
    match ttl {
        None => return Ok(true),
        Some(ttl) => {
            if let Ok(mtime) = dest.metadata().and_then(|d| d.modified()) {
                if mtime.elapsed().unwrap_or_default().lt(ttl) {
                    return Ok(true);
                } else {
                    // Remove so we can re-download
                    std::fs::remove_file(dest)?;
                }
            }
            // If we couldn't read the mtime something weird is probably up, so
            // re-download
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::{get_free_port, remove_file_past_ttl};
    use std::{
        env::temp_dir,
        fs::File,
        net::{TcpListener, TcpStream},
        time::{Duration, SystemTime},
    };

    #[test]
    fn get_free_port_can_bind_immediately() {
        let host = "127.0.0.1";

        for _ in 0..500 {
            let port = get_free_port(host).unwrap();
            try_listen_and_dial_on(host, port).expect("Failed to bind to port");
        }
    }

    #[tokio::test]
    async fn respects_file_ttl() {
        let rand_fname = format!("{}", rand::random::<u64>());
        let temp_dir = temp_dir();

        let dest_file_path = temp_dir.join(format!("core-test-{}", &rand_fname));
        let dest_file = File::create(&dest_file_path).unwrap();
        let set_time_to = SystemTime::now() - Duration::from_secs(100);
        dest_file.set_modified(set_time_to).unwrap();

        remove_file_past_ttl(&Some(Duration::from_secs(60)), &dest_file_path).unwrap();

        // file should be gone
        assert!(!dest_file_path.exists());
    }

    fn try_listen_and_dial_on(host: &str, port: u16) -> std::io::Result<()> {
        let listener = TcpListener::bind((host, port))?;
        let _stream = TcpStream::connect((host, port))?;

        // Accept the connection from the listening side
        let (socket, _addr) = listener.accept()?;

        // Explicitly drop the socket to close the connection from the listening side first
        drop(socket);

        Ok(())
    }
}
