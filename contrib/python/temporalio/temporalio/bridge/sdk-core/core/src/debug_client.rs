//! Defines an http client that is used for the VSCode debug plugin and any other associated
//! machinery.

use anyhow::Context;
use hyper::http::HeaderValue;
use prost::Message;
use reqwest::{self, header::HeaderMap};
use std::time::Duration;
use temporal_sdk_core_protos::temporal::api::history::v1::History;
use url::Url;

/// A client for interacting with the VSCode debug plugin
#[derive(Clone)]
pub struct DebugClient {
    /// URL for the local instance of the debugger server
    debugger_url: Url,
    client: reqwest::Client,
}

#[derive(Clone, serde::Serialize)]
struct WFTStartedMsg {
    event_id: i64,
}

impl DebugClient {
    /// Create a new instance of a DebugClient with the specified url and client name/version
    /// strings.
    pub fn new(
        url: String,
        client_name: &str,
        client_version: &str,
    ) -> Result<DebugClient, anyhow::Error> {
        let mut client = reqwest::ClientBuilder::new();
        client = client.default_headers({
            let mut hm = HeaderMap::new();
            hm.insert("temporal-client-name", HeaderValue::from_str(client_name)?);
            hm.insert(
                "temporal-client-version",
                HeaderValue::from_str(client_version)?,
            );
            hm
        });
        let client = client.build()?;
        Ok(DebugClient {
            debugger_url: Url::parse(&url).context(
                "debugger url malformed, is the TEMPORAL_DEBUGGER_PLUGIN_URL env var correct?",
            )?,
            client,
        })
    }

    /// Get the history from the instance of the debug plugin server
    pub async fn get_history(&self) -> Result<History, anyhow::Error> {
        let url = self.debugger_url.join("history")?;
        let resp = self.client.get(url).send().await?;

        let bytes = resp.bytes().await?;
        Ok(History::decode(bytes)?)
    }

    /// Post to current-wft-started to tell the debug plugin which event we've most recently made it
    /// to
    pub async fn post_wft_started(
        &self,
        event_id: &i64,
    ) -> Result<reqwest::Response, anyhow::Error> {
        let url = self.debugger_url.join("current-wft-started")?;
        Ok(self
            .client
            .get(url)
            .timeout(Duration::from_secs(5))
            .json(&WFTStartedMsg {
                event_id: *event_id,
            })
            .send()
            .await?)
    }
}
