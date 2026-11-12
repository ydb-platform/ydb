Retry an async function with the `Retry-After` headers.

```no_run
use core::time::Duration;
use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;

use anyhow::Result;
use backon::ExponentialBuilder;
use backon::Retryable;
use reqwest::header::HeaderMap;
use reqwest::StatusCode;

#[derive(Debug)]
struct HttpError {
    headers: HeaderMap,
}

impl Display for HttpError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "http error")
    }
}

impl Error for HttpError {}

async fn fetch() -> Result<String> {
    let resp = reqwest::get("https://www.rust-lang.org").await?;
    if resp.status() != StatusCode::OK {
        let source = HttpError {
            headers: resp.headers().clone(),
        };
        return Err(anyhow::Error::new(source));
    }
    Ok(resp.text().await?)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let content = fetch
        .retry(ExponentialBuilder::default())
        .adjust(|err, dur| {
            match err.downcast_ref::<HttpError>() {
                Some(v) => {
                    if let Some(retry_after) = v.headers.get("Retry-After") {
                        // Parse the Retry-After header and adjust the backoff duration
                        let retry_after = retry_after.to_str().unwrap_or("0");
                        let retry_after = retry_after.parse::<u64>().unwrap_or(0);
                        Some(Duration::from_secs(retry_after))
                    } else {
                        dur
                    }
                }
                None => dur,
            }
        })
        .await?;
    println!("fetch succeeded: {}", content);

    Ok(())
}
```
