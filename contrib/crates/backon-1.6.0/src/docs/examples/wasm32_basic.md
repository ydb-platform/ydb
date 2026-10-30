Retry an async function in a wasm32 environment using `backon` for exponential backoff, and `wasm-bindgen` + `spawn_local` to run async code in the browser.

```rust
use anyhow::Result;
use backon::{ExponentialBuilder, Retryable};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

async fn fetch() -> Result<String> {
    Ok("Hello, wasm32!".to_string())
}

#[wasm_bindgen(start)]
fn start() {
    spawn_local(async {
        match fetch.retry(ExponentialBuilder::default()).await {
            Ok(content) => web_sys::console::log_1(&format!("fetch succeeded: {}", content).into()),
            Err(e) => web_sys::console::error_1(&format!("fetch failed: {:?}", e).into()),
        }
    });
}

```