use base64::{Engine, prelude::BASE64_STANDARD};
use std::{
    borrow::Borrow,
    fmt::{Debug, Display, Formatter},
};

static LOCAL_ACT_TASK_TOKEN_PREFIX: &[u8] = b"local_act_";

#[derive(
    Hash,
    Eq,
    PartialEq,
    Clone,
    derive_more::From,
    derive_more::Into,
    serde::Serialize,
    serde::Deserialize,
)]
/// Type-safe wrapper for task token bytes
pub struct TaskToken(pub Vec<u8>);

impl TaskToken {
    /// Task tokens for local activities are always prefixed with a special sigil so they can
    /// be identified easily
    pub fn new_local_activity_token(unique_data: impl IntoIterator<Item = u8>) -> Self {
        let mut bytes = LOCAL_ACT_TASK_TOKEN_PREFIX.to_vec();
        bytes.extend(unique_data);
        TaskToken(bytes)
    }

    /// Returns true if the task token is for a local activity
    pub fn is_local_activity_task(&self) -> bool {
        self.0.starts_with(LOCAL_ACT_TASK_TOKEN_PREFIX)
    }
}

impl Display for TaskToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&fmt_tt(&self.0))
    }
}

impl Debug for TaskToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("TaskToken({})", fmt_tt(&self.0)))
    }
}

impl Borrow<[u8]> for TaskToken {
    fn borrow(&self) -> &[u8] {
        self.0.as_slice()
    }
}

pub(crate) fn fmt_tt(tt: &[u8]) -> String {
    BASE64_STANDARD.encode(tt)
}
