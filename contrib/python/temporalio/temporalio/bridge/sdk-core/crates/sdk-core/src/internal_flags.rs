//! Utilities for and tracking of internal versions which alter history in incompatible ways
//! so that we can use older code paths for workflows executed on older core versions.

use std::collections::{BTreeSet, HashSet};
use temporalio_common::protos::temporal::api::{
    history::v1::WorkflowTaskCompletedEventAttributes, sdk::v1::WorkflowTaskCompletedMetadata,
    workflowservice::v1::get_system_info_response,
};

/// This enumeration contains internal flags that may result in incompatible history changes with
/// older workflows, or other breaking changes.
///
/// When a flag has existed long enough that the version it was introduced in is no longer supported, it
/// may be removed from the enum. *Importantly*, all variants must be given explicit values, such
/// that removing older variants does not create any change in existing values. Removed flag
/// variants must be reserved forever (a-la protobuf), and should be called out in a comment.
#[allow(unreachable_pub)] // re-exported in test_help::integ_helpers
#[repr(u32)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Debug, enum_iterator::Sequence)]
pub enum CoreInternalFlags {
    /// In this flag additional checks were added to a number of state machines to ensure that
    /// the ID and type of activities, local activities, and child workflows match during replay.
    IdAndTypeDeterminismChecks = 1,
    /// Introduced automatically upserting search attributes for each patched call, and
    /// nondeterminism checks for upserts.
    UpsertSearchAttributeOnPatch = 2,
    /// Prior to this flag, we truncated commands received from lang at the
    /// first terminal (i.e. workflow-terminating) command. With this flag, we
    /// reorder commands such that all non-terminal commands come first,
    /// followed by the first terminal command, if any (it's possible that
    /// multiple workflow coroutines generated a terminal command). This has the
    /// consequence that all non-terminal commands are sent to the server, even
    /// if in the sequence delivered by lang they came after a terminal command.
    /// See <https://github.com/temporalio/features/issues/481>.
    MoveTerminalCommands = 3,
    /// We received a value higher than this code can understand.
    TooHigh = u32::MAX,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InternalFlags {
    can_write_sdk_metadata: bool,
    core: BTreeSet<CoreInternalFlags>,
    lang: BTreeSet<u32>,
    core_since_last_complete: HashSet<CoreInternalFlags>,
    lang_since_last_complete: HashSet<u32>,
    last_sdk_name: String,
    last_sdk_version: String,
    sdk_name: String,
    sdk_version: String,
}

impl InternalFlags {
    pub(crate) fn new(
        server_capabilities: &get_system_info_response::Capabilities,
        sdk_name: String,
        sdk_version: String,
    ) -> Self {
        Self {
            can_write_sdk_metadata: server_capabilities.sdk_metadata,
            core: Default::default(),
            lang: Default::default(),
            core_since_last_complete: Default::default(),
            lang_since_last_complete: Default::default(),
            last_sdk_name: "".to_string(),
            last_sdk_version: "".to_string(),
            sdk_name,
            sdk_version,
        }
    }

    pub(crate) fn add_from_complete(&mut self, e: &WorkflowTaskCompletedEventAttributes) {
        if let Some(metadata) = e.sdk_metadata.as_ref() {
            self.core.extend(
                metadata
                    .core_used_flags
                    .iter()
                    .map(|u| CoreInternalFlags::from_u32(*u)),
            );
            self.lang.extend(metadata.lang_used_flags.iter());
            if !metadata.sdk_name.is_empty() {
                self.last_sdk_name = metadata.sdk_name.clone();
            }
            if !metadata.sdk_version.is_empty() {
                self.last_sdk_version = metadata.sdk_version.clone();
            }
        }
    }

    pub(crate) fn add_lang_used(&mut self, flags: impl IntoIterator<Item = u32>) {
        if self.can_write_sdk_metadata {
            self.lang_since_last_complete.extend(flags);
        }
    }

    /// Returns true if this flag may currently be used. If `should_record` is true, and new SDK
    /// metadata can be written, returns true and records the flag.
    pub(crate) fn try_use(&mut self, flag: CoreInternalFlags, should_record: bool) -> bool {
        if should_record {
            if self.can_write_sdk_metadata {
                self.core_since_last_complete.insert(flag);
                true
            } else {
                false
            }
        } else {
            self.core.contains(&flag)
        }
    }

    /// Writes all known core flags to the set which should be recorded in the current WFT if not
    /// already known. Must only be called if not replaying.
    pub(crate) fn write_all_known(&mut self) {
        if self.can_write_sdk_metadata {
            self.core_since_last_complete
                .extend(CoreInternalFlags::all_except_too_high());
        }
    }

    /// Return a partially filled sdk metadata message containing core and lang flags added since
    /// the last WFT complete. The returned value can be combined with other data before sending the
    /// WFT complete.
    pub(crate) fn gather_for_wft_complete(&mut self) -> WorkflowTaskCompletedMetadata {
        if !self.can_write_sdk_metadata {
            return WorkflowTaskCompletedMetadata::default();
        }
        let core_newly_used: Vec<_> = self
            .core_since_last_complete
            .iter()
            .filter(|f| !self.core.contains(f))
            .map(|p| *p as u32)
            .collect();
        let lang_newly_used: Vec<_> = self
            .lang_since_last_complete
            .iter()
            .filter(|f| !self.lang.contains(f))
            .copied()
            .collect();
        self.core.extend(self.core_since_last_complete.iter());
        self.lang.extend(self.lang_since_last_complete.iter());
        let sdk_name = if self.last_sdk_name != self.sdk_name {
            self.sdk_name.clone()
        } else {
            "".to_string()
        };
        let sdk_version = if self.last_sdk_version != self.sdk_version {
            self.sdk_version.clone()
        } else {
            "".to_string()
        };
        WorkflowTaskCompletedMetadata {
            core_used_flags: core_newly_used,
            lang_used_flags: lang_newly_used,
            sdk_name,
            sdk_version,
        }
    }

    pub(crate) fn all_lang(&self) -> impl Iterator<Item = u32> + '_ {
        self.lang.iter().copied()
    }

    pub(crate) fn last_sdk_version(&self) -> Option<&str> {
        if !self.last_sdk_version.is_empty() {
            Some(&self.last_sdk_version)
        } else {
            None
        }
    }
}

impl CoreInternalFlags {
    fn from_u32(v: u32) -> Self {
        match v {
            1 => Self::IdAndTypeDeterminismChecks,
            2 => Self::UpsertSearchAttributeOnPatch,
            3 => Self::MoveTerminalCommands,
            _ => Self::TooHigh,
        }
    }

    pub(crate) fn all_except_too_high() -> impl Iterator<Item = CoreInternalFlags> {
        enum_iterator::all::<CoreInternalFlags>()
            .filter(|f| !matches!(f, CoreInternalFlags::TooHigh))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common::protos::temporal::api::workflowservice::v1::get_system_info_response::Capabilities;

    impl Default for InternalFlags {
        fn default() -> Self {
            Self::new(&Capabilities::default(), "".to_string(), "".to_string())
        }
    }

    #[test]
    fn metadata_disabled_honors_flags_from_history() {
        let mut f = InternalFlags::new(
            &Capabilities::default(),
            "name".to_string(),
            "ver".to_string(),
        );
        f.add_from_complete(&WorkflowTaskCompletedEventAttributes {
            sdk_metadata: Some(WorkflowTaskCompletedMetadata {
                core_used_flags: vec![1],
                lang_used_flags: vec![2],
                sdk_name: "".to_string(),
                sdk_version: "".to_string(),
            }),
            ..Default::default()
        });

        assert!(f.try_use(CoreInternalFlags::IdAndTypeDeterminismChecks, false));
        assert!(f.all_lang().any(|flag| flag == 2));
    }

    #[test]
    fn metadata_disabled_does_not_record_new_flags() {
        let mut f = InternalFlags::new(
            &Capabilities::default(),
            "name".to_string(),
            "ver".to_string(),
        );
        f.add_lang_used([1]);

        assert!(!f.try_use(CoreInternalFlags::IdAndTypeDeterminismChecks, true));

        f.write_all_known();
        let gathered = f.gather_for_wft_complete();
        assert_matches!(gathered.core_used_flags.as_slice(), &[]);
        assert_matches!(gathered.lang_used_flags.as_slice(), &[]);
    }

    #[test]
    fn all_have_u32_from_impl() {
        let all_known = CoreInternalFlags::all_except_too_high();
        for flag in all_known {
            let as_u32 = flag as u32;
            assert_eq!(CoreInternalFlags::from_u32(as_u32), flag);
        }
    }

    #[test]
    fn only_writes_new_flags_and_sdk_info() {
        let mut f = InternalFlags::new(
            &Capabilities {
                sdk_metadata: true,
                ..Default::default()
            },
            "name".to_string(),
            "ver".to_string(),
        );
        f.add_lang_used([1]);
        f.try_use(CoreInternalFlags::IdAndTypeDeterminismChecks, true);
        let gathered = f.gather_for_wft_complete();
        assert_matches!(gathered.core_used_flags.as_slice(), &[1]);
        assert_matches!(gathered.lang_used_flags.as_slice(), &[1]);
        assert_matches!(gathered.sdk_name.as_str(), "name");
        assert_matches!(gathered.sdk_version.as_str(), "ver");

        f.add_from_complete(&WorkflowTaskCompletedEventAttributes {
            sdk_metadata: Some(WorkflowTaskCompletedMetadata {
                core_used_flags: vec![2],
                lang_used_flags: vec![2],
                sdk_name: "name".to_string(),
                sdk_version: "ver".to_string(),
            }),
            ..Default::default()
        });
        f.add_lang_used([2]);
        f.try_use(CoreInternalFlags::UpsertSearchAttributeOnPatch, true);
        let gathered = f.gather_for_wft_complete();
        assert_matches!(gathered.core_used_flags.as_slice(), &[]);
        assert_matches!(gathered.lang_used_flags.as_slice(), &[]);
        assert!(gathered.sdk_name.is_empty());
        assert!(gathered.sdk_version.is_empty());

        f.add_from_complete(&WorkflowTaskCompletedEventAttributes {
            sdk_metadata: Some(WorkflowTaskCompletedMetadata::default()),
            ..Default::default()
        });
        let gathered = f.gather_for_wft_complete();
        assert_matches!(gathered.core_used_flags.as_slice(), &[]);
        assert_matches!(gathered.lang_used_flags.as_slice(), &[]);
        assert!(gathered.sdk_name.is_empty());
        assert!(gathered.sdk_version.is_empty());

        f.add_from_complete(&WorkflowTaskCompletedEventAttributes {
            sdk_metadata: Some(WorkflowTaskCompletedMetadata {
                sdk_name: "other sdk".to_string(),
                sdk_version: "other ver".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        });
        let gathered = f.gather_for_wft_complete();
        assert_matches!(gathered.sdk_name.as_str(), "name");
        assert_matches!(gathered.sdk_version.as_str(), "ver");

        f.add_from_complete(&WorkflowTaskCompletedEventAttributes {
            sdk_metadata: Some(WorkflowTaskCompletedMetadata {
                sdk_name: "name".to_string(),
                sdk_version: "ver2".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        });
        let gathered = f.gather_for_wft_complete();
        assert!(gathered.sdk_name.is_empty());
        assert_matches!(gathered.sdk_version.as_str(), "ver");
    }
}
