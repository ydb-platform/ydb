//! Utilities for and tracking of internal versions which alter history in incompatible ways
//! so that we can use older code paths for workflows executed on older core versions.

use itertools::Either;
use std::{
    collections::{BTreeSet, HashSet},
    iter,
};
use temporal_sdk_core_protos::temporal::api::{
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
#[repr(u32)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Debug, enum_iterator::Sequence)]
pub(crate) enum CoreInternalFlags {
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
    /// See https://github.com/temporalio/features/issues/481.
    MoveTerminalCommands = 3,
    /// We received a value higher than this code can understand.
    TooHigh = u32::MAX,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InternalFlags {
    Enabled {
        core: BTreeSet<CoreInternalFlags>,
        lang: BTreeSet<u32>,
        core_since_last_complete: HashSet<CoreInternalFlags>,
        lang_since_last_complete: HashSet<u32>,
        last_sdk_name: String,
        last_sdk_version: String,
        sdk_name: String,
        sdk_version: String,
    },
    Disabled,
}

impl InternalFlags {
    pub(crate) fn new(
        server_capabilities: &get_system_info_response::Capabilities,
        sdk_name: String,
        sdk_version: String,
    ) -> Self {
        match server_capabilities.sdk_metadata {
            true => Self::Enabled {
                core: Default::default(),
                lang: Default::default(),
                core_since_last_complete: Default::default(),
                lang_since_last_complete: Default::default(),
                last_sdk_name: "".to_string(),
                last_sdk_version: "".to_string(),
                sdk_name,
                sdk_version,
            },
            false => Self::Disabled,
        }
    }

    pub(crate) fn add_from_complete(&mut self, e: &WorkflowTaskCompletedEventAttributes) {
        if let Self::Enabled {
            core,
            lang,
            last_sdk_name,
            last_sdk_version,
            ..
        } = self
            && let Some(metadata) = e.sdk_metadata.as_ref()
        {
            core.extend(
                metadata
                    .core_used_flags
                    .iter()
                    .map(|u| CoreInternalFlags::from_u32(*u)),
            );
            lang.extend(metadata.lang_used_flags.iter());
            if !metadata.sdk_name.is_empty() {
                *last_sdk_name = metadata.sdk_name.clone();
            }
            if !metadata.sdk_version.is_empty() {
                *last_sdk_version = metadata.sdk_version.clone();
            }
        }
    }

    pub(crate) fn add_lang_used(&mut self, flags: impl IntoIterator<Item = u32>) {
        if let Self::Enabled {
            lang_since_last_complete,
            ..
        } = self
        {
            lang_since_last_complete.extend(flags);
        }
    }

    /// Returns true if this flag may currently be used. If `should_record` is true, always returns
    /// true and records the flag as being used, for taking later via
    /// [Self::gather_for_wft_complete].
    pub(crate) fn try_use(&mut self, flag: CoreInternalFlags, should_record: bool) -> bool {
        match self {
            Self::Enabled {
                core,
                core_since_last_complete,
                ..
            } => {
                if should_record {
                    core_since_last_complete.insert(flag);
                    true
                } else {
                    core.contains(&flag)
                }
            }
            // If the server does not support the metadata field, we must assume we can never use
            // any internal flags since they can't be recorded for future use
            Self::Disabled => false,
        }
    }

    /// Writes all known core flags to the set which should be recorded in the current WFT if not
    /// already known. Must only be called if not replaying.
    pub(crate) fn write_all_known(&mut self) {
        if let Self::Enabled {
            core_since_last_complete,
            ..
        } = self
        {
            core_since_last_complete.extend(CoreInternalFlags::all_except_too_high());
        }
    }

    /// Return a partially filled sdk metadata message containing core and lang flags added since
    /// the last WFT complete. The returned value can be combined with other data before sending the
    /// WFT complete.
    pub(crate) fn gather_for_wft_complete(&mut self) -> WorkflowTaskCompletedMetadata {
        match self {
            Self::Enabled {
                core_since_last_complete,
                lang_since_last_complete,
                core,
                lang,
                last_sdk_name,
                last_sdk_version,
                sdk_name,
                sdk_version,
            } => {
                let core_newly_used: Vec<_> = core_since_last_complete
                    .iter()
                    .filter(|f| !core.contains(f))
                    .map(|p| *p as u32)
                    .collect();
                let lang_newly_used: Vec<_> = lang_since_last_complete
                    .iter()
                    .filter(|f| !lang.contains(f))
                    .copied()
                    .collect();
                core.extend(core_since_last_complete.iter());
                lang.extend(lang_since_last_complete.iter());
                let sdk_name = if last_sdk_name != sdk_name {
                    sdk_name.clone()
                } else {
                    "".to_string()
                };
                let sdk_version = if last_sdk_version != sdk_version {
                    sdk_version.clone()
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
            Self::Disabled => WorkflowTaskCompletedMetadata::default(),
        }
    }

    pub(crate) fn all_lang(&self) -> impl Iterator<Item = u32> + '_ {
        match self {
            Self::Enabled { lang, .. } => Either::Left(lang.iter().copied()),
            Self::Disabled => Either::Right(iter::empty()),
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
    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::get_system_info_response::Capabilities;

    impl Default for InternalFlags {
        fn default() -> Self {
            Self::Disabled
        }
    }

    #[test]
    fn disabled_in_capabilities_disables() {
        let mut f = InternalFlags::new(
            &Capabilities::default(),
            "name".to_string(),
            "ver".to_string(),
        );
        f.add_lang_used([1]);
        f.add_from_complete(&WorkflowTaskCompletedEventAttributes {
            sdk_metadata: Some(WorkflowTaskCompletedMetadata {
                core_used_flags: vec![1],
                lang_used_flags: vec![],
                sdk_name: "".to_string(),
                sdk_version: "".to_string(),
            }),
            ..Default::default()
        });
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
