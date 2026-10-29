//! WASM-safe worker-related shared types exposed through workflow APIs.

use crate::protos::{coresdk, temporal};
use std::str::FromStr;

/// Identifies a specific version of a worker deployment.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct WorkerDeploymentVersion {
    /// Name of the deployment
    pub deployment_name: String,
    /// Build ID for the worker.
    pub build_id: String,
}

impl WorkerDeploymentVersion {
    /// Returns true if both the deployment name and build ID are empty.
    pub fn is_empty(&self) -> bool {
        self.deployment_name.is_empty() && self.build_id.is_empty()
    }
}

impl FromStr for WorkerDeploymentVersion {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once('.') {
            Some((name, build_id)) => Ok(WorkerDeploymentVersion {
                deployment_name: name.to_owned(),
                build_id: build_id.to_owned(),
            }),
            _ => Err(()),
        }
    }
}

impl From<WorkerDeploymentVersion> for coresdk::common::WorkerDeploymentVersion {
    fn from(v: WorkerDeploymentVersion) -> coresdk::common::WorkerDeploymentVersion {
        coresdk::common::WorkerDeploymentVersion {
            deployment_name: v.deployment_name,
            build_id: v.build_id,
        }
    }
}

impl From<coresdk::common::WorkerDeploymentVersion> for WorkerDeploymentVersion {
    fn from(v: coresdk::common::WorkerDeploymentVersion) -> WorkerDeploymentVersion {
        WorkerDeploymentVersion {
            deployment_name: v.deployment_name,
            build_id: v.build_id,
        }
    }
}

impl From<temporal::api::deployment::v1::WorkerDeploymentVersion> for WorkerDeploymentVersion {
    fn from(v: temporal::api::deployment::v1::WorkerDeploymentVersion) -> Self {
        Self {
            deployment_name: v.deployment_name,
            build_id: v.build_id,
        }
    }
}
