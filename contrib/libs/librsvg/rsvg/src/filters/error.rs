use std::fmt;

use crate::error::{InternalRenderingError, InvalidTransform};

/// An enumeration of errors that can occur during filter primitive rendering.
#[derive(Clone)]
pub enum FilterError {
    /// The filter was passed invalid input (the `in` attribute).
    InvalidInput,
    /// The filter was passed an invalid parameter.
    InvalidParameter(String),
    /// The filter input surface has an unsuccessful status.
    BadInputSurfaceStatus(cairo::Error),
    /// A Cairo error.
    ///
    /// This means that either a failed intermediate surface creation or bad intermediate surface
    /// status.
    CairoError(cairo::Error),
    /// Error from the rendering backend.
    Rendering(InternalRenderingError),
    /// A lighting filter input surface is too small.
    LightingInputTooSmall,
}

/// Errors that can occur while resolving a `FilterSpec`.
#[derive(Debug)]
pub enum FilterResolveError {
    /// An `uri(#foo)` reference does not point to a `<filter>` element.
    ReferenceToNonFilterElement,
    /// A lighting filter has none or multiple light sources.
    InvalidLightSourceCount,
    /// Child node was in error.
    ChildNodeInError,
}

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            FilterError::InvalidInput => write!(f, "invalid value of the `in` attribute"),
            FilterError::InvalidParameter(ref s) => write!(f, "invalid parameter value: {s}"),
            FilterError::BadInputSurfaceStatus(ref status) => {
                write!(f, "invalid status of the input surface: {status}")
            }
            FilterError::CairoError(ref status) => write!(f, "Cairo error: {status}"),
            FilterError::Rendering(ref e) => write!(f, "Rendering error: {e}"),
            FilterError::LightingInputTooSmall => write!(
                f,
                "lighting filter input surface is too small (less than 2×2 pixels)"
            ),
        }
    }
}

impl fmt::Display for FilterResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            FilterResolveError::ReferenceToNonFilterElement => {
                write!(f, "reference to a non-filter element")
            }
            FilterResolveError::InvalidLightSourceCount => write!(f, "invalid light source count"),
            FilterResolveError::ChildNodeInError => write!(f, "child node was in error"),
        }
    }
}

impl From<cairo::Error> for FilterError {
    #[inline]
    fn from(x: cairo::Error) -> Self {
        FilterError::CairoError(x)
    }
}

impl From<InternalRenderingError> for FilterError {
    #[inline]
    fn from(e: InternalRenderingError) -> Self {
        FilterError::Rendering(e)
    }
}

impl From<Box<InternalRenderingError>> for FilterError {
    #[inline]
    fn from(e: Box<InternalRenderingError>) -> Self {
        FilterError::Rendering(*e)
    }
}

impl From<InvalidTransform> for FilterError {
    fn from(_: InvalidTransform) -> Self {
        FilterError::Rendering(InternalRenderingError::InvalidTransform)
    }
}
