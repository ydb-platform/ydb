//! Examples of using backon.

#[doc = include_str!("basic.md")]
pub mod basic {}

#[doc = include_str!("closure.md")]
pub mod closure {}

#[doc = include_str!("inside_mut_self.md")]
pub mod inside_mut_self {}

#[doc = include_str!("sqlx.md")]
pub mod sqlx {}

#[doc = include_str!("with_args.md")]
pub mod with_args {}

#[doc = include_str!("with_mut_self.md")]
pub mod with_mut_self {}

#[doc = include_str!("with_self.md")]
pub mod with_self {}

#[doc = include_str!("with_specific_error.md")]
pub mod with_specific_error {}

#[doc = include_str!("retry_after.md")]
pub mod retry_after {}
