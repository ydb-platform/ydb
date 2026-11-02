use std::{cell::RefCell, fmt::Write as _, thread};

use proc_macro2::Span;
use syn::{punctuated::Punctuated, spanned::Spanned, Error, Fields, Ident, Meta};

const ATTR_NAME: &str = "valuable";

// All #[valuable] attributes.
static ATTRS: &[AttrDef] = &[
    // #[valuable(rename = "...")]
    AttrDef {
        name: "rename",
        conflicts_with: &[],
        position: &[
            Position::Struct,
            Position::Enum,
            Position::Variant,
            Position::NamedField,
        ],
        style: &[MetaStyle::NameValue],
    },
    // #[valuable(transparent)]
    AttrDef {
        name: "transparent",
        conflicts_with: &["rename"],
        position: &[
            Position::Struct,
            // TODO: We can probably support single-variant enum that has a single field
            // Position::Enum,
        ],
        style: &[MetaStyle::Ident],
    },
    // #[valuable(skip)]
    AttrDef {
        name: "skip",
        conflicts_with: &["rename"],
        position: &[
            // TODO: How do we implement Enumerable::variant and Valuable::as_value if a variant is skipped?
            // Position::Variant,
            Position::NamedField,
            Position::UnnamedField,
        ],
        style: &[MetaStyle::Ident],
    },
];

pub(crate) struct Attrs {
    rename: Option<(syn::MetaNameValue, syn::LitStr)>,
    transparent: Option<Span>,
    skip: Option<Span>,
}

impl Attrs {
    pub(crate) fn rename(&self, original: &Ident) -> syn::LitStr {
        self.rename.as_ref().map_or_else(
            || syn::LitStr::new(&original.to_string(), original.span()),
            |(_, l)| l.clone(),
        )
    }

    pub(crate) fn transparent(&self) -> bool {
        self.transparent.is_some()
    }

    pub(crate) fn skip(&self) -> bool {
        self.skip.is_some()
    }
}

pub(crate) fn parse_attrs(cx: &Context, attrs: &[syn::Attribute], pos: Position) -> Attrs {
    let mut rename = None;
    let mut transparent = None;
    let mut skip = None;

    let attrs = filter_attrs(cx, attrs, pos);
    for (def, meta) in &attrs {
        macro_rules! lit_str {
            ($field:ident) => {{
                let m = match meta {
                    Meta::NameValue(m) => m,
                    _ => unreachable!(),
                };
                let lit = match &m.value {
                    syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(l),
                        ..
                    }) => l,
                    l => {
                        cx.error(format_err!(l, "expected string literal"));
                        continue;
                    }
                };
                $field = Some((m.clone(), lit.clone()));
            }};
        }

        if def.late_check(cx, &attrs) {
            continue;
        }
        match def.name {
            // #[valuable(rename = "...")]
            "rename" => lit_str!(rename),
            // #[valuable(transparent)]
            "transparent" => transparent = Some(meta.span()),
            // #[valuable(skip)]
            "skip" => skip = Some(meta.span()),

            _ => unreachable!("{}", def.name),
        }
    }

    Attrs {
        rename,
        transparent,
        skip,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Position {
    // TODO: kind: struct, tuple, or unit
    Struct,
    Enum,
    // TODO: kind of variants: struct, tuple, or unit
    Variant,
    NamedField,
    UnnamedField,
}

impl Position {
    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn as_str(&self) -> &'static str {
        match self {
            Position::Struct => "struct",
            Position::Enum => "enum",
            Position::Variant => "variant",
            Position::NamedField => "named field",
            Position::UnnamedField => "unnamed field",
        }
    }

    fn is_field(self) -> bool {
        self == Position::NamedField || self == Position::UnnamedField
    }
}

impl From<&Fields> for Position {
    fn from(meta: &Fields) -> Self {
        match meta {
            Fields::Named(..) => Position::NamedField,
            Fields::Unnamed(..) | Fields::Unit => Position::UnnamedField,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetaStyle {
    // #[attr(<name>)]
    Ident,
    // #[attr(<name> = ...)]
    NameValue,
    // #[attr(<name>(...))]
    List,
}

impl MetaStyle {
    pub(crate) fn format(self, name: &str) -> String {
        match self {
            MetaStyle::Ident => name.to_owned(),
            MetaStyle::List => format!("{}(...)", name),
            MetaStyle::NameValue => format!("{} = ...", name),
        }
    }
}

impl From<&Meta> for MetaStyle {
    fn from(meta: &Meta) -> Self {
        match meta {
            Meta::Path(..) => MetaStyle::Ident,
            Meta::List(..) => MetaStyle::List,
            Meta::NameValue(..) => MetaStyle::NameValue,
        }
    }
}

#[derive(Debug)]
struct AttrDef {
    name: &'static str,
    conflicts_with: &'static [&'static str],
    // allowed positions.
    position: &'static [Position],
    // allowed styles.
    style: &'static [MetaStyle],
}

impl AttrDef {
    /// Performs checks that can be performed without parsing other attributes,
    /// and returns `true` if at least one error occurs.
    fn early_check(&self, cx: &Context, pos: Position, meta: &Meta) -> bool {
        let mut has_error = false;
        if let Err(msg) = self.check_position(pos) {
            cx.error(format_err!(meta, msg));
            has_error = true;
        }
        if let Err(msg) = self.check_style(meta) {
            cx.error(format_err!(meta, msg));
            has_error = true;
        }
        has_error
    }

    fn check_position(&self, pos: Position) -> Result<(), String> {
        if self.position.contains(&pos) {
            return Ok(());
        }
        let mut msg = format!("#[{}({})] may only be used on ", ATTR_NAME, self.name);
        // TODO: simplify if possible
        // len == 1: a
        // len == 2: a and b
        // len > 2: a, b, and c
        let position = if self.position.contains(&Position::NamedField)
            && self.position.contains(&Position::UnnamedField)
        {
            let mut position: Vec<_> = self
                .position
                .iter()
                .filter(|p| !p.is_field())
                .map(Position::as_str)
                .collect();
            position.push("field");
            position
        } else {
            self.position.iter().map(Position::as_str).collect()
        };
        let len = position.len();
        for (i, p) in position.iter().enumerate() {
            if i != 0 {
                if len == 2 {
                    msg.push_str(" and ");
                } else {
                    msg.push_str(", ");
                    if i == len - 1 {
                        msg.push_str("and ");
                    }
                }
            }
            msg.push_str(p);
            msg.push('s');
        }
        Err(msg)
    }

    fn check_style(&self, meta: &Meta) -> Result<(), String> {
        let meta = MetaStyle::from(meta);
        if self.style.contains(&meta) {
            return Ok(());
        }
        let mut msg = "expected ".to_owned();
        let mut first = true;
        for style in self.style {
            if first {
                first = false;
            } else {
                msg.push_str(" or ");
            }
            let _ = write!(msg, "`#[{}({})]`", ATTR_NAME, style.format(self.name));
        }
        msg.push_str(", found ");
        let _ = write!(msg, "`#[{}({})]`", ATTR_NAME, meta.format(self.name));
        Err(msg)
    }

    /// Performs checks that can be performed after parsing all attributes in
    /// the same scope and parent scopes, and returns `true` if at least one
    /// error occurs.
    fn late_check(&self, cx: &Context, attrs: &[(&AttrDef, Meta)]) -> bool {
        let mut has_error = false;
        for (def, meta) in attrs {
            if def.name != self.name && self.conflicts_with.contains(&def.name) {
                let msg = format!(
                    "#[{0}({1})] may not be used together with #[{0}({2})]",
                    ATTR_NAME, self.name, def.name
                );
                cx.error(format_err!(meta.path(), msg));
                has_error = true;
            }
        }
        has_error
    }
}

#[derive(Debug)]
pub(crate) struct Context {
    // - `None`: during checking.
    // - `Some(None)`: there are no errors.
    // - `Some(Some)`: there are errors.
    #[allow(clippy::option_option)]
    error: RefCell<Option<Option<Error>>>,
}

impl Context {
    pub(crate) fn error(&self, e: Error) {
        match self.error.borrow_mut().as_mut().unwrap() {
            Some(base) => base.combine(e),
            error @ None => *error = Some(e),
        }
    }

    pub(crate) fn check(self) -> Result<(), Error> {
        match self.error.borrow_mut().take().unwrap() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl Default for Context {
    fn default() -> Self {
        Self {
            error: RefCell::new(Some(None)),
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        if !thread::panicking() && self.error.borrow().is_some() {
            panic!("context need to be checked");
        }
    }
}

fn filter_attrs<'a>(
    cx: &'a Context,
    attrs: &'a [syn::Attribute],
    pos: Position,
) -> Vec<(&'static AttrDef, Meta)> {
    let mut counter = vec![0; ATTRS.len()];
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident(ATTR_NAME))
        .filter_map(move |attr| match &attr.meta {
            Meta::List(list) => match list
                .parse_args_with(Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated)
            {
                Ok(list) => Some(list),
                Err(e) => {
                    cx.error(e);
                    None
                }
            },
            m => {
                cx.error(format_err!(m, "expected `#[{}(...)]`", ATTR_NAME));
                None
            }
        })
        .flatten()
        .filter_map(move |m| match m.path().get_ident() {
            Some(p) => match ATTRS.iter().position(|a| p == a.name) {
                Some(pos) => {
                    counter[pos] += 1;
                    if counter[pos] == 1 {
                        Some((&ATTRS[pos], m))
                    } else {
                        cx.error(format_err!(
                            &m,
                            "duplicate #[{}({})] attribute",
                            ATTR_NAME,
                            p
                        ));
                        None
                    }
                }
                None => {
                    cx.error(format_err!(p, "unknown {} attribute `{}`", ATTR_NAME, p));
                    None
                }
            },
            None => {
                cx.error(format_err!(m, "expected identifier, found path"));
                None
            }
        })
        .filter(|(def, meta)| !def.early_check(cx, pos, meta))
        .collect()
}
