//! The `pattern` element.

use markup5ever::{expanded_name, local_name, ns};

use crate::coord_units;
use crate::coord_units::CoordUnits;

use crate::aspect_ratio::*;
use crate::document::{AcquiredNode, AcquiredNodes, NodeId, NodeStack};
use crate::drawing_ctx::Viewport;
use crate::element::{ElementData, ElementTrait, set_attribute};
use crate::error::*;
use crate::href::{is_href, set_href};
use crate::length::*;
use crate::node::{Node, NodeBorrow, WeakNode};
use crate::parsers::ParseValue;
use crate::rect::Rect;
use crate::rsvg_log;
use crate::session::Session;
use crate::transform::{Transform, TransformAttribute};
use crate::unit_interval::UnitInterval;
use crate::viewbox::*;
use crate::xml::Attributes;

coord_units!(PatternUnits, CoordUnits::ObjectBoundingBox);
coord_units!(PatternContentUnits, CoordUnits::UserSpaceOnUse);

#[derive(Clone, Default)]
struct Common {
    units: Option<PatternUnits>,
    content_units: Option<PatternContentUnits>,
    // This Option<Option<ViewBox>> is a bit strange.  We want a field
    // with value None to mean, "this field isn't resolved yet".  However,
    // the vbox can very well be *not* specified in the SVG file.
    // In that case, the fully resolved pattern will have a .vbox=Some(None) value.
    vbox: Option<Option<ViewBox>>,
    preserve_aspect_ratio: Option<AspectRatio>,
    transform: Option<TransformAttribute>,
    x: Option<Length<Horizontal>>,
    y: Option<Length<Vertical>>,
    width: Option<ULength<Horizontal>>,
    height: Option<ULength<Vertical>>,
}

/// State used during the pattern resolution process
///
/// This is the current node's pattern information, plus the fallback
/// that should be used in case that information is not complete for a
/// resolved pattern yet.
struct Unresolved {
    pattern: UnresolvedPattern,
    fallback: Option<NodeId>,
}

/// Keeps track of which Pattern provided a non-empty set of children during pattern resolution
#[derive(Clone)]
enum UnresolvedChildren {
    /// Points back to the original Pattern if it had no usable children
    Unresolved,

    /// Points back to the original Pattern, as no pattern in the
    /// chain of fallbacks had usable children.  This only gets returned
    /// by resolve_from_defaults().
    ResolvedEmpty,

    /// Points back to the Pattern that had usable children.
    WithChildren(WeakNode),
}

/// Keeps track of which Pattern provided a non-empty set of children during pattern resolution
#[derive(Clone)]
enum Children {
    Empty,

    /// Points back to the Pattern that had usable children
    WithChildren(WeakNode),
}

/// Main structure used during pattern resolution.  For unresolved
/// patterns, we store all fields as `Option<T>` - if `None`, it means
/// that the field is not specified; if `Some(T)`, it means that the
/// field was specified.
struct UnresolvedPattern {
    common: Common,

    // Point back to our corresponding node, or to the fallback node which has children.
    // If the value is None, it means we are fully resolved and didn't find any children
    // among the fallbacks.
    children: UnresolvedChildren,
}

#[derive(Clone)]
pub struct ResolvedPattern {
    units: PatternUnits,
    content_units: PatternContentUnits,
    vbox: Option<ViewBox>,
    preserve_aspect_ratio: AspectRatio,
    transform: TransformAttribute,
    x: Length<Horizontal>,
    y: Length<Vertical>,
    width: ULength<Horizontal>,
    height: ULength<Vertical>,
    opacity: UnitInterval,

    // Link to the node whose children are the pattern's resolved children.
    children: Children,
}

/// Pattern normalized to user-space units.
pub struct UserSpacePattern {
    pub width: f64,
    pub height: f64,
    pub transform: Transform,
    pub coord_transform: Transform,
    pub content_transform: Transform,
    pub opacity: UnitInterval,

    // This one is private so the caller has to go through fn acquire_pattern_node()
    node_with_children: Node,
}

#[derive(Default)]
pub struct Pattern {
    common: Common,
    fallback: Option<NodeId>,
}

impl ElementTrait for Pattern {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "patternUnits") => {
                    set_attribute(&mut self.common.units, attr.parse(value), session)
                }
                expanded_name!("", "patternContentUnits") => {
                    set_attribute(&mut self.common.content_units, attr.parse(value), session);
                }
                expanded_name!("", "viewBox") => {
                    set_attribute(&mut self.common.vbox, attr.parse(value), session)
                }
                expanded_name!("", "preserveAspectRatio") => {
                    set_attribute(
                        &mut self.common.preserve_aspect_ratio,
                        attr.parse(value),
                        session,
                    );
                }
                expanded_name!("", "patternTransform") => {
                    set_attribute(&mut self.common.transform, attr.parse(value), session);
                }
                ref a if is_href(a) => {
                    let mut href = None;
                    set_attribute(
                        &mut href,
                        NodeId::parse(value).map(Some).attribute(attr.clone()),
                        session,
                    );
                    set_href(a, &mut self.fallback, href);
                }
                expanded_name!("", "x") => {
                    set_attribute(&mut self.common.x, attr.parse(value), session)
                }
                expanded_name!("", "y") => {
                    set_attribute(&mut self.common.y, attr.parse(value), session)
                }
                expanded_name!("", "width") => {
                    set_attribute(&mut self.common.width, attr.parse(value), session)
                }
                expanded_name!("", "height") => {
                    set_attribute(&mut self.common.height, attr.parse(value), session)
                }
                _ => (),
            }
        }
    }
}

impl UnresolvedPattern {
    fn into_resolved(self, opacity: UnitInterval) -> ResolvedPattern {
        assert!(self.is_resolved());

        ResolvedPattern {
            units: self.common.units.unwrap(),
            content_units: self.common.content_units.unwrap(),
            vbox: self.common.vbox.unwrap(),
            preserve_aspect_ratio: self.common.preserve_aspect_ratio.unwrap(),
            transform: self.common.transform.unwrap(),
            x: self.common.x.unwrap(),
            y: self.common.y.unwrap(),
            width: self.common.width.unwrap(),
            height: self.common.height.unwrap(),
            opacity,

            children: self.children.to_resolved(),
        }
    }

    fn is_resolved(&self) -> bool {
        self.common.units.is_some()
            && self.common.content_units.is_some()
            && self.common.vbox.is_some()
            && self.common.preserve_aspect_ratio.is_some()
            && self.common.transform.is_some()
            && self.common.x.is_some()
            && self.common.y.is_some()
            && self.common.width.is_some()
            && self.common.height.is_some()
            && self.children.is_resolved()
    }

    fn resolve_from_fallback(&self, fallback: &UnresolvedPattern) -> UnresolvedPattern {
        let units = self.common.units.or(fallback.common.units);
        let content_units = self.common.content_units.or(fallback.common.content_units);
        let vbox = self.common.vbox.or(fallback.common.vbox);
        let preserve_aspect_ratio = self
            .common
            .preserve_aspect_ratio
            .or(fallback.common.preserve_aspect_ratio);
        let transform = self.common.transform.or(fallback.common.transform);
        let x = self.common.x.or(fallback.common.x);
        let y = self.common.y.or(fallback.common.y);
        let width = self.common.width.or(fallback.common.width);
        let height = self.common.height.or(fallback.common.height);
        let children = self.children.resolve_from_fallback(&fallback.children);

        UnresolvedPattern {
            common: Common {
                units,
                content_units,
                vbox,
                preserve_aspect_ratio,
                transform,
                x,
                y,
                width,
                height,
            },
            children,
        }
    }

    fn resolve_from_defaults(&self) -> UnresolvedPattern {
        let units = self.common.units.or_else(|| Some(PatternUnits::default()));
        let content_units = self
            .common
            .content_units
            .or_else(|| Some(PatternContentUnits::default()));
        let vbox = self.common.vbox.or(Some(None));
        let preserve_aspect_ratio = self
            .common
            .preserve_aspect_ratio
            .or_else(|| Some(AspectRatio::default()));
        let transform = self
            .common
            .transform
            .or_else(|| Some(TransformAttribute::default()));
        let x = self.common.x.or_else(|| Some(Default::default()));
        let y = self.common.y.or_else(|| Some(Default::default()));
        let width = self.common.width.or_else(|| Some(Default::default()));
        let height = self.common.height.or_else(|| Some(Default::default()));
        let children = self.children.resolve_from_defaults();

        UnresolvedPattern {
            common: Common {
                units,
                content_units,
                vbox,
                preserve_aspect_ratio,
                transform,
                x,
                y,
                width,
                height,
            },
            children,
        }
    }
}

impl UnresolvedChildren {
    fn from_node(node: &Node) -> UnresolvedChildren {
        let weak = node.downgrade();

        if node.children().any(|child| child.is_element()) {
            UnresolvedChildren::WithChildren(weak)
        } else {
            UnresolvedChildren::Unresolved
        }
    }

    fn is_resolved(&self) -> bool {
        !matches!(*self, UnresolvedChildren::Unresolved)
    }

    fn resolve_from_fallback(&self, fallback: &UnresolvedChildren) -> UnresolvedChildren {
        use UnresolvedChildren::*;

        match (self, fallback) {
            (&Unresolved, &Unresolved) => Unresolved,
            (WithChildren(wc), _) => WithChildren(wc.clone()),
            (_, WithChildren(wc)) => WithChildren(wc.clone()),
            (_, _) => unreachable!(),
        }
    }

    fn resolve_from_defaults(&self) -> UnresolvedChildren {
        use UnresolvedChildren::*;

        match *self {
            Unresolved => ResolvedEmpty,
            _ => (*self).clone(),
        }
    }

    fn to_resolved(&self) -> Children {
        use UnresolvedChildren::*;

        assert!(self.is_resolved());

        match *self {
            ResolvedEmpty => Children::Empty,
            WithChildren(ref wc) => Children::WithChildren(wc.clone()),
            _ => unreachable!(),
        }
    }
}

fn nonempty_rect(bbox: &Option<Rect>) -> Option<Rect> {
    match *bbox {
        None => None,
        Some(r) if r.is_empty() => None,
        Some(r) => Some(r),
    }
}

impl ResolvedPattern {
    fn node_with_children(&self) -> Option<Node> {
        match self.children {
            // This means we didn't find any children among the fallbacks,
            // so there is nothing to render.
            Children::Empty => None,

            Children::WithChildren(ref wc) => Some(wc.upgrade().unwrap()),
        }
    }

    fn get_rect(&self, params: &NormalizeParams) -> Rect {
        let x = self.x.to_user(params);
        let y = self.y.to_user(params);
        let w = self.width.to_user(params);
        let h = self.height.to_user(params);

        Rect::new(x, y, x + w, y + h)
    }

    pub fn to_user_space(
        &self,
        object_bbox: &Option<Rect>,
        viewport: &Viewport,
        values: &NormalizeValues,
    ) -> Option<UserSpacePattern> {
        let node_with_children = self.node_with_children()?;

        let viewport = viewport.with_units(self.units.0);
        let params = NormalizeParams::from_values(values, &viewport);

        let rect = self.get_rect(&params);

        // Create the pattern coordinate system
        let (width, height, coord_transform) = match self.units {
            PatternUnits(CoordUnits::ObjectBoundingBox) => {
                let bbrect = nonempty_rect(object_bbox)?;
                (
                    rect.width() * bbrect.width(),
                    rect.height() * bbrect.height(),
                    Transform::new_translate(
                        bbrect.x0 + rect.x0 * bbrect.width(),
                        bbrect.y0 + rect.y0 * bbrect.height(),
                    ),
                )
            }
            PatternUnits(CoordUnits::UserSpaceOnUse) => (
                rect.width(),
                rect.height(),
                Transform::new_translate(rect.x0, rect.y0),
            ),
        };

        let pattern_transform = self.transform.to_transform();

        let coord_transform = coord_transform.post_transform(&pattern_transform);

        // Create the pattern contents coordinate system
        let content_transform = if let Some(vbox) = self.vbox {
            // If there is a vbox, use that
            let r = self
                .preserve_aspect_ratio
                .compute(&vbox, &Rect::from_size(width, height));

            let sw = r.width() / vbox.width();
            let sh = r.height() / vbox.height();
            let x = r.x0 - vbox.x0 * sw;
            let y = r.y0 - vbox.y0 * sh;

            Transform::new_scale(sw, sh).pre_translate(x, y)
        } else {
            match self.content_units {
                PatternContentUnits(CoordUnits::ObjectBoundingBox) => {
                    let bbrect = nonempty_rect(object_bbox)?;
                    Transform::new_scale(bbrect.width(), bbrect.height())
                }
                PatternContentUnits(CoordUnits::UserSpaceOnUse) => Transform::identity(),
            }
        };

        Some(UserSpacePattern {
            width,
            height,
            transform: pattern_transform,
            coord_transform,
            content_transform,
            opacity: self.opacity,
            node_with_children,
        })
    }
}

impl UserSpacePattern {
    /// Gets the `<pattern>` node that contains the children to be drawn for the pattern's contents.
    ///
    /// This has to go through [AcquiredNodes] to catch circular references among
    /// patterns and their children.
    pub fn acquire_pattern_node(
        &self,
        acquired_nodes: &mut AcquiredNodes<'_>,
    ) -> Result<AcquiredNode, AcquireError> {
        acquired_nodes.acquire_ref(&self.node_with_children)
    }
}

impl Pattern {
    fn get_unresolved(&self, node: &Node) -> Unresolved {
        let pattern = UnresolvedPattern {
            common: self.common.clone(),
            children: UnresolvedChildren::from_node(node),
        };

        Unresolved {
            pattern,
            fallback: self.fallback.clone(),
        }
    }

    pub fn resolve(
        &self,
        node: &Node,
        acquired_nodes: &mut AcquiredNodes<'_>,
        opacity: UnitInterval,
        session: &Session,
    ) -> Result<ResolvedPattern, AcquireError> {
        let Unresolved {
            mut pattern,
            mut fallback,
        } = self.get_unresolved(node);

        let mut stack = NodeStack::new();

        while !pattern.is_resolved() {
            if let Some(ref node_id) = fallback {
                match acquired_nodes.acquire(node_id) {
                    Ok(acquired) => {
                        let acquired_node = acquired.get();

                        if stack.contains(acquired_node) {
                            return Err(AcquireError::CircularReference(acquired_node.clone()));
                        }

                        match *acquired_node.borrow_element_data() {
                            ElementData::Pattern(ref p) => {
                                let unresolved = p.get_unresolved(acquired_node);
                                pattern = pattern.resolve_from_fallback(&unresolved.pattern);
                                fallback = unresolved.fallback;

                                stack.push(acquired_node);
                            }
                            _ => return Err(AcquireError::InvalidLinkType(node_id.clone())),
                        }
                    }

                    Err(AcquireError::MaxReferencesExceeded) => {
                        return Err(AcquireError::MaxReferencesExceeded);
                    }

                    Err(e) => {
                        rsvg_log!(session, "Stopping pattern resolution: {}", e);
                        pattern = pattern.resolve_from_defaults();
                        break;
                    }
                }
            } else {
                pattern = pattern.resolve_from_defaults();
                break;
            }
        }

        Ok(pattern.into_resolved(opacity))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use markup5ever::{QualName, ns};

    use crate::borrow_element_as;
    use crate::node::NodeData;

    #[test]
    fn pattern_resolved_from_defaults_is_really_resolved() {
        let node = Node::new(NodeData::new_element(
            &Session::default(),
            &QualName::new(None, ns!(svg), local_name!("pattern")),
            Attributes::new(),
        ));

        let unresolved = borrow_element_as!(node, Pattern).get_unresolved(&node);
        let pattern = unresolved.pattern.resolve_from_defaults();
        assert!(pattern.is_resolved());
    }
}
