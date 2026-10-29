//! Bounding boxes that know their coordinate space.

use crate::rect::Rect;
use crate::transform::Transform;

#[derive(Debug, Default, Copy, Clone)]
pub struct BoundingBox {
    transform: Transform,
    pub rect: Option<Rect>,     // without stroke
    pub ink_rect: Option<Rect>, // with stroke
}

impl BoundingBox {
    pub fn new() -> BoundingBox {
        Default::default()
    }

    pub fn with_transform(self, transform: Transform) -> BoundingBox {
        BoundingBox { transform, ..self }
    }

    pub fn with_rect(self, rect: Rect) -> BoundingBox {
        BoundingBox {
            rect: Some(rect),
            ..self
        }
    }

    pub fn with_ink_rect(self, ink_rect: Rect) -> BoundingBox {
        BoundingBox {
            ink_rect: Some(ink_rect),
            ..self
        }
    }

    fn combine(&mut self, src: &BoundingBox, clip: bool) {
        if src.rect.is_none() && src.ink_rect.is_none() {
            return;
        }

        // this will panic!() if it's not invertible... should we check on our own?
        let transform = self
            .transform
            .invert()
            .unwrap()
            .pre_transform(&src.transform);

        self.rect = combine_rects(self.rect, src.rect, &transform, clip);
        self.ink_rect = combine_rects(self.ink_rect, src.ink_rect, &transform, clip);
    }

    pub fn insert(&mut self, src: &BoundingBox) {
        self.combine(src, false);
    }

    pub fn clip(&mut self, src: &BoundingBox) {
        self.combine(src, true);
    }
}

fn combine_rects(
    r1: Option<Rect>,
    r2: Option<Rect>,
    transform: &Transform,
    clip: bool,
) -> Option<Rect> {
    match (r1, r2, clip) {
        (r1, None, _) => r1,
        (None, Some(r2), _) => Some(transform.transform_rect(&r2)),
        (Some(r1), Some(r2), true) => transform
            .transform_rect(&r2)
            .intersection(&r1)
            .or_else(|| Some(Rect::default())),
        (Some(r1), Some(r2), false) => Some(transform.transform_rect(&r2).union(&r1)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combine() {
        let r1 = Rect::new(1.0, 2.0, 3.0, 4.0);
        let r2 = Rect::new(1.5, 2.5, 3.5, 4.5);
        let r3 = Rect::new(10.0, 11.0, 12.0, 13.0);
        let t = Transform::new_unchecked(1.0, 0.0, 0.0, 1.0, 0.5, 0.5);

        let res = combine_rects(None, None, &t, true);
        assert_eq!(res, None);

        let res = combine_rects(None, None, &t, false);
        assert_eq!(res, None);

        let res = combine_rects(Some(r1), None, &t, true);
        assert_eq!(res, Some(r1));

        let res = combine_rects(Some(r1), None, &t, false);
        assert_eq!(res, Some(r1));

        let res = combine_rects(None, Some(r2), &t, true);
        assert_eq!(res, Some(Rect::new(2.0, 3.0, 4.0, 5.0)));

        let res = combine_rects(None, Some(r2), &t, false);
        assert_eq!(res, Some(Rect::new(2.0, 3.0, 4.0, 5.0)));

        let res = combine_rects(Some(r1), Some(r2), &t, true);
        assert_eq!(res, Some(Rect::new(2.0, 3.0, 3.0, 4.0)));

        let res = combine_rects(Some(r1), Some(r3), &t, true);
        assert_eq!(res, Some(Rect::default()));

        let res = combine_rects(Some(r1), Some(r2), &t, false);
        assert_eq!(res, Some(Rect::new(1.0, 2.0, 4.0, 5.0)));
    }
}
