//! Resolution for rendering (dots per inch = DPI).

#[derive(Debug, Copy, Clone)]
pub struct Dpi {
    pub x: f64,
    pub y: f64,
}

impl Dpi {
    pub fn new(x: f64, y: f64) -> Dpi {
        Dpi { x, y }
    }
}
