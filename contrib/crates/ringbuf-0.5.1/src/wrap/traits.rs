use crate::rb::RbRef;

/// Ring buffer wrapper that contains reference to the ring buffer inside.
pub trait Wrap: AsRef<Self> + AsMut<Self> {
    /// Ring buffer reference type.
    type RbRef: RbRef;

    /// Underlying ring buffer.
    fn rb(&self) -> &<Self::RbRef as RbRef>::Rb {
        self.rb_ref().rb()
    }
    /// Underlying ring buffer reference.
    fn rb_ref(&self) -> &Self::RbRef;
    /// Destructure into underlying ring buffer reference.
    fn into_rb_ref(self) -> Self::RbRef;
}
