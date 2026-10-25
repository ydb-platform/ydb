pub use inventory;

pub use typetag;

/// Trait to support serialization and deserialization of `prost` messages.
#[typetag::serde(tag = "@type")]
pub trait MessageSerde: prost::Message + std::any::Any {
    /// message name as in proto file
    fn message_name(&self) -> &'static str;
    /// package name as in proto file
    fn package_name(&self) -> &'static str;
    /// the message proto type url e.g. type.googleapis.com/my.package.MyMessage
    fn type_url(&self) -> &'static str;
    /// Creates a new instance of this message using the protobuf encoded data
    fn new_instance(&self, data: Vec<u8>) -> Result<Box<dyn MessageSerde>, prost::DecodeError>;
    /// Returns the encoded protobuf message as bytes
    fn try_encoded(&self) -> Result<Vec<u8>, prost::EncodeError>;
}

/// The implementation here is a direct copy of the `impl dyn` of [`std::any::Any`]!
impl dyn MessageSerde {
    /// Returns `true` if the inner type is the same as `T`.
    #[inline]
    pub fn is<T: MessageSerde>(&self) -> bool {
        // Get `TypeId` of the type this function is instantiated with.
        let t = std::any::TypeId::of::<T>();

        // Get `TypeId` of the type in the trait object (`self`).
        let concrete = self.type_id();

        // Compare both `TypeId`s on equality.
        t == concrete
    }

    /// Returns some reference to the inner value if it is of type `T`, or
    /// `None` if it isn't.
    #[inline]
    pub fn downcast_ref<T: MessageSerde>(&self) -> Option<&T> {
        if self.is::<T>() {
            // SAFETY: just checked whether we are pointing to the correct type, and we can rely on
            // that check for memory safety because we have implemented Any for all types; no other
            // impls can exist as they would conflict with our impl.
            unsafe { Some(self.downcast_ref_unchecked()) }
        } else {
            Option::None
        }
    }

    /// Returns some mutable reference to the boxed value if it is of type `T`,
    /// or `None` if it isn't.
    #[inline]
    pub fn downcast_mut<T: MessageSerde>(&mut self) -> Option<&mut T> {
        if self.is::<T>() {
            // SAFETY: just checked whether we are pointing to the correct type, and we can rely on
            // that check for memory safety because we have implemented Any for all types; no other
            // impls can exist as they would conflict with our impl.
            unsafe { Some(self.downcast_mut_unchecked()) }
        } else {
            Option::None
        }
    }

    /// Returns a reference to the inner value as type `dyn T`.
    ///
    /// # Safety
    ///
    /// The contained value must be of type `T`. Calling this method
    /// with the incorrect type is *undefined behavior*.
    #[inline]
    pub unsafe fn downcast_ref_unchecked<T: MessageSerde>(&self) -> &T {
        debug_assert!(self.is::<T>());
        // SAFETY: caller guarantees that T is the correct type
        unsafe { &*(self as *const dyn MessageSerde as *const T) }
    }

    /// Returns a mutable reference to the inner value as type `dyn T`.
    ///
    /// # Safety
    ///
    /// The contained value must be of type `T`. Calling this method
    /// with the incorrect type is *undefined behavior*.
    #[inline]
    pub unsafe fn downcast_mut_unchecked<T: MessageSerde>(&mut self) -> &mut T {
        &mut *(self as *mut Self as *mut T)
    }
}

type MessageSerdeDecoderFn = fn(&[u8]) -> Result<Box<dyn MessageSerde>, ::prost::DecodeError>;

pub struct MessageSerdeDecoderEntry {
    pub type_url: &'static str,
    pub decoder: MessageSerdeDecoderFn,
}

inventory::collect!(MessageSerdeDecoderEntry);
