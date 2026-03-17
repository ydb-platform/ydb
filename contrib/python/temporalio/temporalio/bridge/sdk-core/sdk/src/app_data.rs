use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fmt,
};

/// A Wrapper Type for workflow and activity app data
#[derive(Default)]
pub(crate) struct AppData {
    map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl AppData {
    /// Insert an item, overwritting duplicates
    pub(crate) fn insert<T: Send + Sync + 'static>(&mut self, val: T) -> Option<T> {
        self.map
            .insert(TypeId::of::<T>(), Box::new(val))
            .and_then(downcast_owned)
    }

    /// Get a reference to a type in the map
    pub(crate) fn get<T: 'static>(&self) -> Option<&T> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref())
    }
}

impl fmt::Debug for AppData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppData").finish()
    }
}

fn downcast_owned<T: Send + Sync + 'static>(boxed: Box<dyn Any + Send + Sync>) -> Option<T> {
    boxed.downcast().ok().map(|boxed| *boxed)
}
