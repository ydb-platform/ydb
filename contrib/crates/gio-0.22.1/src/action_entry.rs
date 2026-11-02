// Take a look at the license at the top of the repository in the LICENSE file.

use glib::{Variant, VariantTy, VariantType, prelude::*};

use crate::{ActionMap, SimpleAction};

#[doc(alias = "GActionEntry")]
pub struct ActionEntry<O>
where
    O: IsA<ActionMap>,
{
    name: String,
    parameter_type: Option<VariantType>,
    state: Option<Variant>,
    #[allow(clippy::type_complexity)]
    pub(crate) activate: Option<Box<dyn Fn(&O, &SimpleAction, Option<&Variant>) + 'static>>,
    #[allow(clippy::type_complexity)]
    pub(crate) change_state: Option<Box<dyn Fn(&O, &SimpleAction, Option<&Variant>) + 'static>>,
}

impl<O> ActionEntry<O>
where
    O: IsA<ActionMap>,
{
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn parameter_type(&self) -> Option<&VariantTy> {
        self.parameter_type.as_deref()
    }

    pub fn state(&self) -> Option<&Variant> {
        self.state.as_ref()
    }

    pub fn builder(name: &str) -> ActionEntryBuilder<O> {
        ActionEntryBuilder::new(name)
    }
}

impl<O> std::fmt::Debug for ActionEntry<O>
where
    O: IsA<ActionMap>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActionEntry")
            .field("name", &self.name())
            .field("parameter_type", &self.parameter_type())
            .field("state", &self.state())
            .finish()
    }
}

#[derive(Debug)]
pub struct ActionEntryBuilder<O>(ActionEntry<O>)
where
    O: IsA<ActionMap>;

impl<O> ActionEntryBuilder<O>
where
    O: IsA<ActionMap>,
{
    pub fn new(name: &str) -> Self {
        Self(ActionEntry {
            name: name.to_owned(),
            parameter_type: Default::default(),
            state: Default::default(),
            activate: Default::default(),
            change_state: Default::default(),
        })
    }

    pub fn parameter_type(mut self, parameter_type: Option<&VariantTy>) -> Self {
        self.0.parameter_type = parameter_type.map(|vt| vt.to_owned());
        self
    }

    pub fn state(mut self, state: Variant) -> Self {
        self.0.state = Some(state);
        self
    }

    pub fn activate<F: Fn(&O, &SimpleAction, Option<&Variant>) + 'static>(
        mut self,
        callback: F,
    ) -> Self {
        self.0.activate = Some(Box::new(callback));
        self
    }

    pub fn change_state<F: Fn(&O, &SimpleAction, Option<&Variant>) + 'static>(
        mut self,
        callback: F,
    ) -> Self {
        self.0.change_state = Some(Box::new(callback));
        self
    }

    pub fn build(self) -> ActionEntry<O> {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;

    #[test]
    fn action_entry() {
        let app = crate::Application::new(None, Default::default());

        app.add_action_entries(vec![
            ActionEntry::builder("close")
                .activate(move |_app, _, _| {
                    //Do something
                })
                .build(),
            ActionEntry::builder("enable")
                .state(true.to_variant())
                .change_state(move |_app, _, _| {
                    //Do something
                })
                .build(),
        ]);
        assert!(app.lookup_action("close").is_some());
        assert!(app.lookup_action("enable").is_some());
    }
}
