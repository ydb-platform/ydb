use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use syn::{parse::Parser, parse_macro_input};

mod activities_definitions;
mod fsm_impl;
mod macro_utils;
mod workflow_definitions;

/// Can be used to define Activities for invocation and execution. Using this macro requires that
/// you also depend on the `temporalio_sdk` crate.
///
/// For a usage example, see that crate's documentation.
#[proc_macro_attribute]
pub fn activities(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let def = parse_macro_input!(item with activities_definitions::parse_activities);
    def.codegen()
}

/// Marks a method within an `#[activities]` impl block as an activity.
/// This attribute is processed by the `#[activities]` macro and should not be used standalone.
#[proc_macro_attribute]
pub fn activity(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Declares activities without providing implementations. Each method must omit the
/// `ActivityContext` parameter and have a body of exactly `unimplemented!()`.
///
/// Intended for workflow crates that need typed activity declarations which are implemented
/// elsewhere by a separate worker crate (or in another language).
#[proc_macro_attribute]
pub fn activity_definitions(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let def = parse_macro_input!(item with activities_definitions::parse_definitions);
    def.codegen()
}

/// Marks a struct as a workflow definition.
///
/// By default, the struct name is used as the workflow type name. To specify a custom workflow
/// name, use `#[run(name = "my-custom-workflow")]` on the run method.
///
/// This attribute must be used in conjunction with `#[workflow_methods]` on an impl block.
#[proc_macro_attribute]
pub fn workflow(attr: TokenStream, item: TokenStream) -> TokenStream {
    match validate_workflow_attributes(attr.into()) {
        Ok(()) => item,
        Err(err) => err.into_compile_error().into(),
    }
}

fn validate_workflow_attributes(attr: TokenStream2) -> syn::Result<()> {
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            Err(meta.error("`name` is not supported on #[workflow]; use #[run(name = ...)] on the workflow run method"))
        } else {
            Err(meta.error("unsupported workflow attribute"))
        }
    });
    parser.parse2(attr)
}

/// Defines workflow methods for a workflow struct. Using this macro requires that
/// you also depend on the `temporalio_sdk` crate.
///
/// This macro processes an impl block and generates:
/// - Marker structs for each workflow method
/// - Trait implementations for workflow definition and execution
/// - Registration code for workers
///
/// ## Macro Attributes
///
/// - `factory_only` - When set, the workflow must be registered using
///   `register_workflow_with_factory` and does not need to implement `Default` or define an `#[init]`
///   method. Ex: `#[workflow_methods(factory_only)]`
///
/// ## Method Attributes
///
/// - `#[init]` - Optional initialization method. Signature: `fn new(input: T, ctx: &WorkflowContext) -> Self`
/// - `#[run]` - Required main workflow function. Signature: `async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<T>`. Supports optional `name`.
/// - `#[signal]` - Signal handler. Sync: `fn signal(&mut self, ctx: &mut SyncWorkflowContext, input: T)`. Async: `async fn signal(ctx: &mut WorkflowContext, input: T)`
/// - `#[query]` - Query handler. Signature: `fn query(&self, ctx: &WorkflowContextView, input: T) -> R` (must NOT be async)
/// - `#[update]` - Update handler. Sync: `fn update(&mut self, ctx: &mut SyncWorkflowContext, input: T) -> R`. Async: `async fn update(ctx: &mut WorkflowContext, input: T) -> R`
///
/// For a usage example, see the `temporalio_sdk` crate's documentation.
#[proc_macro_attribute]
pub fn workflow_methods(attr: TokenStream, item: TokenStream) -> TokenStream {
    let factory_only = !attr.is_empty() && attr.to_string().contains("factory_only");
    let def: workflow_definitions::WorkflowMethodsDefinition =
        parse_macro_input!(item as workflow_definitions::WorkflowMethodsDefinition);
    def.codegen_with_options(factory_only)
}

/// Marks a method within a `#[workflow_methods]` impl block as the initialization method.
/// This attribute is processed by the `#[workflow_methods]` macro and should not be used standalone.
#[proc_macro_attribute]
pub fn init(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks a method within a `#[workflow_methods]` impl block as the main run method.
/// This attribute is processed by the `#[workflow_methods]` macro and should not be used standalone.
#[proc_macro_attribute]
pub fn run(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks a method within a `#[workflow_methods]` impl block as a signal handler.
/// This attribute is processed by the `#[workflow_methods]` macro and should not be used standalone.
///
/// Supports an optional `name` parameter to override the signal name:
/// `#[signal(name = "my_signal")]`
#[proc_macro_attribute]
pub fn signal(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks a method within a `#[workflow_methods]` impl block as a query handler.
/// This attribute is processed by the `#[workflow_methods]` macro and should not be used standalone.
///
/// Supports an optional `name` parameter to override the query name:
/// `#[query(name = "my_query")]`
#[proc_macro_attribute]
pub fn query(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks a method within a `#[workflow_methods]` impl block as an update handler.
/// This attribute is processed by the `#[workflow_methods]` macro and should not be used standalone.
///
/// Supports an optional `name` parameter to override the update name:
/// `#[update(name = "my_update")]`
#[proc_macro_attribute]
pub fn update(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks a method within a `#[workflow_methods]` impl block as a validator for an update handler.
/// This attribute is processed by the `#[workflow_methods]` macro and should not be used standalone.
///
/// The parameter specifies which update this validator applies to:
/// `#[update_validator(my_update)]`
///
/// The validator method must:
/// - Take `&self` (not `&mut self`)
/// - Take `&WorkflowContextView` as the first parameter
/// - Take a reference to the update's input type as the second parameter
/// - Return `Result<(), Box<dyn std::error::Error + Send + Sync>>`
#[proc_macro_attribute]
pub fn update_validator(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Parses a DSL for defining finite state machines, and produces code implementing the
/// [StateMachine](trait.StateMachine.html) trait.
///
/// An example state machine definition of a card reader for unlocking a door:
/// ```
/// use std::convert::Infallible;
/// use temporalio_common::fsm_trait::{StateMachine, TransitionResult};
/// use temporalio_macros::fsm;
///
/// fsm! {
///     name CardReader; command Commands; error Infallible; shared_state SharedState;
///
///     Locked --(CardReadable(CardData), shared on_card_readable) --> ReadingCard;
///     Locked --(CardReadable(CardData), shared on_card_readable) --> Locked;
///     ReadingCard --(CardAccepted, on_card_accepted) --> DoorOpen;
///     ReadingCard --(CardRejected, on_card_rejected) --> Locked;
///     DoorOpen --(DoorClosed, on_door_closed) --> Locked;
/// }
///
/// #[derive(Clone)]
/// pub struct SharedState {
///     last_id: Option<String>,
/// }
///
/// #[derive(Debug, Clone, Eq, PartialEq, Hash)]
/// pub enum Commands {
///     StartBlinkingLight,
///     StopBlinkingLight,
///     ProcessData(CardData),
/// }
///
/// type CardData = String;
///
/// /// Door is locked / idle / we are ready to read
/// #[derive(Debug, Clone, Eq, PartialEq, Hash, Default)]
/// pub struct Locked {}
///
/// /// Actively reading the card
/// #[derive(Debug, Clone, Eq, PartialEq, Hash)]
/// pub struct ReadingCard {
///     card_data: CardData,
/// }
///
/// /// The door is open, we shouldn't be accepting cards and should be blinking the light
/// #[derive(Debug, Clone, Eq, PartialEq, Hash)]
/// pub struct DoorOpen {}
/// impl DoorOpen {
///     fn on_door_closed(&self) -> CardReaderTransition<Locked> {
///         TransitionResult::ok(vec![], Locked {})
///     }
/// }
///
/// impl Locked {
///     fn on_card_readable(
///         &self,
///         shared_dat: &mut SharedState,
///         data: CardData,
///     ) -> CardReaderTransition<ReadingCardOrLocked> {
///         match &shared_dat.last_id {
///             // Arbitrarily deny the same person entering twice in a row
///             Some(d) if d == &data => TransitionResult::ok(vec![], Locked {}.into()),
///             _ => {
///                 // Otherwise issue a processing command. This illustrates using the same handler
///                 // for different destinations
///                 shared_dat.last_id = Some(data.clone());
///                 TransitionResult::ok(
///                     vec![
///                         Commands::ProcessData(data.clone()),
///                         Commands::StartBlinkingLight,
///                     ],
///                     ReadingCard { card_data: data }.into(),
///                 )
///             }
///         }
///     }
/// }
///
/// impl ReadingCard {
///     fn on_card_accepted(&self) -> CardReaderTransition<DoorOpen> {
///         TransitionResult::ok(vec![Commands::StopBlinkingLight], DoorOpen {})
///     }
///     fn on_card_rejected(&self) -> CardReaderTransition<Locked> {
///         TransitionResult::ok(vec![Commands::StopBlinkingLight], Locked {})
///     }
/// }
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let crs = CardReaderState::Locked(Locked {});
/// let mut cr = CardReader::from_parts(crs, SharedState { last_id: None });
/// let cmds = cr.on_event(CardReaderEvents::CardReadable("badguy".to_string()))?;
/// assert_eq!(cmds[0], Commands::ProcessData("badguy".to_string()));
/// assert_eq!(cmds[1], Commands::StartBlinkingLight);
///
/// let cmds = cr.on_event(CardReaderEvents::CardRejected)?;
/// assert_eq!(cmds[0], Commands::StopBlinkingLight);
///
/// let cmds = cr.on_event(CardReaderEvents::CardReadable("goodguy".to_string()))?;
/// assert_eq!(cmds[0], Commands::ProcessData("goodguy".to_string()));
/// assert_eq!(cmds[1], Commands::StartBlinkingLight);
///
/// let cmds = cr.on_event(CardReaderEvents::CardAccepted)?;
/// assert_eq!(cmds[0], Commands::StopBlinkingLight);
/// # Ok(())
/// # }
/// ```
///
/// In the above example the first word is the name of the state machine, then after the comma the
/// type (which you must define separately) of commands produced by the machine.
///
/// then each line represents a transition, where the first word is the initial state, the tuple
/// inside the arrow is `(eventtype[, event handler])`, and the word after the arrow is the
/// destination state. here `eventtype` is an enum variant , and `event_handler` is a function you
/// must define outside the enum whose form depends on the event variant. the only variant types
/// allowed are unit and one-item tuple variants. For unit variants, the function takes no
/// parameters. For the tuple variants, the function takes the variant data as its parameter. In
/// either case the function is expected to return a `TransitionResult` to the appropriate state.
///
/// The first transition can be interpreted as "If the machine is in the locked state, when a
/// `CardReadable` event is seen, call `on_card_readable` (passing in `CardData`) and transition to
/// the `ReadingCard` state.
///
/// The macro will generate a few things:
/// * A struct for the overall state machine, named with the provided name. Here:
///   ```text
///   struct CardReader {
///       state: CardReaderState,
///       shared_state: SharedState,
///   }
///   ```
/// * An enum with a variant for each state, named with the provided name + "State".
///   ```text
///   enum CardReaderState {
///       Locked(Locked),
///       ReadingCard(ReadingCard),
///       DoorOpen(DoorOpen),
///   }
///   ```
///
///   You are expected to define a type for each state, to contain that state's data. If there is
///   no data, you can simply: `type StateName = ()`
/// * For any instance of transitions with the same event/handler which transition to different
///   destination states (dynamic destinations), an enum named like `DestAOrDestBOrDestC` is
///   generated. This enum must be used as the destination "state" from those handlers.
/// * An enum with a variant for each event. You are expected to define the type (if any) contained
///   in the event variant.
///   ```text
///   enum CardReaderEvents {
///     DoorClosed,
///     CardAccepted,
///     CardRejected,
///     CardReadable(CardData),
///   }
///   ```
/// * An implementation of the [StateMachine](trait.StateMachine.html) trait for the generated state
///   machine enum (in this case, `CardReader`)
/// * A type alias for a [TransitionResult](enum.TransitionResult.html) with the appropriate generic
///   parameters set for your machine. It is named as your machine with `Transition` appended. In
///   this case, `CardReaderTransition`.
#[proc_macro]
pub fn fsm(input: TokenStream) -> TokenStream {
    let def: fsm_impl::StateMachineDefinition =
        parse_macro_input!(input as fsm_impl::StateMachineDefinition);
    def.codegen()
}

#[cfg(test)]
mod tests {
    use super::validate_workflow_attributes;
    use quote::quote;

    #[test]
    fn workflow_attribute_rejects_name_override() {
        let err = validate_workflow_attributes(quote!(name = "RenamedWorkflow")).unwrap_err();
        assert!(
            err.to_string()
                .contains("use #[run(name = ...)] on the workflow run method")
        );
    }
}
