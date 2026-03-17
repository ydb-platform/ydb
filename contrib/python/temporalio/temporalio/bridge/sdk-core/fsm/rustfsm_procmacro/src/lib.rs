use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use std::collections::{HashMap, HashSet, hash_map::Entry};
use syn::{
    Error, Fields, Ident, Token, Type, Variant, Visibility, parenthesized,
    parse::{Parse, ParseStream, Result},
    parse_macro_input,
    spanned::Spanned,
};

/// Parses a DSL for defining finite state machines, and produces code implementing the
/// [StateMachine](trait.StateMachine.html) trait.
///
/// An example state machine definition of a card reader for unlocking a door:
/// ```
/// # extern crate rustfsm_trait as rustfsm;
/// use rustfsm_procmacro::fsm;
/// use std::convert::Infallible;
/// use rustfsm_trait::{StateMachine, TransitionResult};
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
///     last_id: Option<String>
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
///     fn on_card_readable(&self, shared_dat: &mut SharedState, data: CardData)
///       -> CardReaderTransition<ReadingCardOrLocked> {
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
    let def: StateMachineDefinition = parse_macro_input!(input as StateMachineDefinition);
    def.codegen()
}

mod kw {
    syn::custom_keyword!(name);
    syn::custom_keyword!(command);
    syn::custom_keyword!(error);
    syn::custom_keyword!(shared);
    syn::custom_keyword!(shared_state);
}

struct StateMachineDefinition {
    visibility: Visibility,
    name: Ident,
    shared_state_type: Option<Type>,
    command_type: Ident,
    error_type: Ident,
    transitions: Vec<Transition>,
}

impl StateMachineDefinition {
    fn is_final_state(&self, state: &Ident) -> bool {
        // If no transitions go from this state, it's a final state.
        !self.transitions.iter().any(|t| t.from == *state)
    }
}

impl Parse for StateMachineDefinition {
    fn parse(input: ParseStream) -> Result<Self> {
        // Parse visibility if present
        let visibility = input.parse()?;
        // parse the state machine name, command type, and error type
        let (name, command_type, error_type, shared_state_type) = parse_machine_types(input)
            .map_err(|mut e| {
                e.combine(Error::new(
                    e.span(),
                    "The fsm definition should begin with `name MachineName; command CommandType; \
                    error ErrorType;` optionally followed by `shared_state SharedStateType;`",
                ));
                e
            })?;
        // Then the state machine definition is simply a sequence of transitions separated by
        // semicolons
        let transitions = input.parse_terminated(Transition::parse, Token![;])?;
        let transitions: Vec<_> = transitions.into_iter().collect();
        // Check for and whine about any identical transitions. We do this here because preserving
        // the order transitions were defined in is important, so simply collecting to a set is
        // not ideal.
        let trans_set: HashSet<_> = transitions.iter().collect();
        if trans_set.len() != transitions.len() {
            return Err(Error::new(
                input.span(),
                "Duplicate transitions are not allowed!",
            ));
        }
        Ok(Self {
            visibility,
            name,
            shared_state_type,
            command_type,
            error_type,
            transitions,
        })
    }
}

fn parse_machine_types(input: ParseStream) -> Result<(Ident, Ident, Ident, Option<Type>)> {
    let _: kw::name = input.parse()?;
    let name: Ident = input.parse()?;
    input.parse::<Token![;]>()?;

    let _: kw::command = input.parse()?;
    let command_type: Ident = input.parse()?;
    input.parse::<Token![;]>()?;

    let _: kw::error = input.parse()?;
    let error_type: Ident = input.parse()?;
    input.parse::<Token![;]>()?;

    let shared_state_type: Option<Type> = if input.peek(kw::shared_state) {
        let _: kw::shared_state = input.parse()?;
        let typep = input.parse()?;
        input.parse::<Token![;]>()?;
        Some(typep)
    } else {
        None
    };
    Ok((name, command_type, error_type, shared_state_type))
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct Transition {
    from: Ident,
    to: Vec<Ident>,
    event: Variant,
    handler: Option<Ident>,
    mutates_shared: bool,
}

impl Parse for Transition {
    fn parse(input: ParseStream) -> Result<Self> {
        // Parse the initial state name
        let from: Ident = input.parse()?;
        // Parse at least one dash
        input.parse::<Token![-]>()?;
        while input.peek(Token![-]) {
            input.parse::<Token![-]>()?;
        }
        // Parse transition information inside parens
        let transition_info;
        parenthesized!(transition_info in input);
        // Get the event variant definition
        let event: Variant = transition_info.parse()?;
        // Reject non-unit or single-item-tuple variants
        match &event.fields {
            Fields::Named(_) => {
                return Err(Error::new(
                    event.span(),
                    "Struct variants are not supported for events",
                ));
            }
            Fields::Unnamed(uf) => {
                if uf.unnamed.len() != 1 {
                    return Err(Error::new(
                        event.span(),
                        "Only tuple variants with exactly one item are supported for events",
                    ));
                }
            }
            Fields::Unit => {}
        }
        // Check if there is an event handler, and parse it
        let (mutates_shared, handler) = if transition_info.peek(Token![,]) {
            transition_info.parse::<Token![,]>()?;
            // Check for mut keyword signifying handler wants to mutate shared state
            let mutates = if transition_info.peek(kw::shared) {
                transition_info.parse::<kw::shared>()?;
                true
            } else {
                false
            };
            (mutates, Some(transition_info.parse()?))
        } else {
            (false, None)
        };
        // Parse at least one dash followed by the "arrow"
        input.parse::<Token![-]>()?;
        while input.peek(Token![-]) {
            input.parse::<Token![-]>()?;
        }
        input.parse::<Token![>]>()?;
        // Parse the destination state
        let to: Ident = input.parse()?;

        Ok(Self {
            from,
            event,
            handler,
            to: vec![to],
            mutates_shared,
        })
    }
}

impl StateMachineDefinition {
    fn codegen(&self) -> TokenStream {
        let visibility = self.visibility.clone();
        // First extract all of the states into a set, and build the enum's insides
        let states = self.all_states();
        let state_variants = states.iter().map(|s| {
            let statestr = s.to_string();
            quote! {
                #[display(#statestr)]
                #s(#s)
            }
        });
        let name = &self.name;
        let name_str = &self.name.to_string();

        let transition_result_name = Ident::new(&format!("{name}Transition"), name.span());
        let transition_type_alias = quote! {
            type #transition_result_name<Ds, Sm = #name> = TransitionResult<Sm, Ds>;
        };

        let state_enum_name = Ident::new(&format!("{name}State"), name.span());
        // If user has not defined any shared state, use the unit type.
        let shared_state_type = self
            .shared_state_type
            .clone()
            .unwrap_or_else(|| syn::parse_str("()").unwrap());
        let machine_struct = quote! {
            #[derive(Clone)]
            #visibility struct #name {
                state: ::core::option::Option<#state_enum_name>,
                shared_state: #shared_state_type
            }
        };
        let states_enum = quote! {
            #[derive(::derive_more::From, Clone, ::derive_more::Display)]
            #visibility enum #state_enum_name {
                #(#state_variants),*
            }
        };
        let state_is_final_match_arms = states.iter().map(|s| {
            let val = if self.is_final_state(s) {
                quote! { true }
            } else {
                quote! { false }
            };
            quote! { #state_enum_name::#s(_) => #val }
        });
        let states_enum_impl = quote! {
            impl #state_enum_name {
                fn is_final(&self) -> bool {
                    match self {
                        #(#state_is_final_match_arms),*
                    }
                }
            }
        };

        // Build the events enum
        let events: HashSet<Variant> = self.transitions.iter().map(|t| t.event.clone()).collect();
        let events_enum_name = Ident::new(&format!("{name}Events"), name.span());
        let events: Vec<_> = events
            .into_iter()
            .map(|v| {
                let vname = v.ident.to_string();
                quote! {
                    #[display(#vname)]
                    #v
                }
            })
            .collect();
        let events_enum = quote! {
            #[derive(::derive_more::Display)]
            #visibility enum #events_enum_name {
                #(#events),*
            }
        };

        // Construct the trait implementation
        let cmd_type = &self.command_type;
        let err_type = &self.error_type;
        let mut statemap: HashMap<Ident, Vec<Transition>> = HashMap::new();
        for t in &self.transitions {
            statemap
                .entry(t.from.clone())
                .and_modify(|v| v.push(t.clone()))
                .or_insert_with(|| vec![t.clone()]);
        }
        // Add any states without any transitions to the map
        for s in &states {
            if !statemap.contains_key(s) {
                statemap.insert(s.clone(), vec![]);
            }
        }
        let transition_result_transform = quote! {
            match res.into_cmd_result() {
                Ok((cmds, state)) => {
                    self.state = Some(state);
                    Ok(cmds)
                }
                Err(e) => Err(e)
            }
        };
        let mut multi_dest_enums = vec![];
        let mut multi_dest_enum_names = HashSet::new();
        let state_branches: Vec<_> = statemap.into_iter().map(|(from, transitions)| {
            let occupied_current_state = quote! { Some(#state_enum_name::#from(state_data)) };
            // Merge transition dest states with the same handler
            let transitions = merge_transition_dests(transitions);
            let event_branches = transitions
                .into_iter()
                .map(|ts| {
                    let ev_variant = &ts.event.ident;
                    if let Some(ts_fn) = ts.handler.clone() {
                        let span = ts_fn.span();
                        let trans_type = match ts.to.as_slice() {
                            [] => unreachable!("There will be at least one dest state in transitions"),
                            [one_to] => quote! {
                                            #transition_result_name<#one_to>
                                        },
                            multi_dests => {
                                let string_dests: Vec<_> = multi_dests.iter()
                                    .map(ToString::to_string).collect();
                                let enum_ident = Ident::new(&string_dests.join("Or"),
                                                            multi_dests[0].span());
                                let multi_dest_enum = quote! {
                                    #[derive(::derive_more::From)]
                                    #visibility enum #enum_ident {
                                        #(#multi_dests(#multi_dests)),*
                                    }
                                    impl ::core::convert::From<#enum_ident> for #state_enum_name {
                                        fn from(v: #enum_ident) -> Self {
                                            match v {
                                                #( #enum_ident::#multi_dests(sv) =>
                                                    Self::#multi_dests(sv) ),*
                                            }
                                        }
                                    }
                                };
                                // Deduplicate; two different events may each result in a transition
                                // set with the same set of dest states
                                if multi_dest_enum_names.insert(enum_ident.clone()) {
                                    multi_dest_enums.push(multi_dest_enum);
                                }
                                quote! {
                                    #transition_result_name<#enum_ident>
                                }
                            }
                        };
                        match ts.event.fields {
                            Fields::Unnamed(_) => {
                                let arglist = if ts.mutates_shared {
                                    quote! {&mut self.shared_state, val}
                                } else {
                                    quote! {val}
                                };
                                quote_spanned! { span =>
                                    #events_enum_name::#ev_variant(val) => {
                                        let res: #trans_type = state_data.#ts_fn(#arglist);
                                        #transition_result_transform
                                    }
                                }
                            }
                            Fields::Unit => {
                                let arglist = if ts.mutates_shared {
                                    quote! {&mut self.shared_state}
                                } else {
                                    quote! {}
                                };
                                quote_spanned! { span =>
                                    #events_enum_name::#ev_variant => {
                                        let res: #trans_type = state_data.#ts_fn(#arglist);
                                        #transition_result_transform
                                    }
                                }
                            }
                            Fields::Named(_) => unreachable!(),
                        }
                    } else {
                        // If events do not have a handler, attempt to construct the next state
                        // using `Default`.
                        if let [new_state] = ts.to.as_slice() {
                            let span = new_state.span();
                            let default_trans = quote_spanned! { span =>
                            let res = TransitionResult::<Self, #new_state>::from::<#from>(state_data);
                                #transition_result_transform
                            };
                            let span = ts.event.span();
                            match ts.event.fields {
                                Fields::Unnamed(_) => quote_spanned! { span =>
                                    #events_enum_name::#ev_variant(_val) => {
                                        #default_trans
                                    }
                                },
                                Fields::Unit => quote_spanned! { span =>
                                    #events_enum_name::#ev_variant => {
                                        #default_trans
                                    }
                                },
                                Fields::Named(_) => unreachable!(),
                            }
                        } else {
                            unreachable!("It should be impossible to have more than one dest state \
                                          in no-handler transitions")
                        }
                    }
                })
                // Since most states won't handle every possible event, return an error to that
                // effect
                .chain(std::iter::once(
                    quote! { _ => {
                        // Restore state in event the transition doesn't match
                        self.state = #occupied_current_state;
                        return Err(::rustfsm::MachineError::InvalidTransition)
                    } },
                ));
            quote! {
                #occupied_current_state => match event {
                    #(#event_branches),*
                }
            }
        }).chain(std::iter::once(
            quote! {
                None => Err(::rustfsm::MachineError::InvalidTransition)
            }
        )).collect();

        let viz_str = self.visualize();

        let trait_impl = quote! {
            impl ::rustfsm::StateMachine for #name {
                type Error = #err_type;
                type State = #state_enum_name;
                type SharedState = #shared_state_type;
                type Event = #events_enum_name;
                type Command = #cmd_type;

                fn name(&self) -> &str {
                  #name_str
                }

                fn on_event(&mut self, event: #events_enum_name)
                  -> ::core::result::Result<::std::vec::Vec<Self::Command>,
                                            ::rustfsm::MachineError<Self::Error>> {
                    let taken_state = self.state.take();
                    match taken_state {
                        #(#state_branches),*
                    }
                }

                fn state(&self) -> &Self::State {
                    self.state.as_ref().unwrap()
                }

                fn set_state(&mut self, new: Self::State) {
                    self.state = Some(new)
                }

                fn shared_state(&self) -> &Self::SharedState{
                    &self.shared_state
                }

                fn has_reached_final_state(&self) -> bool {
                    self.state.as_ref().unwrap().is_final()
                }

                fn from_parts(state: Self::State, shared: Self::SharedState) -> Self {
                    Self { shared_state: shared, state: Some(state) }
                }

                fn visualizer() -> &'static str {
                    #viz_str
                }
            }
        };

        let output = quote! {
            #transition_type_alias
            #machine_struct
            #states_enum
            #(#multi_dest_enums)*
            #states_enum_impl
            #events_enum
            #trait_impl
        };

        TokenStream::from(output)
    }

    fn all_states(&self) -> HashSet<Ident> {
        self.transitions
            .iter()
            .flat_map(|t| {
                let mut states = t.to.clone();
                states.push(t.from.clone());
                states
            })
            .collect()
    }

    fn visualize(&self) -> String {
        let transitions: Vec<String> = self
            .transitions
            .iter()
            .flat_map(|t| {
                t.to.iter()
                    .map(move |d| format!("{} --> {}: {}", t.from, d, t.event.ident))
            })
            // Add all final state transitions
            .chain(
                self.all_states()
                    .iter()
                    .filter(|s| self.is_final_state(s))
                    .map(|s| format!("{s} --> [*]")),
            )
            .collect();
        let transitions = transitions.join("\n");
        format!("@startuml\n{transitions}\n@enduml")
    }
}

/// Merge transition's dest state lists for those with the same from state & handler
fn merge_transition_dests(transitions: Vec<Transition>) -> Vec<Transition> {
    let mut map = HashMap::<_, Transition>::new();
    for t in transitions {
        // We want to use the transition sans-destinations as the key
        let without_dests = {
            let mut wd = t.clone();
            wd.to = vec![];
            wd
        };
        match map.entry(without_dests) {
            Entry::Occupied(mut e) => {
                e.get_mut().to.extend(t.to.into_iter());
            }
            Entry::Vacant(v) => {
                v.insert(t);
            }
        }
    }
    map.into_values().collect()
}
