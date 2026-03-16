use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
};

/// This trait defines a state machine (more formally, a [finite state
/// transducer](https://en.wikipedia.org/wiki/Finite-state_transducer)) which accepts events (the
/// input alphabet), uses them to mutate itself, and (may) output some commands (the output
/// alphabet) as a result.
pub trait StateMachine: Sized {
    /// The error type produced by this state machine when handling events
    type Error: Error;
    /// The type used to represent different machine states. Should be an enum.
    type State;
    /// The type used to represent state that common among all states. Should be a struct.
    type SharedState;
    /// The type used to represent events the machine handles. Should be an enum.
    type Event;
    /// The type used to represent commands the machine issues upon transitions.
    type Command;

    /// Handle an incoming event, returning any new commands or an error. Implementations may
    /// mutate current state, possibly moving to a new state.
    fn on_event(
        &mut self,
        event: Self::Event,
    ) -> Result<Vec<Self::Command>, MachineError<Self::Error>>;

    fn name(&self) -> &str;

    /// Returns the current state of the machine
    fn state(&self) -> &Self::State;
    fn set_state(&mut self, new_state: Self::State);

    /// Returns the current shared state of the machine
    fn shared_state(&self) -> &Self::SharedState;

    /// Returns true if the machine's current state is a final one
    fn has_reached_final_state(&self) -> bool;

    /// Given the shared data and new state, create a new instance.
    fn from_parts(state: Self::State, shared: Self::SharedState) -> Self;

    /// Return a PlantUML definition of the fsm that can be used to visualize it
    fn visualizer() -> &'static str;
}

/// The error returned by [StateMachine]s when handling events
#[derive(Debug)]
pub enum MachineError<E: Error> {
    /// An undefined transition was attempted
    InvalidTransition,
    /// Some error occurred while processing the transition
    Underlying(E),
}

impl<E: Error> From<E> for MachineError<E> {
    fn from(e: E) -> Self {
        Self::Underlying(e)
    }
}

impl<E: Error> Display for MachineError<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MachineError::InvalidTransition => f.write_str("Invalid transition"),
            MachineError::Underlying(e) => Display::fmt(&e, f),
        }
    }
}
impl<E: Error> Error for MachineError<E> {}

pub enum MachineUpdate<Machine>
where
    Machine: StateMachine,
{
    InvalidTransition,
    Ok { commands: Vec<Machine::Command> },
}

impl<M> MachineUpdate<M>
where
    M: StateMachine,
{
    /// Unwraps the machine update, panicking if the transition was invalid.
    pub fn unwrap(self) -> Vec<M::Command> {
        match self {
            Self::Ok { commands } => commands,
            Self::InvalidTransition => panic!("Transition was not successful!"),
        }
    }
}

/// A transition result is emitted every time the [StateMachine] handles an event.
pub enum TransitionResult<Machine, DestinationState>
where
    Machine: StateMachine,
    DestinationState: Into<Machine::State>,
{
    /// This state does not define a transition for this event from this state. All other errors
    /// should use the [Err](enum.TransitionResult.html#variant.Err) variant.
    InvalidTransition,
    /// The transition was successful
    Ok {
        commands: Vec<Machine::Command>,
        new_state: DestinationState,
    },
    /// There was some error performing the transition
    Err(Machine::Error),
}

impl<Sm, Ds> TransitionResult<Sm, Ds>
where
    Sm: StateMachine,
    Ds: Into<Sm::State>,
{
    /// Produce a transition with the provided commands to the provided state. No changes to shared
    /// state if it exists
    pub fn ok<CI>(commands: CI, new_state: Ds) -> Self
    where
        CI: IntoIterator<Item = Sm::Command>,
    {
        Self::Ok {
            commands: commands.into_iter().collect(),
            new_state,
        }
    }

    /// Uses `Into` to produce a transition with no commands from the provided current state to
    /// the provided (by type parameter) destination state.
    pub fn from<CurrentState>(current_state: CurrentState) -> Self
    where
        CurrentState: Into<Ds>,
    {
        let as_dest: Ds = current_state.into();
        Self::Ok {
            commands: vec![],
            new_state: as_dest,
        }
    }
}

impl<Sm, Ds> TransitionResult<Sm, Ds>
where
    Sm: StateMachine,
    Ds: Into<Sm::State> + Default,
{
    /// Produce a transition with commands relying on [Default] for the destination state's value
    pub fn commands<CI>(commands: CI) -> Self
    where
        CI: IntoIterator<Item = Sm::Command>,
    {
        Self::Ok {
            commands: commands.into_iter().collect(),
            new_state: Ds::default(),
        }
    }
}

impl<Sm, Ds> Default for TransitionResult<Sm, Ds>
where
    Sm: StateMachine,
    Ds: Into<Sm::State> + Default,
{
    fn default() -> Self {
        Self::Ok {
            commands: vec![],
            new_state: Ds::default(),
        }
    }
}

impl<Sm, Ds> TransitionResult<Sm, Ds>
where
    Sm: StateMachine,
    Ds: Into<Sm::State>,
{
    /// Turns more-specific (struct) transition result into more-general (enum) transition result
    pub fn into_general(self) -> TransitionResult<Sm, Sm::State> {
        match self {
            TransitionResult::InvalidTransition => TransitionResult::InvalidTransition,
            TransitionResult::Ok {
                commands,
                new_state,
            } => TransitionResult::Ok {
                commands,
                new_state: new_state.into(),
            },
            TransitionResult::Err(e) => TransitionResult::Err(e),
        }
    }

    /// Transforms the transition result into a machine-ready outcome with commands and new state,
    /// or a [MachineError]
    #[allow(clippy::type_complexity)]
    pub fn into_cmd_result(self) -> Result<(Vec<Sm::Command>, Sm::State), MachineError<Sm::Error>> {
        let general = self.into_general();
        match general {
            TransitionResult::Ok {
                new_state,
                commands,
            } => Ok((commands, new_state)),
            TransitionResult::InvalidTransition => Err(MachineError::InvalidTransition),
            TransitionResult::Err(e) => Err(MachineError::Underlying(e)),
        }
    }
}
