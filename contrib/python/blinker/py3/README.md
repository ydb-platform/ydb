# Blinker

Blinker provides a fast dispatching system that allows any number of
interested parties to subscribe to events, or "signals".


## Pallets Community Ecosystem

> [!IMPORTANT]\
> This project is part of the Pallets Community Ecosystem. Pallets is the open
> source organization that maintains Flask; Pallets-Eco enables community
> maintenance of related projects. If you are interested in helping maintain
> this project, please reach out on [the Pallets Discord server][discord].
>
> [discord]: https://discord.gg/pallets


## Example

Signal receivers can subscribe to specific senders or receive signals
sent by any sender.

```pycon
>>> from blinker import signal
>>> started = signal('round-started')
>>> def each(round):
...     print(f"Round {round}")
...
>>> started.connect(each)

>>> def round_two(round):
...     print("This is round two.")
...
>>> started.connect(round_two, sender=2)

>>> for round in range(1, 4):
...     started.send(round)
...
Round 1!
Round 2!
This is round two.
Round 3!
```
