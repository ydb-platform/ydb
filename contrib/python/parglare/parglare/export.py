from parglare.common import dot_escape
from parglare.parser import REDUCE, SHIFT

HEADER = """
    digraph grammar {
    rankdir=LR
    fontname = "Bitstream Vera Sans"
    fontsize = 8
    node[
        shape=record,
        style=filled,
        fillcolor=aliceblue
    ]
    nodesep = 0.3
    edge[dir=black,arrowtail=empty]


"""


def grammar_pda_export(table, file_name):
    with open(file_name, "w", encoding="utf-8") as f:
        f.write(HEADER)

        for state in table.states:
            kernel_items = ""
            for item in state.kernel_items:
                kernel_items += f"{dot_escape(str(item))}\\l"

            nonkernel_items = "|" if state.nonkernel_items else ""
            for item in state.nonkernel_items:
                nonkernel_items += f"{dot_escape(str(item))}\\l"

            # SHIFT actions and GOTOs will be encoded in links.
            # REDUCE actions will be presented inside each node.
            reduce_actions = []
            for term, actions in state.actions.items():
                r_actions = [a for a in actions if a.action is REDUCE]
                if r_actions:
                    reduce_actions.append((term, r_actions))

            reductions = ""
            if reduce_actions:
                reductions = "|Reductions:\\l{}".format(
                    ", ".join(
                        [
                            "{}:{}".format(
                                dot_escape(x[0].name),
                                x[1][0].prod.prod_id
                                if len(x[1]) == 1
                                else "[{}]".format(
                                    ",".join([str(i.prod.prod_id) for i in x[1]])
                                ),
                            )
                            for x in reduce_actions
                        ]
                    )
                )

            # States
            f.write(
                '{}[label="{}|{}{}{}"]\n'.format(
                    state.state_id,
                    dot_escape(f"{state.state_id}:{state.symbol}"),
                    kernel_items,
                    nonkernel_items,
                    reductions,
                )
            )

            f.write("\n")

            # SHIFT and GOTOs as links
            shacc = []
            for term, actions in state.actions.items():
                for a in [a for a in actions if a.action is SHIFT]:
                    shacc.append((term, a))
            for term, action in shacc:
                f.write(
                    '{} -> {} [label="{}:{}"]'.format(
                        state.state_id,
                        action.state.state_id,
                        "SHIFT" if action.action is SHIFT else "ACCEPT",
                        term,
                    )
                )

            for symb, goto_state in ((symb, goto) for symb, goto in state.gotos.items()):
                f.write(
                    f'{state.state_id} -> {goto_state.state_id} [label="GOTO:{symb}"]'
                )

        f.write("\n}\n")
