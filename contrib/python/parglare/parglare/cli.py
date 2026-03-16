#!/usr/bin/env python
import sys

import click

import parglare.termui as t
from parglare import GLRParser, Grammar, GrammarError, Parser, SyntaxError
from parglare.export import grammar_pda_export
from parglare.tables import create_load_table
from parglare.termui import a_print, h_print, prints


@click.group()
@click.option("--debug", default=False, is_flag=True, help="Debug/trace output.")
@click.option("--no-colors", default=False, is_flag=True, help="Disable output coloring.")
@click.option(
    "--prefer-shifts",
    default=False,
    is_flag=True,
    help="Prefer shifts over reductions.",
)
@click.option(
    "--prefer-shifts-over-empty",
    default=False,
    is_flag=True,
    help="Prefer shifts over empty reductions.",
)
@click.pass_context
def pglr(ctx, debug, no_colors, prefer_shifts, prefer_shifts_over_empty):
    """
    Command line interface for working with parglare grammars.
    """
    ctx.obj = {
        "debug": debug,
        "colors": not no_colors,
        "prefer_shifts": prefer_shifts,
        "prefer_shifts_over_empty": prefer_shifts_over_empty,
    }


@pglr.command()
@click.argument("grammar_file", type=click.Path())
@click.pass_context
def compile(ctx, grammar_file):
    debug = ctx.obj["debug"]
    colors = ctx.obj["colors"]
    prefer_shifts = ctx.obj["prefer_shifts"]
    prefer_shifts_over_empty = ctx.obj["prefer_shifts_over_empty"]
    h_print("Compiling...")
    compile_get_grammar_table(
        grammar_file, debug, colors, prefer_shifts, prefer_shifts_over_empty
    )


@pglr.command()
@click.argument("grammar_file", type=click.Path())
@click.option("--input-file", "-f", type=click.Path(), help="File to parse")
@click.option("--input", "-i", help="Input string to parse")
@click.option("--glr", "-g", default=False, is_flag=True, help="Parse with GLR")
@click.option("--recovery", "-r", default=False, is_flag=True, help="Use error recovery")
@click.option("--dot", default=False, is_flag=True, help="Export tree/forest to dot file")
@click.option(
    "--positions",
    default=False,
    is_flag=True,
    help="Render node positions in dot export",
)
@click.pass_context
def parse(ctx, grammar_file, input_file, input, glr, recovery, dot, positions):
    if not (input_file or input):
        prints("Expected either input_file or input string.")
        sys.exit(1)
    colors = ctx.obj["colors"]
    debug = ctx.obj["debug"]
    prefer_shifts = ctx.obj["prefer_shifts"]
    prefer_shifts_over_empty = ctx.obj["prefer_shifts_over_empty"]
    grammar = Grammar.from_file(grammar_file, debug=debug, debug_colors=colors)
    if glr:
        parser = GLRParser(
            grammar,
            debug=debug,
            debug_colors=colors,
            error_recovery=recovery,
            prefer_shifts=prefer_shifts,
            prefer_shifts_over_empty=prefer_shifts_over_empty,
        )
    else:
        parser = Parser(
            grammar,
            build_tree=True,
            debug=debug,
            debug_colors=colors,
            error_recovery=recovery,
            prefer_shifts=prefer_shifts,
            prefer_shifts_over_empty=prefer_shifts_over_empty,
        )

    result = parser.parse(input) if input else parser.parse_file(input_file)

    if glr:
        print(f"Solutions:{result.solutions}")
        print(f"Ambiguities:{result.ambiguities}")

    if recovery:
        print(f"Errors: {len(parser.errors)}")
        for error in parser.errors:
            print("\t", str(error))

    if glr and result.solutions > 1:
        print("Printing the forest:\n")
        result = result
    else:
        print("Printing the parse tree:\n")

    print(result.to_str())

    if dot:
        f_name = "forest.dot" if glr and result.solutions > 1 else "tree.dot"
        with open(f_name, "w") as f:
            f.write(result.to_dot(positions))
        print("Created dot file ", f_name)


@pglr.command()
@click.argument("grammar_file", type=click.Path())
@click.pass_context
def viz(ctx, grammar_file):
    debug = ctx.obj["debug"]
    colors = ctx.obj["colors"]
    prefer_shifts = ctx.obj["prefer_shifts"]
    prefer_shifts_over_empty = ctx.obj["prefer_shifts_over_empty"]
    t.colors = colors
    grammar, table = compile_get_grammar_table(
        grammar_file, debug, colors, prefer_shifts, prefer_shifts_over_empty
    )
    prints(f"Generating '{grammar_file}.dot' file for the grammar PDA.")
    prints(
        "Use dot viewer (e.g. xdot) or convert to pdf by running "
        f"'dot -Tpdf -O {grammar_file}.dot'"
    )
    t.colors = False
    grammar_pda_export(table, f"{grammar_file}.dot")


@pglr.command()
@click.argument("grammar_file", type=click.Path())
@click.option("--input-file", "-f", type=click.Path(), help="Input file for tracing")
@click.option("--input", "-i", help="Input string for tracing")
@click.option(
    "--frontiers",
    "-r",
    default=False,
    is_flag=True,
    help="Align GSS nodes into frontiers (token levels)",
)
@click.pass_context
def trace(ctx, grammar_file, input_file, input, frontiers):
    if not (input_file or input):
        prints("Expected either input_file or input string.")
        sys.exit(1)
    colors = ctx.obj["colors"]
    prefer_shifts = ctx.obj["prefer_shifts"]
    prefer_shifts_over_empty = ctx.obj["prefer_shifts_over_empty"]
    grammar, table = compile_get_grammar_table(
        grammar_file, True, colors, prefer_shifts, prefer_shifts_over_empty
    )
    parser = GLRParser(
        grammar,
        debug=True,
        debug_trace=True,
        debug_colors=colors,
        prefer_shifts=prefer_shifts,
        prefer_shifts_over_empty=prefer_shifts_over_empty,
        debug_trace_frontiers=frontiers,
    )
    if input:
        parser.parse(input)
    else:
        parser.parse_file(input_file)


def compile_get_grammar_table(
    grammar_file, debug, colors, prefer_shifts, prefer_shifts_over_empty
):
    try:
        g = Grammar.from_file(
            grammar_file, _no_check_recognizers=True, debug_colors=colors
        )
        if debug:
            g.print_debug()
        table = create_load_table(
            g,
            prefer_shifts=prefer_shifts,
            prefer_shifts_over_empty=prefer_shifts_over_empty,
            force_create=True,
            debug=debug,
        )
        if debug or table.sr_conflicts or table.rr_conflicts:
            table.print_debug()

        if not table.sr_conflicts and not table.rr_conflicts:
            h_print("Grammar OK.")

        if table.sr_conflicts:
            if len(table.sr_conflicts) == 1:
                message = "There is 1 Shift/Reduce conflict."
            else:
                message = f"There are {len(table.sr_conflicts)} Shift/Reduce conflicts."
            a_print(message)
            prints(
                "Either use 'prefer_shifts' parser mode, try to resolve "
                "manually, or use GLR parsing."
            )
        if table.rr_conflicts:
            if len(table.rr_conflicts) == 1:
                message = "There is 1 Reduce/Reduce conflict."
            else:
                message = f"There are {len(table.rr_conflicts)} Reduce/Reduce conflicts."
            a_print(message)
            prints("Try to resolve manually or use GLR parsing.")

    except (GrammarError, SyntaxError) as e:
        print("Error in the grammar file.")
        print(e)
        sys.exit(1)

    return g, table


if __name__ == "__main__":
    pglr()
