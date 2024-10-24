/* Find and resolve or report lookahead conflicts for bison,

   Copyright (C) 1984, 1989, 1992, 2000-2015, 2018-2021 Free Software
   Foundation, Inc.

   This file is part of Bison, the GNU Compiler Compiler.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

#include <config.h>
#include "system.h"

#include <bitset.h>

#include "complain.h"
#include "conflicts.h"
#include "counterexample.h"
#include "files.h"
#include "getargs.h"
#include "gram.h"
#include "lalr.h"
#include "lr0.h"
#include "print-xml.h"
#include "reader.h"
#include "state.h"
#include "symtab.h"

/* -1 stands for not specified. */
int expected_sr_conflicts = -1;
int expected_rr_conflicts = -1;

/* CONFLICTS[STATE-NUM] -- Whether that state has unresolved conflicts.  */
static bool *conflicts;

static struct obstack solved_conflicts_obstack;
static struct obstack solved_conflicts_xml_obstack;

static bitset shift_set;
static bitset lookahead_set;

bool
has_conflicts (const state *s)
{
  return conflicts[s->number];
}



enum conflict_resolution
  {
    shift_resolution,
    reduce_resolution,
    left_resolution,
    right_resolution,
    nonassoc_resolution
  };


/*----------------------------------------------------------------.
| Explain how an SR conflict between TOKEN and RULE was resolved: |
| RESOLUTION.                                                     |
`----------------------------------------------------------------*/

static inline void
log_resolution (rule *r, symbol_number token,
                enum conflict_resolution resolution)
{
  if (report_flag & report_solved_conflicts)
    {
      /* The description of the resolution. */
      switch (resolution)
        {
        case shift_resolution:
        case right_resolution:
          obstack_sgrow (&solved_conflicts_obstack, "    ");
          obstack_printf (&solved_conflicts_obstack,
                          _("Conflict between rule %d and token %s"
                            " resolved as shift"),
                          r->number,
                          symbols[token]->tag);
          break;

        case reduce_resolution:
        case left_resolution:
          obstack_sgrow (&solved_conflicts_obstack, "    ");
          obstack_printf (&solved_conflicts_obstack,
                          _("Conflict between rule %d and token %s"
                            " resolved as reduce"),
                          r->number,
                          symbols[token]->tag);
          break;

        case nonassoc_resolution:
          obstack_sgrow (&solved_conflicts_obstack, "    ");
          obstack_printf (&solved_conflicts_obstack,
                          _("Conflict between rule %d and token %s"
                            " resolved as an error"),
                          r->number,
                          symbols[token]->tag);
          break;
        }

      /* The reason. */
      switch (resolution)
        {
        case shift_resolution:
          obstack_printf (&solved_conflicts_obstack,
                          " (%s < %s)",
                          r->prec->symbol->tag,
                          symbols[token]->tag);
          break;

        case reduce_resolution:
          obstack_printf (&solved_conflicts_obstack,
                          " (%s < %s)",
                          symbols[token]->tag,
                          r->prec->symbol->tag);
          break;

        case left_resolution:
          obstack_printf (&solved_conflicts_obstack,
                          " (%%left %s)",
                          symbols[token]->tag);
          break;

        case right_resolution:
          obstack_printf (&solved_conflicts_obstack,
                          " (%%right %s)",
                          symbols[token]->tag);
          break;

        case nonassoc_resolution:
          obstack_printf (&solved_conflicts_obstack,
                          " (%%nonassoc %s)",
                          symbols[token]->tag);
          break;
        }

      obstack_sgrow (&solved_conflicts_obstack, ".\n");
    }

  /* XML report */
  if (xml_flag)
    {
      /* The description of the resolution. */
      switch (resolution)
        {
        case shift_resolution:
        case right_resolution:
          obstack_printf (&solved_conflicts_xml_obstack,
                          "        <resolution rule=\"%d\" symbol=\"%s\""
                          " type=\"shift\">",
                          r->number,
                          xml_escape (symbols[token]->tag));
          break;

        case reduce_resolution:
        case left_resolution:
          obstack_printf (&solved_conflicts_xml_obstack,
                          "        <resolution rule=\"%d\" symbol=\"%s\""
                          " type=\"reduce\">",
                          r->number,
                          xml_escape (symbols[token]->tag));
          break;

        case nonassoc_resolution:
          obstack_printf (&solved_conflicts_xml_obstack,
                          "        <resolution rule=\"%d\" symbol=\"%s\""
                          " type=\"error\">",
                          r->number,
                          xml_escape (symbols[token]->tag));
          break;
        }

      /* The reason. */
      switch (resolution)
        {
        case shift_resolution:
          obstack_printf (&solved_conflicts_xml_obstack,
                          "%s &lt; %s",
                          xml_escape_n (0, r->prec->symbol->tag),
                          xml_escape_n (1, symbols[token]->tag));
          break;

        case reduce_resolution:
          obstack_printf (&solved_conflicts_xml_obstack,
                          "%s &lt; %s",
                          xml_escape_n (0, symbols[token]->tag),
                          xml_escape_n (1, r->prec->symbol->tag));
          break;

        case left_resolution:
          obstack_printf (&solved_conflicts_xml_obstack,
                          "%%left %s",
                          xml_escape (symbols[token]->tag));
          break;

        case right_resolution:
          obstack_printf (&solved_conflicts_xml_obstack,
                          "%%right %s",
                          xml_escape (symbols[token]->tag));
          break;

        case nonassoc_resolution:
          obstack_printf (&solved_conflicts_xml_obstack,
                          "%%nonassoc %s",
                          xml_escape (symbols[token]->tag));
      break;
        }

      obstack_sgrow (&solved_conflicts_xml_obstack, "</resolution>\n");
    }
}


/*------------------------------------------------------------------.
| Turn off the shift recorded for the specified token in the        |
| specified state.  Used when we resolve a shift/reduce conflict in |
| favor of the reduction or as an error (%nonassoc).                |
`------------------------------------------------------------------*/

static void
flush_shift (state *s, int token)
{
  transitions *trans = s->transitions;

  bitset_reset (lookahead_set, token);
  for (int i = 0; i < trans->num; ++i)
    if (!TRANSITION_IS_DISABLED (trans, i)
        && TRANSITION_SYMBOL (trans, i) == token)
      TRANSITION_DISABLE (trans, i);
}


/*--------------------------------------------------------------------.
| Turn off the reduce recorded for the specified token in the         |
| specified lookahead set.  Used when we resolve a shift/reduce       |
| conflict in favor of the shift or as an error (%nonassoc).          |
`--------------------------------------------------------------------*/

static void
flush_reduce (bitset lookaheads, int token)
{
  bitset_reset (lookaheads, token);
}


/*------------------------------------------------------------------.
| Attempt to resolve shift/reduce conflict for one rule by means of |
| precedence declarations.  It has already been checked that the    |
| rule has a precedence.  A conflict is resolved by modifying the   |
| shift or reduce tables so that there is no longer a conflict.     |
|                                                                   |
| RULENO is the number of the lookahead bitset to consider.         |
|                                                                   |
| ERRORS and NERRS can be used to store discovered explicit         |
| errors.                                                           |
`------------------------------------------------------------------*/

static void
resolve_sr_conflict (state *s, int ruleno, symbol **errors, int *nerrs)
{
  reductions *reds = s->reductions;
  /* Find the rule to reduce by to get precedence of reduction.  */
  rule *redrule = reds->rules[ruleno];
  int redprec = redrule->prec->prec;
  bitset lookaheads = reds->lookaheads[ruleno];

  for (symbol_number i = 0; i < ntokens; ++i)
    if (bitset_test (lookaheads, i)
        && bitset_test (lookahead_set, i)
        && symbols[i]->content->prec)
      {
        /* Shift/reduce conflict occurs for token number i
           and it has a precedence.
           The precedence of shifting is that of token i.  */
        if (symbols[i]->content->prec < redprec)
          {
            register_precedence (redrule->prec->number, i);
            log_resolution (redrule, i, reduce_resolution);
            flush_shift (s, i);
          }
        else if (symbols[i]->content->prec > redprec)
          {
            register_precedence (i, redrule->prec->number);
            log_resolution (redrule, i, shift_resolution);
            flush_reduce (lookaheads, i);
          }
        else
          /* Matching precedence levels.
             For non-defined associativity, keep both: unexpected
             associativity conflict.
             For left associativity, keep only the reduction.
             For right associativity, keep only the shift.
             For nonassociativity, keep neither.  */

          switch (symbols[i]->content->assoc)
            {
            case undef_assoc:
              abort ();

            case precedence_assoc:
              break;

            case right_assoc:
              register_assoc (i, redrule->prec->number);
              log_resolution (redrule, i, right_resolution);
              flush_reduce (lookaheads, i);
              break;

            case left_assoc:
              register_assoc (i, redrule->prec->number);
              log_resolution (redrule, i, left_resolution);
              flush_shift (s, i);
              break;

            case non_assoc:
              register_assoc (i, redrule->prec->number);
              log_resolution (redrule, i, nonassoc_resolution);
              flush_shift (s, i);
              flush_reduce (lookaheads, i);
              /* Record an explicit error for this token.  */
              errors[(*nerrs)++] = symbols[i];
              break;
            }
      }
}


/*-------------------------------------------------------------------.
| Solve the S/R conflicts of state S using the                       |
| precedence/associativity, and flag it inconsistent if it still has |
| conflicts.  ERRORS can be used as storage to compute the list of   |
| lookahead tokens on which S raises a syntax error (%nonassoc).     |
`-------------------------------------------------------------------*/

static void
set_conflicts (state *s, symbol **errors)
{
  if (s->consistent)
    return;

  reductions *reds = s->reductions;
  int nerrs = 0;

  bitset_zero (lookahead_set);

  {
    transitions *trans = s->transitions;
    int i;
    FOR_EACH_SHIFT (trans, i)
      bitset_set (lookahead_set, TRANSITION_SYMBOL (trans, i));
  }

  /* Loop over all rules which require lookahead in this state.  First
     check for shift/reduce conflict, and try to resolve using
     precedence.  */
  for (int i = 0; i < reds->num; ++i)
    if (reds->rules[i]->prec
        && reds->rules[i]->prec->prec
        && !bitset_disjoint_p (reds->lookaheads[i], lookahead_set))
      resolve_sr_conflict (s, i, errors, &nerrs);

  if (nerrs)
    /* Some tokens have been explicitly made errors.  Allocate a
       permanent errs structure for this state, to record them.  */
    state_errs_set (s, nerrs, errors);

  if (obstack_object_size (&solved_conflicts_obstack))
    s->solved_conflicts = obstack_finish0 (&solved_conflicts_obstack);
  if (obstack_object_size (&solved_conflicts_xml_obstack))
    s->solved_conflicts_xml = obstack_finish0 (&solved_conflicts_xml_obstack);

  /* Loop over all rules which require lookahead in this state.  Check
     for conflicts not resolved above.

     reds->lookaheads can be NULL if the LR type is LR(0).  */
  if (reds->lookaheads)
    for (int i = 0; i < reds->num; ++i)
      {
        if (!bitset_disjoint_p (reds->lookaheads[i], lookahead_set))
          conflicts[s->number] = true;
        bitset_or (lookahead_set, lookahead_set, reds->lookaheads[i]);
      }
}


/*----------------------------------------------------------------.
| Solve all the S/R conflicts using the precedence/associativity, |
| and flag as inconsistent the states that still have conflicts.  |
`----------------------------------------------------------------*/

void
conflicts_solve (void)
{
  /* List of lookahead tokens on which we explicitly raise a syntax error.  */
  symbol **errors = xnmalloc (ntokens + 1, sizeof *errors);

  conflicts = xcalloc (nstates, sizeof *conflicts);
  shift_set = bitset_create (ntokens, BITSET_FIXED);
  lookahead_set = bitset_create (ntokens, BITSET_FIXED);
  obstack_init (&solved_conflicts_obstack);
  obstack_init (&solved_conflicts_xml_obstack);

  for (state_number i = 0; i < nstates; ++i)
    {
      set_conflicts (states[i], errors);

      /* For uniformity of the code, make sure all the states have a valid
         'errs' member.  */
      if (!states[i]->errs)
        states[i]->errs = errs_new (0, 0);
    }

  free (errors);
}


void
conflicts_update_state_numbers (state_number old_to_new[],
                                state_number nstates_old)
{
  for (state_number i = 0; i < nstates_old; ++i)
    if (old_to_new[i] != nstates_old)
      conflicts[old_to_new[i]] = conflicts[i];
}


/*---------------------------------------------.
| Count the number of shift/reduce conflicts.  |
`---------------------------------------------*/

static size_t
count_state_sr_conflicts (const state *s)
{
  transitions *trans = s->transitions;
  reductions *reds = s->reductions;

  if (!trans)
    return 0;

  bitset_zero (lookahead_set);
  bitset_zero (shift_set);

  {
    int i;
    FOR_EACH_SHIFT (trans, i)
      bitset_set (shift_set, TRANSITION_SYMBOL (trans, i));
  }

  for (int i = 0; i < reds->num; ++i)
    bitset_or (lookahead_set, lookahead_set, reds->lookaheads[i]);

  bitset_and (lookahead_set, lookahead_set, shift_set);

  return bitset_count (lookahead_set);
}

/*---------------------------------------------.
| The total number of shift/reduce conflicts.  |
`---------------------------------------------*/

static size_t
count_sr_conflicts (void)
{
  size_t res = 0;
  /* Conflicts by state.  */
  for (state_number i = 0; i < nstates; ++i)
    if (conflicts[i])
      res += count_state_sr_conflicts (states[i]);
  return res;
}



/*-----------------------------------------------------------------.
| Count the number of reduce/reduce conflicts.  Count one conflict |
| for each reduction after the first for a given token.            |
`-----------------------------------------------------------------*/

static size_t
count_state_rr_conflicts (const state *s)
{
  reductions *reds = s->reductions;
  size_t res = 0;

  for (symbol_number i = 0; i < ntokens; ++i)
    {
      int count = 0;
      for (int j = 0; j < reds->num; ++j)
        count += bitset_test (reds->lookaheads[j], i);
      if (2 <= count)
        res += count-1;
    }

  return res;
}

static size_t
count_rr_conflicts (void)
{
  size_t res = 0;
  /* Conflicts by state.  */
  for (state_number i = 0; i < nstates; ++i)
    if (conflicts[i])
      res += count_state_rr_conflicts (states[i]);
  return res;
}


/*------------------------------------------------------------------.
| For a given rule, the number of shift/reduce conflicts in a given |
| state.                                                            |
`------------------------------------------------------------------*/

static size_t
count_rule_state_sr_conflicts (rule *r, state *s)
{
  size_t res = 0;
  transitions *trans = s->transitions;
  reductions *reds = s->reductions;

  for (int i = 0; i < reds->num; ++i)
    if (reds->rules[i] == r)
      {
        bitset lookaheads = reds->lookaheads[i];
        int j;
        FOR_EACH_SHIFT (trans, j)
          res += bitset_test (lookaheads, TRANSITION_SYMBOL (trans, j));
      }

  return res;
}

/*----------------------------------------------------------------------.
| For a given rule, count the number of states for which it is involved |
| in shift/reduce conflicts.                                            |
`----------------------------------------------------------------------*/

static size_t
count_rule_sr_conflicts (rule *r)
{
  size_t res = 0;
  for (state_number i = 0; i < nstates; ++i)
    if (conflicts[i])
      res += count_rule_state_sr_conflicts (r, states[i]);
  return res;
}

/*-----------------------------------------------------------------.
| For a given rule, count the number of states in which it is      |
| involved in reduce/reduce conflicts.                             |
`-----------------------------------------------------------------*/

static size_t
count_rule_state_rr_conflicts (rule *r, state *s)
{
  size_t res = 0;
  const reductions *reds = s->reductions;
  bitset lookaheads = bitset_create (ntokens, BITSET_FIXED);

  for (int i = 0; i < reds->num; ++i)
    if (reds->rules[i] == r)
      for (int j = 0; j < reds->num; ++j)
        if (reds->rules[j] != r)
          {
            bitset_and (lookaheads,
                        reds->lookaheads[i],
                        reds->lookaheads[j]);
            res += bitset_count (lookaheads);
          }
  bitset_free (lookaheads);
  return res;
}

static size_t
count_rule_rr_conflicts (rule *r)
{
  size_t res = 0;
  for (state_number i = 0; i < nstates; ++i)
    res += count_rule_state_rr_conflicts (r, states[i]);
  return res;
}

/*-----------------------------------------------------------.
| Output the detailed description of states with conflicts.  |
`-----------------------------------------------------------*/

void
conflicts_output (FILE *out)
{
  bool printed_sth = false;
  for (state_number i = 0; i < nstates; ++i)
    if (conflicts[i])
      {
        const state *s = states[i];
        int src = count_state_sr_conflicts (s);
        int rrc = count_state_rr_conflicts (s);
        fprintf (out, _("State %d "), i);
        if (src && rrc)
          fprintf (out,
                   _("conflicts: %d shift/reduce, %d reduce/reduce\n"),
                   src, rrc);
        else if (src)
          fprintf (out, _("conflicts: %d shift/reduce\n"), src);
        else if (rrc)
          fprintf (out, _("conflicts: %d reduce/reduce\n"), rrc);
        printed_sth = true;
      }
  if (printed_sth)
    fputs ("\n\n", out);
}

/*--------------------------------------------.
| Total the number of S/R and R/R conflicts.  |
`--------------------------------------------*/

int
conflicts_total_count (void)
{
  return count_sr_conflicts () + count_rr_conflicts ();
}

static void
report_counterexamples (void)
{
  for (state_number sn = 0; sn < nstates; ++sn)
    if (conflicts[sn])
      counterexample_report_state (states[sn], stderr, "");
}

/*------------------------------------------------.
| Report per-rule %expect/%expect-rr mismatches.  |
`------------------------------------------------*/

static void
report_rule_expectation_mismatches (void)
{
  for (rule_number i = 0; i < nrules; i += 1)
    {
      rule *r = &rules[i];
      int expected_sr = r->expected_sr_conflicts;
      int expected_rr = r->expected_rr_conflicts;

      if (expected_sr != -1 || expected_rr != -1)
        {
          int sr = count_rule_sr_conflicts (r);
          if (sr != expected_sr && (sr != 0 || expected_sr != -1))
            complain (&r->location, complaint,
                      _("shift/reduce conflicts for rule %d:"
                        " %d found, %d expected"),
                      r->code, sr, expected_sr);
          int rr = count_rule_rr_conflicts (r);
          if (rr != expected_rr && (rr != 0 || expected_rr != -1))
            complain (&r->location, complaint,
                      _("reduce/reduce conflicts for rule %d:"
                        " %d found, %d expected"),
                      r->code, rr, expected_rr);
        }
    }
}

/*---------------------------------.
| Reporting numbers of conflicts.  |
`---------------------------------*/

void
conflicts_print (void)
{
  report_rule_expectation_mismatches ();

  if (! glr_parser && expected_rr_conflicts != -1)
    {
      complain (NULL, Wother, _("%%expect-rr applies only to GLR parsers"));
      expected_rr_conflicts = -1;
    }

  // The warning flags used to emit a diagnostic, if we did.
  warnings unexpected_conflicts_warning = Wnone;
  /* The following two blocks scream for factoring, but i18n support
     would make it ugly.  */
  {
    int total = count_sr_conflicts ();
    /* If %expect is not used, but %expect-rr is, then expect 0 sr.  */
    int expected =
      (expected_sr_conflicts == -1 && expected_rr_conflicts != -1)
      ? 0
      : expected_sr_conflicts;
    if (expected != -1)
      {
        if (expected != total)
          {
            complain (NULL, complaint,
                      _("shift/reduce conflicts: %d found, %d expected"),
                      total, expected);
            if (total)
              unexpected_conflicts_warning = complaint;
          }
      }
    else if (total)
      {
        complain (NULL, Wconflicts_sr,
                  ngettext ("%d shift/reduce conflict",
                            "%d shift/reduce conflicts",
                            total),
                  total);
        unexpected_conflicts_warning = Wconflicts_sr;
      }
  }

  {
    int total = count_rr_conflicts ();
    /* If %expect-rr is not used, but %expect is, then expect 0 rr.  */
    int expected =
      (expected_rr_conflicts == -1 && expected_sr_conflicts != -1)
      ? 0
      : expected_rr_conflicts;
    if (expected != -1)
      {
        if (expected != total)
          {
            complain (NULL, complaint,
                      _("reduce/reduce conflicts: %d found, %d expected"),
                      total, expected);
            if (total)
              unexpected_conflicts_warning = complaint;
          }
      }
    else if (total)
      {
        complain (NULL, Wconflicts_rr,
                  ngettext ("%d reduce/reduce conflict",
                            "%d reduce/reduce conflicts",
                            total),
                  total);
        unexpected_conflicts_warning = Wconflicts_rr;
      }
  }

  if (warning_is_enabled (Wcounterexamples))
    report_counterexamples ();
  else if (unexpected_conflicts_warning != Wnone)
    subcomplain (NULL, unexpected_conflicts_warning,
                 _("rerun with option '-Wcounterexamples'"
                   " to generate conflict counterexamples"));
}

void
conflicts_free (void)
{
  free (conflicts);
  bitset_free (shift_set);
  bitset_free (lookahead_set);
  obstack_free (&solved_conflicts_obstack, NULL);
  obstack_free (&solved_conflicts_xml_obstack, NULL);
}
