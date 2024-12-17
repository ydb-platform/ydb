/* Type definitions for the finite state machine for Bison.

   Copyright (C) 2001-2007, 2009-2015, 2018-2021 Free Software
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
#include "state.h"

#include "system.h"

#include <hash.h>

#include "closure.h"
#include "complain.h"
#include "getargs.h"
#include "gram.h"
#include "print-xml.h"


                        /*-------------------.
                        | Shifts and Gotos.  |
                        `-------------------*/


/*-----------------------------------------.
| Create a new array of NUM shifts/gotos.  |
`-----------------------------------------*/

static transitions *
transitions_new (int num, state **dst)
{
  size_t states_size = num * sizeof *dst;
  transitions *res = xmalloc (offsetof (transitions, states) + states_size);
  res->num = num;
  memcpy (res->states, dst, states_size);
  return res;
}


state *
transitions_to (state *s, symbol_number sym)
{
  transitions *trans = s->transitions;
  for (int i = 0; i < trans->num; ++i)
    if (TRANSITION_SYMBOL (trans, i) == sym)
      return trans->states[i];
  abort ();
}


                        /*--------------------.
                        | Error transitions.  |
                        `--------------------*/


/*---------------------------------.
| Create a new array of NUM errs.  |
`---------------------------------*/

errs *
errs_new (int num, symbol **tokens)
{
  size_t symbols_size = num * sizeof *tokens;
  errs *res = xmalloc (offsetof (errs, symbols) + symbols_size);
  res->num = num;
  if (tokens)
    memcpy (res->symbols, tokens, symbols_size);
  return res;
}




                        /*-------------.
                        | Reductions.  |
                        `-------------*/


/*---------------------------------------.
| Create a new array of NUM reductions.  |
`---------------------------------------*/

static reductions *
reductions_new (int num, rule **reds)
{
  size_t rules_size = num * sizeof *reds;
  reductions *res = xmalloc (offsetof (reductions, rules) + rules_size);
  res->num = num;
  res->lookaheads = NULL;
  memcpy (res->rules, reds, rules_size);
  return res;
}



                        /*---------.
                        | States.  |
                        `---------*/


state_number nstates = 0;
/* FINAL_STATE is properly set by new_state when it recognizes its
   accessing symbol: $end.  */
state *final_state = NULL;


/*------------------------------------------------------------------.
| Create a new state with ACCESSING_SYMBOL, for those items.  Store |
| it in the state hash table.                                       |
`------------------------------------------------------------------*/

state *
state_new (symbol_number accessing_symbol,
           size_t nitems, item_index *core)
{
  aver (nstates < STATE_NUMBER_MAXIMUM);

  size_t items_size = nitems * sizeof *core;
  state *res = xmalloc (offsetof (state, items) + items_size);
  res->number = nstates++;
  res->accessing_symbol = accessing_symbol;
  res->transitions = NULL;
  res->reductions = NULL;
  res->errs = NULL;
  res->state_list = NULL;
  res->consistent = false;
  res->solved_conflicts = NULL;
  res->solved_conflicts_xml = NULL;

  res->nitems = nitems;
  memcpy (res->items, core, items_size);

  state_hash_insert (res);

  return res;
}

state *
state_new_isocore (state const *s)
{
  aver (nstates < STATE_NUMBER_MAXIMUM);

  size_t items_size = s->nitems * sizeof *s->items;
  state *res = xmalloc (offsetof (state, items) + items_size);
  res->number = nstates++;
  res->accessing_symbol = s->accessing_symbol;
  res->transitions =
    transitions_new (s->transitions->num, s->transitions->states);
  res->reductions = reductions_new (s->reductions->num, s->reductions->rules);
  res->errs = NULL;
  res->state_list = NULL;
  res->consistent = s->consistent;
  res->solved_conflicts = NULL;
  res->solved_conflicts_xml = NULL;

  res->nitems = s->nitems;
  memcpy (res->items, s->items, items_size);

  return res;
}


/*---------.
| Free S.  |
`---------*/

static void
state_free (state *s)
{
  free (s->transitions);
  free (s->reductions);
  free (s->errs);
  free (s);
}


void
state_transitions_print (const state *s, FILE *out)
{
  const transitions *trans = s->transitions;
  fprintf (out, "transitions of %d (%d):\n",
           s->number, trans->num);
  for (int i = 0; i < trans->num; ++i)
    fprintf (out, "  %d: (%d, %s, %d)\n",
             i,
             s->number,
             symbols[s->transitions->states[i]->accessing_symbol]->tag,
             s->transitions->states[i]->number);
}


/*---------------------------.
| Set the transitions of S.  |
`---------------------------*/

void
state_transitions_set (state *s, int num, state **dst)
{
  aver (!s->transitions);
  s->transitions = transitions_new (num, dst);
  if (trace_flag & trace_automaton)
    state_transitions_print (s, stderr);
}


/*--------------------------.
| Set the reductions of S.  |
`--------------------------*/

void
state_reductions_set (state *s, int num, rule **reds)
{
  aver (!s->reductions);
  s->reductions = reductions_new (num, reds);
}


int
state_reduction_find (state const *s, rule const *r)
{
  reductions *reds = s->reductions;
  for (int i = 0; i < reds->num; ++i)
    if (reds->rules[i] == r)
      return i;
  abort ();
}


/*--------------------.
| Set the errs of S.  |
`--------------------*/

void
state_errs_set (state *s, int num, symbol **tokens)
{
  aver (!s->errs);
  s->errs = errs_new (num, tokens);
}



/*--------------------------------------------------.
| Print on OUT all the lookahead tokens such that S |
| wants to reduce R.                                |
`--------------------------------------------------*/

void
state_rule_lookaheads_print (state const *s, rule const *r, FILE *out)
{
  /* Find the reduction we are handling.  */
  reductions *reds = s->reductions;
  int red = state_reduction_find (s, r);

  /* Print them if there are.  */
  if (reds->lookaheads && red != -1)
    {
      bitset_iterator biter;
      int k;
      char const *sep = "";
      fprintf (out, "  [");
      BITSET_FOR_EACH (biter, reds->lookaheads[red], k, 0)
        {
          fprintf (out, "%s%s", sep, symbols[k]->tag);
          sep = ", ";
        }
      fprintf (out, "]");
    }
}

void
state_rule_lookaheads_print_xml (state const *s, rule const *r,
                                       FILE *out, int level)
{
  /* Find the reduction we are handling.  */
  reductions *reds = s->reductions;
  int red = state_reduction_find (s, r);

  /* Print them if there are.  */
  if (reds->lookaheads && red != -1)
    {
      bitset_iterator biter;
      int k;
      xml_puts (out, level, "<lookaheads>");
      BITSET_FOR_EACH (biter, reds->lookaheads[red], k, 0)
        {
          xml_printf (out, level + 1, "<symbol>%s</symbol>",
                      xml_escape (symbols[k]->tag));
        }
      xml_puts (out, level, "</lookaheads>");
    }
}


/*---------------------.
| A state hash table.  |
`---------------------*/

/* Initial capacity of states hash table.  */
#define HT_INITIAL_CAPACITY 257

static struct hash_table *state_table = NULL;

/* Two states are equal if they have the same core items.  */
static inline bool
state_compare (state const *s1, state const *s2)
{
  if (s1->nitems != s2->nitems)
    return false;

  for (size_t i = 0; i < s1->nitems; ++i)
    if (s1->items[i] != s2->items[i])
      return false;

  return true;
}

static bool
state_comparator (void const *s1, void const *s2)
{
  return state_compare (s1, s2);
}

static inline size_t
state_hash (state const *s, size_t tablesize)
{
  /* Add up the state's item numbers to get a hash key.  */
  size_t key = 0;
  for (size_t i = 0; i < s->nitems; ++i)
    key += s->items[i];
  return key % tablesize;
}

static size_t
state_hasher (void const *s, size_t tablesize)
{
  return state_hash (s, tablesize);
}


/*-------------------------------.
| Create the states hash table.  |
`-------------------------------*/

void
state_hash_new (void)
{
  state_table = hash_xinitialize (HT_INITIAL_CAPACITY,
                                  NULL,
                                  state_hasher,
                                  state_comparator,
                                  NULL);
}


/*---------------------------------------------.
| Free the states hash table, not the states.  |
`---------------------------------------------*/

void
state_hash_free (void)
{
  hash_free (state_table);
}


/*-----------------------------------.
| Insert S in the state hash table.  |
`-----------------------------------*/

void
state_hash_insert (state *s)
{
  hash_xinsert (state_table, s);
}


/*------------------------------------------------------------------.
| Find the state associated to the CORE, and return it.  If it does |
| not exist yet, return NULL.                                       |
`------------------------------------------------------------------*/

state *
state_hash_lookup (size_t nitems, const item_index *core)
{
  size_t items_size = nitems * sizeof *core;
  state *probe = xmalloc (offsetof (state, items) + items_size);
  probe->nitems = nitems;
  memcpy (probe->items, core, items_size);
  state *entry = hash_lookup (state_table, probe);
  free (probe);
  return entry;
}


/*--------------------------------------------------------.
| Record S and all states reachable from S in REACHABLE.  |
`--------------------------------------------------------*/

static void
state_record_reachable_states (state *s, bitset reachable)
{
  if (bitset_test (reachable, s->number))
    return;
  bitset_set (reachable, s->number);
  for (int i = 0; i < s->transitions->num; ++i)
    if (!TRANSITION_IS_DISABLED (s->transitions, i))
      state_record_reachable_states (s->transitions->states[i], reachable);
}

void
state_remove_unreachable_states (state_number old_to_new[])
{
  state_number nstates_reachable = 0;
  bitset reachable = bitset_create (nstates, BITSET_FIXED);
  state_record_reachable_states (states[0], reachable);
  for (state_number i = 0; i < nstates; ++i)
    {
      if (bitset_test (reachable, states[i]->number))
        {
          states[nstates_reachable] = states[i];
          states[nstates_reachable]->number = nstates_reachable;
          old_to_new[i] = nstates_reachable++;
        }
      else
        {
          state_free (states[i]);
          old_to_new[i] = nstates;
        }
    }
  nstates = nstates_reachable;
  bitset_free (reachable);
}

/* All the decorated states, indexed by the state number.  */
state **states = NULL;


/*----------------------.
| Free all the states.  |
`----------------------*/

void
states_free (void)
{
  closure_free ();
  for (state_number i = 0; i < nstates; ++i)
    state_free (states[i]);
  free (states);
}
