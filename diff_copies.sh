#!/usr/bin/env bash
# diff_copies.sh — compare YDB solver/cbo copies against their YQL originals.
#
# Usage:
#   ./diff_copies.sh              — full diff for every pair
#   ./diff_copies.sh --stat-only  — one summary line per pair, no diff body
#
# Colour legend:
#   GREEN      — identical (bit-for-bit)
#   CYAN       — mechanical changes only (namespace/include/type-qualification renames)
#   RED        — substantive logic differences

STAT_ONLY=0
[[ "${1:-}" == "--stat-only" ]] && STAT_ONLY=1

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

SOLVER=ydb/core/kqp/opt/cbo/solver
YQLDQ=ydb/library/yql/dq/opt
CBO=ydb/core/kqp/opt/cbo
YQLCBO=yql/essentials/core/cbo

RED=$'\e[31m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'; CYAN=$'\e[36m'; BOLD=$'\e[1m'; RESET=$'\e[0m'

declare -a PAIRS=(
    # solver/ vs ydb/library/yql/dq/opt/
    "$SOLVER/bitset.h                            $YQLDQ/bitset.h"
    "$SOLVER/dq_opt_conflict_rules_collector.h   $YQLDQ/dq_opt_conflict_rules_collector.h"
    "$SOLVER/dq_opt_conflict_rules_collector.cpp $YQLDQ/dq_opt_conflict_rules_collector.cpp"
    "$SOLVER/dq_opt_dphyp_solver.h               $YQLDQ/dq_opt_dphyp_solver.h"
    "$SOLVER/dq_opt_join.h                       $YQLDQ/dq_opt_join.h"
    "$SOLVER/dq_opt_join.cpp                     $YQLDQ/dq_opt_join.cpp"
    "$SOLVER/dq_opt_join_cbo_factory.h           $YQLDQ/dq_opt_join_cbo_factory.h"
    "$SOLVER/dq_opt_join_cbo_factory.cpp         $YQLDQ/dq_opt_join_cbo_factory.cpp"
    "$SOLVER/dq_opt_join_cost_based.h            $YQLDQ/dq_opt_join_cost_based.h"
    "$SOLVER/dq_opt_join_cost_based.cpp          $YQLDQ/dq_opt_join_cost_based.cpp"
    "$SOLVER/dq_opt_join_hypergraph.h            $YQLDQ/dq_opt_join_hypergraph.h"
    "$SOLVER/dq_opt_join_tree_node.h             $YQLDQ/dq_opt_join_tree_node.h"
    "$SOLVER/dq_opt_join_tree_node.cpp           $YQLDQ/dq_opt_join_tree_node.cpp"
    "$SOLVER/dq_opt_make_join_hypergraph.h       $YQLDQ/dq_opt_make_join_hypergraph.h"
    "$SOLVER/dq_opt_stat.h                       $YQLDQ/dq_opt_stat.h"
    "$SOLVER/dq_opt_stat.cpp                     $YQLDQ/dq_opt_stat.cpp"
    "$SOLVER/dq_opt_predicate_selectivity.cpp    $YQLDQ/dq_opt_predicate_selectivity.cpp"
    "$SOLVER/dq_opt_stat_transformer_base.h      $YQLDQ/dq_opt_stat_transformer_base.h"
    "$SOLVER/dq_opt_stat_transformer_base.cpp    $YQLDQ/dq_opt_stat_transformer_base.cpp"
    # cbo/ vs yql/essentials/core/cbo/ and yql/essentials/core/
    "$CBO/kqp_statistics.h                       yql/essentials/core/yql_statistics.h"
    "$CBO/cbo_optimizer_new.h                    $YQLCBO/cbo_optimizer_new.h"
    "$CBO/cbo_optimizer_new.cpp                  $YQLCBO/cbo_optimizer_new.cpp"
    "$CBO/cbo_hints.cpp                          $YQLCBO/cbo_hints.cpp"
    "$CBO/cbo_interesting_orderings.h            $YQLCBO/cbo_interesting_orderings.h"
    "$CBO/cbo_interesting_orderings.cpp          $YQLCBO/cbo_interesting_orderings.cpp"
)

# Normalize a file for mechanical-change detection.
# Order matters: longest/most-specific patterns first, raw prefix stripping last.
normalize() {
    sed \
        -e 's|#include <yql/essentials/core/cbo/|#include <__CBO__/|g'  \
        -e 's|#include <ydb/core/kqp/opt/cbo/|#include <__CBO__/|g'    \
        -e 's|#include "cbo_optimizer_new\.h"|#include <__CBO__/cbo_optimizer_new.h>|g' \
        -e 's|#include "cbo_interesting_orderings\.h"|#include <__CBO__/cbo_interesting_orderings.h>|g' \
        -e 's|#include "dq_opt\.h"|#include <__DQ__/dq_opt.h>|g'       \
        -e 's|#include <ydb/library/yql/dq/opt/dq_opt\.h>|#include <__DQ__/dq_opt.h>|g' \
        -e 's|#include <ydb/library/yql/dq/opt/dq_opt_join_cost_based\.h>|#include <__CBO__/solver/dq_opt_join_cost_based.h>|g' \
        -e 's|#include <ydb/core/kqp/opt/cbo/solver/dq_opt_join_cost_based\.h>|#include <__CBO__/solver/dq_opt_join_cost_based.h>|g' \
        -e 's|#include <yql/essentials/core/yql_statistics\.h>|#include <__STATS__/yql_statistics.h>|g' \
        -e 's|#include "kqp_statistics\.h"|#include <__STATS__/yql_statistics.h>|g' \
        -e 's|#include <ydb/core/kqp/opt/cbo/kqp_statistics\.h>|#include <__STATS__/yql_statistics.h>|g' \
        -e 's|#include <yql/essentials/core/statistics/yql_statistics\.h>|#include <__STATS__/yql_statistics.h>|g' \
        -e 's/namespace NYql::NDq/namespace __NS__/g'      \
        -e 's/namespace NKikimr::NKqp/namespace __NS__/g'  \
        -e 's/namespace NYql/namespace __NS__/g'            \
        -e 's/namespace NKikimr/namespace __NS__/g'         \
        -e 's/using namespace NYql::NDq;/using namespace __NS__;/g'     \
        -e 's/using namespace NKikimr::NKqp;/using namespace __NS__;/g' \
        -e 's/using namespace NYql;/using namespace __NS__;/g'          \
        -e 's/using namespace NKikimr;/using namespace __NS__;/g'       \
        -e 's/NYql::NNodes:://g'   \
        -e 's/NYql::NDq:://g'      \
        -e 's/NYql::NLog:://g'     \
        -e 's/NKikimr::NKqp:://g'  \
        -e 's/NYql:://g'           \
        -e 's/NKikimr:://g'        \
        -e 's/NDq:://g'            \
        -e 's/NNodes:://g'         \
        -e 's/using namespace __NS__::NNodes;/using namespace NNodes;/g' \
        -e 's|#include "dq_opt_phy\.h"|#include <__DQ__/dq_opt_phy.h>|g' \
        -e 's|#include <ydb/library/yql/dq/opt/dq_opt_phy\.h>|#include <__DQ__/dq_opt_phy.h>|g' \
        -e 's/[[:space:]]*$//'     \
        "$1"
}

print_diff() {
    echo "$1" | sed \
        "s/^+[^+].*/$(printf '\e[32m')&$(printf '\e[0m')/; \
         s/^-[^-].*/$(printf '\e[31m')&$(printf '\e[0m')/"
    echo
}

count_changes() {
    local adds dels
    adds=$(echo "$1" | grep -c '^+[^+]' 2>/dev/null) || adds=0
    dels=$(echo "$1" | grep -c '^-[^-]' 2>/dev/null) || dels=0
    echo "+${adds}/-${dels}"
}

total=0; n_identical=0; n_mechanical=0; n_substantive=0; n_missing=0

resulting_diff="$(pwd)/cbo-to-ydb-diff"
rm "$resulting_diff"

for pair in "${PAIRS[@]}"; do
    read -r ydb_file orig_file <<< "$pair"
    total=$((total + 1))

    has_ydb=0; has_orig=0
    [[ -f "$ydb_file" ]]  && has_ydb=1
    [[ -f "$orig_file" ]] && has_orig=1

    label="${BOLD}$(basename "$ydb_file")${RESET}  | ${CYAN}${ydb_file}${RESET} <- ${CYAN}${orig_file}${RESET}"

    if (( !has_orig || !has_ydb )); then
        n_missing=$((n_missing + 1))
        echo "${YELLOW}MISSING${RESET}  $label"
        continue
    fi

    raw_diff=$(diff -u "$orig_file" "$ydb_file" || true)

    if [[ -z "$raw_diff" ]]; then
        n_identical=$((n_identical + 1))
        echo "${GREEN}IDENTICAL  ${RESET}$label"
        continue
    fi

    norm_diff=$(diff -u <(normalize "$orig_file") <(normalize "$ydb_file") || true)

    raw_stat=$(count_changes "$raw_diff")

    if [[ -z "$norm_diff" ]]; then
        n_mechanical=$((n_mechanical + 1))
        echo "${CYAN}MECHANICAL${RESET} (${raw_stat})  $label"
        if (( STAT_ONLY == 0 )); then
            print_diff "$raw_diff"
        fi
    else
        n_substantive=$((n_substantive + 1))
        norm_stat=$(count_changes "$norm_diff")
        echo "${RED}SUBSTANTIVE${RESET} (raw ${raw_stat}  norm ${norm_stat})  $label"
        if (( STAT_ONLY == 0 )); then
            echo "${BOLD}--- normalized diff (mechanical changes stripped) ---${RESET}"

            diff_dir="$(mktemp -d "/tmp/diffing-ydb-XXXXXX")"
            mkdir -p "$(dirname "$diff_dir/$orig_file")"
            mkdir -p "$(dirname "$diff_dir/$ydb_file")"
            normalize "$orig_file" > "$diff_dir/$orig_file"
            normalize "$ydb_file" > "$diff_dir/$ydb_file"

            difft_output="$(cd "$diff_dir" && difft "$orig_file" "$ydb_file" --color=always 2>&1 || true)"

            # Print to terminal
            echo "$difft_output"
            echo

            # Also append to collected diff file
            (
                echo
                echo
                echo "===================================================================================================="
                echo "${RED}SUBSTANTIVE${RESET} (raw ${raw_stat}  norm ${norm_stat})  $label"
                echo "$difft_output"
            ) >> "$resulting_diff"

            rm -rf "$diff_dir"

            echo "${BOLD}--- raw diff ---${RESET}"
            print_diff "$raw_diff"
        fi
    fi
done

echo
echo "${BOLD}Summary: $total pairs${RESET}"
echo "  ${GREEN}$n_identical identical${RESET}"
echo "  ${CYAN}$n_mechanical mechanical only (namespace/include/type-qualification renames)${RESET}"
echo "  ${RED}$n_substantive substantive differences${RESET}"
if (( n_missing > 0 )); then
    echo "  ${YELLOW}$n_missing missing${RESET}"
fi
