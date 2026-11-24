use alloc::vec::Vec;
use core::{
    cell::Cell,
    cmp::{self, Ordering},
};

use bumpalo::Bump;

// Bounded package merge algorithm, based on the paper
// "A Fast and Space-Economical Algorithm for Length-Limited Coding
// Jyrki Katajainen, Alistair Moffat, Andrew Turpin".

struct Thing<'a> {
    node_arena: &'a Bump,
    leaves: Vec<Leaf>,
    lists: [List<'a>; 15],
}

struct Node<'a> {
    weight: usize,
    count: usize,
    tail: Cell<Option<&'a Node<'a>>>,
}

struct Leaf {
    weight: usize,
    count: usize,
}
impl PartialEq for Leaf {
    fn eq(&self, other: &Self) -> bool {
        self.weight == other.weight
    }
}
impl Eq for Leaf {}
impl Ord for Leaf {
    fn cmp(&self, other: &Self) -> Ordering {
        self.weight.cmp(&other.weight)
    }
}
impl PartialOrd for Leaf {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy)]
struct List<'arena> {
    lookahead0: &'arena Node<'arena>,
    lookahead1: &'arena Node<'arena>,
}

/// Calculates the bitlengths for the Huffman tree, based on the counts of each
/// symbol.
pub fn length_limited_code_lengths(frequencies: &[usize], max_bits: usize) -> Vec<u32> {
    let num_freqs = frequencies.len();
    assert!(num_freqs <= 288);

    // Count used symbols and place them in the leaves.
    let mut leaves = frequencies
        .iter()
        .enumerate()
        .filter_map(|(i, &freq)| {
            (freq != 0).then_some(Leaf {
                weight: freq,
                count: i,
            })
        })
        .collect::<Vec<_>>();

    let num_symbols = leaves.len();

    // Short circuit some special cases

    // TODO:
    // if ((1 << maxbits) < numsymbols) {
    //   free(leaves);
    //   return 1;  /* Error, too few maxbits to represent symbols. */
    // }

    if num_symbols <= 2 {
        // The symbols for the non-zero frequencies can be represented
        // with zero or one bits.
        let mut bit_lengths = vec![0; num_freqs];
        for i in 0..num_symbols {
            bit_lengths[leaves[i].count] = 1;
        }
        return bit_lengths;
    }

    // Sort the leaves from least frequent to most frequent.
    leaves.sort();

    let max_bits = cmp::min(num_symbols - 1, max_bits);
    assert!(max_bits <= 15);

    let arena_capacity = max_bits * 2 * num_symbols;
    let node_arena = Bump::with_capacity(arena_capacity);

    let node0 = node_arena.alloc(Node {
        weight: leaves[0].weight,
        count: 1,
        tail: Cell::new(None),
    });

    let node1 = node_arena.alloc(Node {
        weight: leaves[1].weight,
        count: 2,
        tail: Cell::new(None),
    });

    let lists = [List {
        lookahead0: node0,
        lookahead1: node1,
    }; 15];

    let mut thing = Thing {
        node_arena: &node_arena,
        leaves,
        lists,
    };

    // In the last list, 2 * num_symbols - 2 active chains need to be created. Two
    // are already created in the initialization. Each boundary_pm run creates one.
    let num_boundary_pm_runs = 2 * num_symbols - 4;
    for _ in 0..num_boundary_pm_runs - 1 {
        thing.boundary_pm(max_bits - 1);
    }

    thing.boundary_pm_final(max_bits - 1);

    thing.extract_bit_lengths(max_bits, num_freqs)
}

impl Thing<'_> {
    fn boundary_pm(&mut self, index: usize) {
        let num_symbols = self.leaves.len();

        let last_count = self.lists[index].lookahead1.count; // Count of last chain of list.

        if index == 0 && last_count >= num_symbols {
            return;
        }

        self.lists[index].lookahead0 = self.lists[index].lookahead1;

        if index == 0 {
            // New leaf node in list 0.
            let new_chain = self.node_arena.alloc(Node {
                weight: self.leaves[last_count].weight,
                count: last_count + 1,
                tail: self.lists[index].lookahead0.tail.clone(),
            });
            self.lists[index].lookahead1 = new_chain;
        } else {
            let weight_sum = {
                let previous_list = &self.lists[index - 1];
                previous_list.lookahead0.weight + previous_list.lookahead1.weight
            };

            if last_count < num_symbols && weight_sum > self.leaves[last_count].weight {
                // New leaf inserted in list, so count is incremented.
                let new_chain = self.node_arena.alloc(Node {
                    weight: self.leaves[last_count].weight,
                    count: last_count + 1,
                    tail: self.lists[index].lookahead0.tail.clone(),
                });
                self.lists[index].lookahead1 = new_chain;
            } else {
                let new_chain = self.node_arena.alloc(Node {
                    weight: weight_sum,
                    count: last_count,
                    tail: Cell::new(Some(self.lists[index - 1].lookahead1)),
                });
                self.lists[index].lookahead1 = new_chain;

                // Two lookahead chains of previous list used up, create new ones.
                self.boundary_pm(index - 1);
                self.boundary_pm(index - 1);
            }
        }
    }

    fn boundary_pm_final(&mut self, index: usize) {
        let num_symbols = self.leaves.len();

        // Count of last chain of list.
        let last_count = self.lists[index].lookahead1.count;

        let weight_sum = {
            let previous_list = &self.lists[index - 1];
            previous_list.lookahead0.weight + previous_list.lookahead1.weight
        };

        if last_count < num_symbols && weight_sum > self.leaves[last_count].weight {
            let new_chain = self.node_arena.alloc(Node {
                weight: 0,
                count: last_count + 1,
                tail: self.lists[index].lookahead1.tail.clone(),
            });
            self.lists[index].lookahead1 = new_chain;
        } else {
            self.lists[index]
                .lookahead1
                .tail
                .set(Some(self.lists[index - 1].lookahead1));
        }
    }

    fn extract_bit_lengths(&self, max_bits: usize, num_freqs: usize) -> Vec<u32> {
        let mut counts = [0; 16];
        let mut end = 16;
        let mut ptr = 15;
        let mut value = 1;

        let mut node = self.lists[max_bits - 1].lookahead1;

        end -= 1;
        counts[end] = node.count;

        while let Some(tail) = node.tail.get() {
            end -= 1;
            counts[end] = tail.count;
            node = tail;
        }

        let mut val = counts[15];

        let mut bit_lengths = vec![0; num_freqs];

        while ptr >= end {
            while val > counts[ptr - 1] {
                bit_lengths[self.leaves[val - 1].count] = value;
                val -= 1;
            }
            ptr -= 1;
            value += 1;
        }

        bit_lengths
    }
}

// fn next_leaf(lists: &mut [List], leaves: &[Leaf], current_list_index: usize) {
//     let mut current_list = &mut lists[current_list_index];
//
//     // The next leaf goes next; counting itself makes the leaf_count increase by one.
//     current_list.lookahead1.weight = leaves[current_list.next_leaf_index].weight;
//     current_list.lookahead1.leaf_counts.clear();
//     current_list.lookahead1.leaf_counts.extend_from_slice(&current_list.lookahead0.leaf_counts);
//     let last_index = current_list.lookahead1.leaf_counts.len() - 1;
//     current_list.lookahead1.leaf_counts[last_index] += 1;
//     current_list.next_leaf_index += 1;
// }

// fn next_tree(weight_sum: usize, lists: &mut [List], leaves: &[Leaf], current_list_index: usize) {
//     {
//         let (head, tail) = lists.split_at_mut(current_list_index);
//         let prev_list = head.last_mut().unwrap();
//         let current_list = tail.first_mut().unwrap();
//
//         let previous_list_leaf_counts = &prev_list.lookahead1.leaf_counts;
//
//         // Make a tree from the lookaheads from the previous list; that goes next.
//         // This is not a leaf node, so the leaf count stays the same.
//         current_list.lookahead1.weight = weight_sum;
//         current_list.lookahead1.leaf_counts.clear();
//
//         current_list.lookahead1.leaf_counts.extend_from_slice(previous_list_leaf_counts);
//         current_list.lookahead1.leaf_counts.push(*current_list.lookahead0.leaf_counts.last().unwrap());
//     }
//
//     // The previous list needs two new lookahead nodes.
//     boundary_pm(lists, leaves, current_list_index - 1);
//     boundary_pm(lists, leaves, current_list_index - 1);
// }

// fn boundary_pm_toplevel(lists: &mut [List], leaves: &[Leaf]) {
//     let last_index = lists.len() - 1;
//     boundary_pm(lists, leaves, last_index);
// }

// fn boundary_pm(lists: &mut [List], leaves: &[Leaf], current_list_index: usize) {
//     let next_leaf_index = lists[current_list_index].next_leaf_index;
//     let num_symbols = leaves.len();
//
//     if current_list_index == 0 && next_leaf_index == num_symbols {
//         // We've added all the leaves to the lowest list, so we're done here
//         return;
//     }
//
//     mem::swap(&mut lists[current_list_index].lookahead0, &mut lists[current_list_index].lookahead1);
//
//     if current_list_index == 0 {
//         lowest_list(lists, leaves);
//     } else {
//         // We're at a list other than the lowest list.
//         let weight_sum = {
//             let previous_list = &lists[current_list_index - 1];
//             previous_list.lookahead0.weight + previous_list.lookahead1.weight
//         };
//
//         if next_leaf_index < num_symbols && weight_sum > leaves[next_leaf_index].weight {
//             next_leaf(lists, leaves, current_list_index);
//         } else {
//             next_tree(weight_sum, lists, leaves, current_list_index);
//         }
//     }
// }

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_from_paper_3() {
        let input = [1, 1, 5, 7, 10, 14];
        let output = length_limited_code_lengths(&input, 3);
        let answer = vec![3, 3, 3, 3, 2, 2];
        assert_eq!(output, answer);
    }

    #[test]
    fn test_from_paper_4() {
        let input = [1, 1, 5, 7, 10, 14];
        let output = length_limited_code_lengths(&input, 4);
        let answer = vec![4, 4, 3, 2, 2, 2];
        assert_eq!(output, answer);
    }

    #[test]
    fn max_bits_7() {
        let input = [252, 0, 1, 6, 9, 10, 6, 3, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let output = length_limited_code_lengths(&input, 7);
        let answer = vec![1, 0, 6, 4, 3, 3, 3, 5, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(output, answer);
    }

    #[test]
    fn max_bits_15() {
        let input = [
            0, 0, 0, 0, 0, 0, 18, 0, 6, 0, 12, 2, 14, 9, 27, 15, 23, 15, 17, 8, 1, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
        ];
        let output = length_limited_code_lengths(&input, 15);
        let answer = vec![
            0, 0, 0, 0, 0, 0, 3, 0, 5, 0, 4, 6, 4, 4, 3, 4, 3, 3, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        assert_eq!(output, answer);
    }

    #[test]
    fn no_frequencies() {
        let input = [0, 0, 0, 0, 0];
        let output = length_limited_code_lengths(&input, 7);
        let answer = vec![0, 0, 0, 0, 0];
        assert_eq!(output, answer);
    }

    #[test]
    fn only_one_frequency() {
        let input = [0, 10, 0];
        let output = length_limited_code_lengths(&input, 7);
        let answer = vec![0, 1, 0];
        assert_eq!(output, answer);
    }

    #[test]
    fn only_two_frequencies() {
        let input = [0, 0, 0, 0, 252, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let output = length_limited_code_lengths(&input, 7);
        let answer = [0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(output, answer);
    }
}
