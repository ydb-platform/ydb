use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use rsvg::bench_only::Lexer;
use rsvg::bench_only::PathBuilder;

static INPUT: &str = "M10 20 C 30,40 50 60-70,80,90 100,110 120,130,140";

static BYTES: &[u8; 49] = b"M10 20 C 30,40 50 60-70,80,90 100,110 120,130,140";

static SLICE_EDGES: [(usize, usize); 14] = [
    (1, 3),
    (4, 6),
    (9, 11),
    (12, 14),
    (15, 17),
    (18, 20),
    (20, 23),
    (24, 26),
    (27, 29),
    (30, 33),
    (34, 37),
    (38, 41),
    (42, 45),
    (46, 49),
];

fn lex_path(input: &str) {
    let lexer = Lexer::new(black_box(input));

    for (_pos, _token) in lexer {
        // no-op
    }
}

fn path_parser(c: &mut Criterion) {
    c.bench_function("parse path into builder", |b| {
        let input = black_box(INPUT);

        b.iter(|| {
            let mut builder = PathBuilder::default();
            let _ = builder.parse(input);
        });
    });

    c.bench_function("lex str", |b| {
        let input = black_box(INPUT);

        b.iter(|| {
            lex_path(input);
        });
    });

    // look at how much time *just* the parse::<i32> part of the lexer should be taking...
    c.bench_function("std i32 parse (bytes)", |b| {
        let input = black_box(BYTES);
        let slice_boundaries = black_box(SLICE_EDGES);

        b.iter(|| {
            for (a, b) in slice_boundaries.iter() {
                let a: usize = *a;
                let b: usize = *b;
                unsafe {
                    let _ = std::str::from_utf8_unchecked(&input[a..b])
                        .parse::<i32>()
                        .unwrap();
                }
            }
        });
    });
}

criterion_group!(benches, path_parser);
criterion_main!(benches);
