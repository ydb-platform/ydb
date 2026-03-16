pub mod utils;

use crate::classes::{FileComplexity, FunctionComplexity};
use ignore::Walk;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use pyo3::prelude::*;
use rayon::prelude::*;
use rustpython_parser::{
    ast::{self, Stmt},
    Parse,
};
use std::env;
use std::path;
use std::process;
use std::sync::{Arc, Mutex};
use std::thread;
use tempfile::tempdir;
use utils::{count_bool_ops, get_repo_name, is_decorator};

// Main function
#[pyfunction]
pub fn main(
    path: &str,
    is_dir: bool,
    is_url: bool,
    max_complexity: usize,
    file_level: bool,
) -> PyResult<Vec<FileComplexity>> {
    let mut ans: Vec<FileComplexity> = Vec::new();

    if is_url {
        let dir = tempdir()?;
        let repo_name = get_repo_name(path);

        env::set_current_dir(&dir)?;

        let cloning_done = Arc::new(Mutex::new(false));
        let cloning_done_clone = Arc::clone(&cloning_done);
        let path_clone = path.to_owned(); // Clone the path variable

        thread::spawn(move || {
            let _output = process::Command::new("git")
                .args(&["clone", &path_clone]) // Use the cloned path variable
                .output()
                .expect("failed to execute process");

            let mut done = cloning_done_clone.lock().unwrap();
            *done = true;
        });

        let pb = ProgressBar::new_spinner();
        pb.set_style(ProgressStyle::default_spinner());
        pb.set_message("Cloning repository...");

        while !*cloning_done.lock().unwrap() {
            pb.tick();
            thread::sleep(std::time::Duration::from_millis(100));
        }

        pb.finish_with_message("Repository cloned!");

        let repo_path = dir.path().join(&repo_name).to_str().unwrap().to_string();

        match evaluate_dir(&repo_path, max_complexity, file_level) {
            Ok(files_complexity) => ans = files_complexity,
            Err(e) => return Err(e),
        }

        dir.close()?;
    } else if is_dir {
        match evaluate_dir(path, max_complexity, file_level) {
            Ok(files_complexity) => ans = files_complexity,
            Err(e) => return Err(e),
        }
    } else {
        let parent_dir = path::Path::new(path).parent().unwrap().to_str().unwrap();

        match cognitive_complexity(path, parent_dir, max_complexity, file_level) {
            Ok(file_complexity) => ans.push(file_complexity),
            Err(e) => return Err(e),
        }
    }

    ans.iter_mut()
        .for_each(|f| f.functions.sort_by_key(|f| (f.complexity, f.name.clone())));

    ans.sort_by_key(|f| (f.path.clone(), f.file_name.clone(), f.complexity));
    Ok(ans)
}

fn evaluate_dir(
    path: &str,
    max_complexity: usize,
    file_level: bool,
) -> PyResult<Vec<FileComplexity>> {
    let mut files_paths: Vec<String> = Vec::new();

    let parent_dir = path::Path::new(path).parent().unwrap().to_str().unwrap();

    // Get all the python files in the directory
    for entry in Walk::new(path) {
        let entry = entry.unwrap();
        let file_path_str = entry.path().to_str().unwrap();

        if entry.path().extension().and_then(|s| s.to_str()) == Some("py") {
            files_paths.push(file_path_str.to_string());
        }
    }

    let pb = ProgressBar::new(files_paths.len() as u64);
    pb.set_style(
        indicatif::ProgressStyle::default_bar()
            .template(
                "{spiner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}",
            )
            .unwrap()
            .progress_chars("##-"),
    );

    let files_complexity_result: Result<Vec<FileComplexity>, PyErr> = files_paths
        .par_iter()
        .map(|file_path| {
            pb.inc(1);
            match cognitive_complexity(file_path, parent_dir, max_complexity, file_level) {
                Ok(file_complexity) => Ok(file_complexity),
                Err(e) => Err(e),
            }
        })
        .collect();

    pb.finish_with_message("Done!");

    match files_complexity_result {
        Ok(files_complexity) => Ok(files_complexity),
        Err(e) => Err(e),
    }
}

/// Calculate the cognitive complexity of a python file.
#[pyfunction]
pub fn cognitive_complexity(
    file_path: &str,
    base_path: &str,
    _max_complexity: usize,
    _file_level: bool,
) -> PyResult<FileComplexity> {
    let code = std::fs::read_to_string(file_path)?;
    let ast = ast::Suite::parse(&code, "<embedded>").unwrap();

    let mut complexity: u64 = 0;
    let path = path::Path::new(file_path);
    let file_name = path.file_name().unwrap().to_str().unwrap();
    let mut functions: Vec<FunctionComplexity> = Vec::new();

    let relative_path = path.strip_prefix(base_path).unwrap().to_str().unwrap();

    if _file_level {
        for node in ast.iter() {
            complexity += statement_cognitive_complexity(node.clone(), 0)?;
        }
    } else {
        let (f, c) = function_level_cognitive_complexity(&ast)?;
        functions = f;
        complexity = c;
    }

    Ok(FileComplexity {
        path: relative_path.to_string(),
        file_name: file_name.to_string(),
        complexity: complexity,
        functions: functions,
    })
}

fn function_level_cognitive_complexity(
    ast: &Vec<Stmt>,
) -> PyResult<(Vec<FunctionComplexity>, u64)> {
    let mut functions: Vec<FunctionComplexity> = Vec::new();
    let mut complexity: u64 = 0;

    for node in ast.iter() {
        match node {
            Stmt::FunctionDef(f) => {
                let function_complexity = FunctionComplexity {
                    name: f.name.to_string(),
                    complexity: statement_cognitive_complexity(node.clone(), 0)?,
                };
                functions.push(function_complexity);
            }
            Stmt::AsyncFunctionDef(f) => {
                let function_complexity = FunctionComplexity {
                    name: f.name.to_string(),
                    complexity: statement_cognitive_complexity(node.clone(), 0)?,
                };
                functions.push(function_complexity);
            }
            Stmt::ClassDef(c) => {
                for node in c.body.iter() {
                    match node {
                        Stmt::FunctionDef(f) => {
                            let function_complexity = FunctionComplexity {
                                name: format!("{}::{}", c.name, f.name),
                                complexity: statement_cognitive_complexity(node.clone(), 0)?,
                            };
                            functions.push(function_complexity);
                        }
                        Stmt::AsyncFunctionDef(f) => {
                            let function_complexity = FunctionComplexity {
                                name: format!("{}::{}", c.name, f.name),
                                complexity: statement_cognitive_complexity(node.clone(), 0)?,
                            };
                            functions.push(function_complexity);
                        }
                        _ => {}
                    }
                }
            }
            _ => {
                complexity += statement_cognitive_complexity(node.clone(), 0)?;
            }
        }
    }

    Ok((functions, complexity))
}

/// Calculate the cognitive complexity of a python statement
fn statement_cognitive_complexity(statement: Stmt, nesting_level: u64) -> PyResult<u64> {
    let mut complexity: u64 = 0;

    if is_decorator(statement.clone()) {
        match statement {
            Stmt::FunctionDef(f) => {
                return statement_cognitive_complexity(f.body[0].clone(), nesting_level);
            }
            _ => {}
        }
    }

    match statement {
        Stmt::FunctionDef(f) => {
            for node in f.body.iter() {
                match node {
                    Stmt::FunctionDef(..) | Stmt::AsyncFunctionDef(..) => {
                        complexity +=
                            statement_cognitive_complexity(node.clone(), nesting_level + 1)?;
                    }
                    _ => {
                        complexity += statement_cognitive_complexity(node.clone(), nesting_level)?;
                    }
                }
            }
        }
        Stmt::AsyncFunctionDef(f) => {
            for node in f.body.iter() {
                match node {
                    Stmt::FunctionDef(..) | Stmt::AsyncFunctionDef(..) => {
                        complexity +=
                            statement_cognitive_complexity(node.clone(), nesting_level + 1)?;
                    }
                    _ => {
                        complexity += statement_cognitive_complexity(node.clone(), nesting_level)?;
                    }
                }
            }
        }
        Stmt::ClassDef(c) => {
            for node in c.body.iter() {
                complexity += statement_cognitive_complexity(node.clone(), nesting_level)?;
            }
        }
        Stmt::Assign(a) => {
            complexity += count_bool_ops(*a.value, nesting_level);
        }
        Stmt::For(f) => {
            complexity += 1 + nesting_level;
            for node in f.body.iter() {
                complexity += statement_cognitive_complexity(node.clone(), nesting_level + 1)?;
            }

            complexity += count_bool_ops(*f.iter, nesting_level);
        }
        Stmt::While(w) => {
            complexity += 1 + nesting_level;
            complexity += count_bool_ops(*w.test, nesting_level);
            for node in w.body.iter() {
                complexity += statement_cognitive_complexity(node.clone(), nesting_level + 1)?;
            }
        }
        Stmt::If(i) => {
            complexity += 1 + nesting_level;
            complexity += count_bool_ops(*i.test, nesting_level);
            for node in i.body.iter() {
                complexity += statement_cognitive_complexity(node.clone(), nesting_level + 1)?;
            }

            let orelse_complexities: Vec<u64> = i
                .orelse
                .iter()
                .map(|node| statement_cognitive_complexity(node.clone(), nesting_level))
                .filter(|complexity| complexity.is_ok())
                .filter(|complexity| *complexity.as_ref().unwrap() > 0)
                .map(|complexity| complexity.unwrap())
                .collect();

            let orelse_complexity: u64 = orelse_complexities.iter().sum();

            if orelse_complexities.len() > 0 {
                complexity += orelse_complexity;
                complexity -= (nesting_level) * orelse_complexities.len() as u64;
            }
        }
        Stmt::Try(t) => {
            for node in t.body.iter() {
                complexity += statement_cognitive_complexity(node.clone(), nesting_level + 1)?;
            }

            for handler in t.handlers.iter() {
                complexity += 1;
                match handler {
                    ast::ExceptHandler::ExceptHandler(e) => {
                        for node in e.body.iter() {
                            complexity +=
                                statement_cognitive_complexity(node.clone(), nesting_level + 1)?;
                        }
                    }
                }
            }

            for node in t.orelse.iter() {
                complexity += statement_cognitive_complexity(node.clone(), nesting_level + 1)?;
            }

            for node in t.finalbody.iter() {
                complexity += statement_cognitive_complexity(node.clone(), nesting_level + 1)?;
            }

            if complexity > 0 {
                complexity += nesting_level;
            }
        }
        Stmt::Match(m) => {
            for case in m.cases.iter() {
                for node in case.body.iter() {
                    complexity += statement_cognitive_complexity(node.clone(), nesting_level + 1)?;
                }
            }

            if complexity > 0 {
                complexity += nesting_level;
            }
        }
        _ => {}
    };

    Ok(complexity)
}
