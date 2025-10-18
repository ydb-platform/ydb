SELECT 
        build_type, 
        job_name, 
        job_id, 
        commit, 
        branch, 
        pull, 
        run_timestamp, 
        test_id, 
        suite_folder, 
        test_name,
        cast(suite_folder || '/' || test_name as UTF8)  as full_name, 
        duration,
        status,
        cast(String::ReplaceAll(status_description, ';;', '\n')as Utf8) as status_description ,
        owners
    FROM `test_results/test_runs_column`  as all_data
    WHERE
    run_timestamp >= CurrentUtcDate() - Interval("P1D")
    and String::Contains(test_name, '.flake8')  = FALSE
    and (CASE 
        WHEN String::Contains(test_name, 'sole chunk') OR String::Contains(test_name, 'chunk+chunk')  OR String::Contains(test_name, '] chunk') THEN TRUE
        ELSE FALSE
        END) = FALSE
    and (branch = 'main' or branch like 'stable-%')
