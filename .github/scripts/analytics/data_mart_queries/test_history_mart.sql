SELECT 
    build_type , 
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
    String::ReplaceAll(status_description, ';;', '\n') as status_description,
    owners,
    String::ReplaceAll(owners, 'TEAM:@ydb-platform/', '') as owner_team,
    String::SplitToList(pull,'_A')[0] as pull_raw,
    cast(COALESCE(String::SplitToList(pull,'_A')[1],"1") as Uint16) as attempt,
    (cast(pull as String) || '_' || SUBSTRING(cast(commit as String), 1, 8)) as pull_commit,
    CASE 
        WHEN String::Contains(test_name, 'sole chunk') OR String::Contains(test_name, 'chunk+chunk')  OR String::Contains(test_name, '] chunk') THEN TRUE
        ELSE FALSE
    END as with_cunks
   
FROM `test_results/test_runs_column` 

WHERE
    run_timestamp >= CurrentUtcDate() - 1*Interval("P1D")
    and String::Contains(test_name, '.flake8')  = FALSE
    and (CASE 
        WHEN String::Contains(test_name, 'sole chunk') OR String::Contains(test_name, 'chunk+chunk')  OR String::Contains(test_name, '] chunk') THEN TRUE
        ELSE FALSE
        END) = FALSE
