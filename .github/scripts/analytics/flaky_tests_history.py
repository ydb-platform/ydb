#!/usr/bin/env python3

import argparse
import datetime
import os
import ydb
from ydb_wrapper import YDBWrapper


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--days-window', default=1, type=int, help='how many days back we collecting history')
    parser.add_argument('--build_type', default='relwithdebinfo', type=str, help='build types')
    parser.add_argument('--branch', default='main', type=str, help='branch')

    args, unknown = parser.parse_known_args()
    history_for_n_day = args.days_window
    build_type = args.build_type
    branch = args.branch
    
    print(f'🚀 Starting flaky_tests_history.py')
    print(f'   📅 Days window: {history_for_n_day}')
    print(f'   🔧 Build type: {build_type}')
    print(f'   🌿 Branch: {branch}')
    
    with YDBWrapper() as ydb_wrapper:
      
        # Получаем последнюю дату из истории
        table_path = f'test_results/analytics/flaky_tests_window_{history_for_n_day}_days'
        last_date_query = f"""
            select max(date_window) as max_date_window from `{table_path}`
            where build_type = '{build_type}' and branch = '{branch}'
        """
        
        try:
            results = ydb_wrapper.execute_scan_query(last_date_query)
            
            default_start_date = datetime.date(2024, 9, 1)
            base_date = datetime.date(1970, 1, 1)
            
            # YDB может вернуть date_window как int (дни с 1970-01-01) или datetime.date
            max_date_window = results[0].get('max_date_window') if results[0] else None
            if max_date_window is not None:
                # Конвертируем int в date если нужно
                if isinstance(max_date_window, int):
                    max_date_window = base_date + datetime.timedelta(days=max_date_window)
                # Теперь max_date_window это datetime.date, можно сравнивать
                if max_date_window > default_start_date:
                    last_datetime = max_date_window
                else:
                    last_datetime = default_start_date
            else:
                last_datetime = default_start_date
                
            last_date = last_datetime.strftime('%Y-%m-%d')
            print(f'📅 Last history date: {last_date}')
            
            # Создаем таблицу если не существует
            create_table_sql = f"""
            CREATE table IF NOT EXISTS `{table_path}` (
                `test_name` Utf8 NOT NULL,
                `suite_folder` Utf8 NOT NULL,
                `full_name` Utf8 NOT NULL,
                `date_window` Date NOT NULL,
                `build_type` Utf8 NOT NULL,
                `branch` Utf8 NOT NULL,
                `first_run` Timestamp,
                `last_run` Timestamp ,
                `owners` Utf8 ,
                `days_ago_window` Uint64 NOT NULL,
                `history` String,
                `history_class` String,
                `pass_count` Uint64,
                `mute_count` Uint64,
                `fail_count` Uint64,
                `skip_count` Uint64,
                PRIMARY KEY (`test_name`, `suite_folder`, `full_name`,date_window, build_type, branch)
            )
                PARTITION BY HASH(`full_name`,build_type,branch)
                WITH (STORE = COLUMN)
        """
            
            ydb_wrapper.create_table(table_path, create_table_sql)
            
            # Обрабатываем каждую дату
            today = datetime.date.today()
            date_list = [today - datetime.timedelta(days=x) for x in range((today - last_datetime).days+1)]
            
            print(f'📊 Processing {len(date_list)} dates from {last_date} to {today}')
            
            # Собираем все данные для bulk upsert
            all_prepared_rows = []
            
            for i, date in enumerate(sorted(date_list), 1):
                print(f'📅 Processing date {i}/{len(date_list)}: {date}')
                
                query_get_history = f"""
                select
                    full_name,
                    date_base,
                    history_list,
                    if(dist_hist = '','no_runs',dist_hist) as dist_hist,
                    suite_folder,
                    test_name,
                    build_type,
                    branch,
                    owners,
                    first_run,
                    last_run

                from (
                    select
                        full_name,
                        date_base,
                        AGG_LIST(status) as history_list ,
                        String::JoinFromList( ListSort(AGG_LIST_DISTINCT(status)) ,',') as dist_hist,
                        suite_folder,
                        test_name,
                        owners,
                        build_type,
                        branch,
                        min(run_timestamp) as first_run,
                        max(run_timestamp) as last_run
                    from (
                        select * from (
       
                            select distinct
                                full_name,
                                suite_folder,
                                test_name,
                                owners,
                                Date('{date}') as date_base,
                                '{build_type}' as  build_type,
                                '{branch}' as  branch
                            from  `test_results/analytics/testowners` 
                        ) as test_and_date
                        left JOIN (
                            
                            select
                                suite_folder || '/' || test_name as full_name,
                                run_timestamp,
                                status
                            from  `test_results/test_runs_column`
                            where
                                run_timestamp <= Date('{date}') + Interval("P1D")
                                and run_timestamp >= Date('{date}') - {history_for_n_day+1}*Interval("P1D") 

                                and job_name in (
                                    'Nightly-run',
                                    'Regression-run',
                                    'Regression-run_Large',
                                    'Regression-run_Small_and_Medium',
                                    'Regression-run_compatibility',
                                    'Regression-whitelist-run',
                                    'Postcommit_relwithdebinfo', 
                                    'Postcommit_asan'
                                ) 
                                and build_type = '{build_type}'
                                and branch = '{branch}'
                            order by full_name,run_timestamp desc
                            
                        ) as hist
                        ON test_and_date.full_name=hist.full_name
                    )
                    GROUP BY full_name,suite_folder,test_name,date_base,build_type,branch,owners
                )
            """
                
                results = ydb_wrapper.execute_scan_query(query_get_history)
                print(f'📈 History data captured, {len(results)} rows')
                
                # Подготавливаем данные для upsert
                for row in results:
                    row['count'] = dict(zip(list(row['history_list']), [list(
                        row['history_list']).count(i) for i in list(row['history_list'])]))   
                    all_prepared_rows.append({
                        'suite_folder': row['suite_folder'],
                        'test_name': row['test_name'],
                        'full_name': row['full_name'],
                        'date_window': row['date_base'],
                        'days_ago_window': history_for_n_day,
                        'build_type': row['build_type'],
                        'branch': row['branch'],
                        'first_run': row['first_run'],
                        'last_run': row['last_run'],
                        'history': ','.join(row['history_list']).encode('utf8'),
                        'history_class': row['dist_hist'],
                        'pass_count': row['count'].get('passed', 0),
                        'mute_count': row['count'].get('mute', 0),
                        'fail_count': row['count'].get('failure', 0),
                        'skip_count': row['count'].get('skipped', 0),
                    })
            
            # Вставляем все данные одним batch
            if all_prepared_rows:
                print(f'💾 Upserting {len(all_prepared_rows)} rows of history data')
                
                # Подготавливаем column types для bulk upsert
                column_types = (
                    ydb.BulkUpsertColumns()
                    .add_column("test_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("suite_folder", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("build_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("first_run", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                    .add_column("last_run", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                    .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("date_window", ydb.OptionalType(ydb.PrimitiveType.Date))
                    .add_column("days_ago_window", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                    .add_column("history", ydb.OptionalType(ydb.PrimitiveType.String))
                    .add_column("history_class", ydb.OptionalType(ydb.PrimitiveType.String))
                    .add_column("pass_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                    .add_column("mute_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                    .add_column("fail_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                    .add_column("skip_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                )
                
                ydb_wrapper.bulk_upsert_batches(table_path, all_prepared_rows, column_types, batch_size=1000)
                
                print('✅ History updated successfully')
            else:
                print('ℹ️  No data to upload')

        except Exception as e:
            print(f'❌ Script failed: {e}')
            raise


if __name__ == "__main__":
    main()