select AsList('aaa', 'aaa'u);       -- List<String>
select AsList('aaa', '[1, 2, 3]'j); -- List<String>
select AsList('aaa', '[1; 2; 3]'y); -- List<String>

select AsList('aaa'u, 'aaa');        -- List<String>
select AsList('aaa'u, '[1, 2, 3]'j); -- List<Utf8>
select AsList('aaa'u, '[1; 2; 3]'y); -- List<String>

select AsList('[1, 2, 3]'j, 'aaa');        -- List<String>
select AsList('[1, 2, 3]'j, 'aaa'u);       -- List<Utf8>
select AsList('[1, 2, 3]'j, '[1; 2; 3]'y); -- List<String>

select AsList('[1; 2; 3]'y, 'aaa');        -- List<String>
select AsList('[1; 2; 3]'y, 'aaa'u);       -- List<String>
select AsList('[1; 2; 3]'y, '[1, 2, 3]'j); -- List<String>

