--!syntax_pg
select upper('straße' COLLATE "de-DE-x-icu"), upper('straße'), upper('i' COLLATE "tr-TR-x-icu"), upper('i');
