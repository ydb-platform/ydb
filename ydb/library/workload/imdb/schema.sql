--!syntax_v1
-- Translated from https://github.com/gregrahn/join-order-benchmark/blob/master/schema.sql
-- PostgreSQL varchar length limits are not enforced by Utf8.

CREATE TABLE aka_name (
    id Int32 NOT NULL,
    person_id Int32 NOT NULL,
    name Utf8 NOT NULL,
    imdb_index Utf8,
    name_pcode_cf Utf8,
    name_pcode_nf Utf8,
    surname_pcode Utf8,
    md5sum Utf8,
    PRIMARY KEY (id)
);

CREATE TABLE aka_title (
    id Int32 NOT NULL,
    movie_id Int32 NOT NULL,
    title Utf8 NOT NULL,
    imdb_index Utf8,
    kind_id Int32 NOT NULL,
    production_year Int32,
    phonetic_code Utf8,
    episode_of_id Int32,
    season_nr Int32,
    episode_nr Int32,
    note Utf8,
    md5sum Utf8,
    PRIMARY KEY (id)
);

CREATE TABLE cast_info (
    id Int32 NOT NULL,
    person_id Int32 NOT NULL,
    movie_id Int32 NOT NULL,
    person_role_id Int32,
    note Utf8,
    nr_order Int32,
    role_id Int32 NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE char_name (
    id Int32 NOT NULL,
    name Utf8 NOT NULL,
    imdb_index Utf8,
    imdb_id Int32,
    name_pcode_nf Utf8,
    surname_pcode Utf8,
    md5sum Utf8,
    PRIMARY KEY (id)
);

CREATE TABLE comp_cast_type (
    id Int32 NOT NULL,
    kind Utf8 NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE company_name (
    id Int32 NOT NULL,
    name Utf8 NOT NULL,
    country_code Utf8,
    imdb_id Int32,
    name_pcode_nf Utf8,
    name_pcode_sf Utf8,
    md5sum Utf8,
    PRIMARY KEY (id)
);

CREATE TABLE company_type (
    id Int32 NOT NULL,
    kind Utf8 NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE complete_cast (
    id Int32 NOT NULL,
    movie_id Int32,
    subject_id Int32 NOT NULL,
    status_id Int32 NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE info_type (
    id Int32 NOT NULL,
    info Utf8 NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE keyword (
    id Int32 NOT NULL,
    keyword Utf8 NOT NULL,
    phonetic_code Utf8,
    PRIMARY KEY (id)
);

CREATE TABLE kind_type (
    id Int32 NOT NULL,
    kind Utf8 NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE link_type (
    id Int32 NOT NULL,
    link Utf8 NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE movie_companies (
    id Int32 NOT NULL,
    movie_id Int32 NOT NULL,
    company_id Int32 NOT NULL,
    company_type_id Int32 NOT NULL,
    note Utf8,
    PRIMARY KEY (id)
);

CREATE TABLE movie_info (
    id Int32 NOT NULL,
    movie_id Int32 NOT NULL,
    info_type_id Int32 NOT NULL,
    info Utf8 NOT NULL,
    note Utf8,
    PRIMARY KEY (id)
);

CREATE TABLE movie_info_idx (
    id Int32 NOT NULL,
    movie_id Int32 NOT NULL,
    info_type_id Int32 NOT NULL,
    info Utf8 NOT NULL,
    note Utf8,
    PRIMARY KEY (id)
);

CREATE TABLE movie_keyword (
    id Int32 NOT NULL,
    movie_id Int32 NOT NULL,
    keyword_id Int32 NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE movie_link (
    id Int32 NOT NULL,
    movie_id Int32 NOT NULL,
    linked_movie_id Int32 NOT NULL,
    link_type_id Int32 NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE name (
    id Int32 NOT NULL,
    name Utf8 NOT NULL,
    imdb_index Utf8,
    imdb_id Int32,
    gender Utf8,
    name_pcode_cf Utf8,
    name_pcode_nf Utf8,
    surname_pcode Utf8,
    md5sum Utf8,
    PRIMARY KEY (id)
);

CREATE TABLE person_info (
    id Int32 NOT NULL,
    person_id Int32 NOT NULL,
    info_type_id Int32 NOT NULL,
    info Utf8 NOT NULL,
    note Utf8,
    PRIMARY KEY (id)
);

CREATE TABLE role_type (
    id Int32 NOT NULL,
    role Utf8 NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE title (
    id Int32 NOT NULL,
    title Utf8 NOT NULL,
    imdb_index Utf8,
    kind_id Int32 NOT NULL,
    production_year Int32,
    imdb_id Int32,
    phonetic_code Utf8,
    episode_of_id Int32,
    season_nr Int32,
    episode_nr Int32,
    series_years Utf8,
    md5sum Utf8,
    PRIMARY KEY (id)
);
