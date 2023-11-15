-- from http://www.depesz.com/index.php/2010/04/19/getting-unique-elements/
CREATE TEMP TABLE articles (
    id int CONSTRAINT articles_pkey PRIMARY KEY,
    keywords text,
    title text UNIQUE NOT NULL,
    body text UNIQUE,
    created date
);
CREATE TEMP TABLE articles_in_category (
    article_id int,
    category_id int,
    changed date,
    PRIMARY KEY (article_id, category_id)
);
-- example from documentation
CREATE TEMP TABLE products (product_id int, name text, price numeric);
CREATE TEMP TABLE sales (product_id int, units int);
