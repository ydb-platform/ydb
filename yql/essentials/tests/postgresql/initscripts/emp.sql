CREATE TABLE emp (
	name            text,
        age             int4,
        -- TODO: uncomment after point type gets supported
        --location        point
        salary          int4,
        manager         name
);

INSERT INTO emp VALUES
('sharon',	25,	/*(15,12),*/	1000,	'sam'),
('sam',		30,	/*(10,5),*/	2000,	'bill'),
('bill',	20,	/*(11,10),*/	1000,	'sharon');

