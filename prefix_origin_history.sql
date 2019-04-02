CREATE TABLE prefix_origin_history (
        origin bigint,
	prefix cidr,
        first_seen date DEFAULT current_date,
	history bigint,
        PRIMARY KEY (origin, prefix)
);
