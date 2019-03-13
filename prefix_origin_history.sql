CREATE TABLE prefix_origin_history (
	prefix_origin varchar(100) PRIMARY KEY,
	first_seen date DEFAULT current_date,
	history bytea
);
