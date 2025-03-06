/* contrib/pg_table_bloat/pg_table_bloat--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_table_bloat" to load this file. \quit

--
-- get_bloat()
--

CREATE FUNCTION get_bloat(IN table_schema text, IN table_name text,
    OUT relname text,
    OUT num_dead_tuples int8,
    OUT dead_tuple_size int8,
    OUT dead_index int8)
AS 'MODULE_PATHNAME', 'get_bloat'
LANGUAGE C STRICT PARALLEL SAFE;

-- GRANT SELECT ON get_bloat(text) TO PUBLIC;
REVOKE EXECUTE ON FUNCTION get_bloat(TEXT, TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION get_bloat(TEXT, TEXT) TO pg_read_server_files;