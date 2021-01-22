CREATE OR REPLACE FUNCTION pg_rw_dependencies(
    OUT xid_in xid,
    OUT pid_in int,
    OUT xid_out xid,
    OUT pid_out int
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_rw_dependencies'
LANGUAGE C;
