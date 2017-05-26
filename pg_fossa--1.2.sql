CREATE TYPE fossa_dependencies_graph_metadata AS (child VARCHAR(255), depth INT, excludes VARCHAR[]);

CREATE OR REPLACE FUNCTION array_intersect(a1 VARCHAR[], a2 VARCHAR[]) RETURNS VARCHAR[] as $$
BEGIN
    IF a1 is NULL THEN
        RETURN a2;
    ELSEIF a2 is NULL THEN
        RETURN a1;
    END IF;
    RETURN ( SELECT ARRAY_AGG(e) FROM (SELECT UNNEST(a1) INTERSECT SELECT UNNEST(a2)) as dt(e) );
END; $$
LANGUAGE PLPGSQL;

DROP AGGREGATE IF EXISTS array_intersect_agg(VARCHAR[]);
CREATE AGGREGATE array_intersect_agg(VARCHAR[]) (
    SFUNC = array_intersect,
    STYPE = VARCHAR[]
);

CREATE OR REPLACE FUNCTION fossa_dependencies_maxdepth(locator VARCHAR(255), maxdepth INT) RETURNS SETOF fossa_dependencies_graph_metadata AS $$
DECLARE
  current_depth INT := 1;
BEGIN
  CREATE TEMP TABLE results ON COMMIT DROP AS SELECT d.child AS child, 1 AS depth, d.transitive_excludes AS excludes FROM "Dependencies" AS d WHERE d.parent=locator;

  WHILE current_depth < maxdepth LOOP
    WITH f AS ( SELECT d.child AS child,
      (r.depth + 1) AS depth,
      (ARRAY(SELECT DISTINCT UNNEST(d.transitive_excludes || r.excludes) ORDER BY 1)) AS excludes
      FROM "Dependencies" AS d
      INNER JOIN results AS r ON d.parent=r.child
      WHERE r.depth=current_depth
        AND d.child NOT LIKE ALL(r.excludes)
        AND d.child NOT IN (
          SELECT child
          FROM results AS rtmp
          WHERE rtmp.child IS NOT NULL
        )) INSERT INTO results SELECT child, MIN(depth), array_intersect_agg(excludes) FROM f GROUP BY child;
    RAISE NOTICE 'Current table size %', ( SELECT COUNT(*) FROM results );
    current_depth := current_depth + 1;
  END LOOP;

  RETURN QUERY SELECT DISTINCT ON (child) * FROM results;
END; $$
LANGUAGE PLPGSQL;

COMMENT ON FUNCTION fossa_dependencies_maxdepth(locator VARCHAR(255), maxdepth INT) IS 'pg_fossa version 1.2';
