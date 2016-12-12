CREATE TYPE fossa_dependencies_graph_metadata AS (child VARCHAR(255), depth INT);

CREATE OR REPLACE FUNCTION fossa_dependencies_maxdepth(locator VARCHAR(255), maxdepth INT) RETURNS SETOF fossa_dependencies_graph_metadata AS $$
DECLARE
  current_depth INT := 1;
BEGIN
  CREATE TEMP TABLE results ON COMMIT DROP AS SELECT d.child AS child, 1 AS depth FROM "Dependencies" AS d WHERE d.parent=locator;

  WHILE current_depth < maxdepth LOOP
    WITH f AS (
      SELECT d.child AS child, (results.depth + 1) AS depth FROM "Dependencies" AS d INNER JOIN results ON d.parent=results.child WHERE results.depth=current_depth AND d.child NOT IN ( SELECT child FROM results WHERE child IS NOT NULL)
    ) INSERT INTO results SELECT * FROM f;
    current_depth := current_depth + 1;
  END LOOP;

  RETURN QUERY SELECT DISTINCT ON (child) * FROM results;
END; $$
LANGUAGE PLPGSQL;
