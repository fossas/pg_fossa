CREATE TYPE fossa_dependencies_graph_metadata AS (parent VARCHAR(255), child VARCHAR(255), depth INT);

CREATE OR REPLACE FUNCTION fossa_dependencies_maxdepth(locator VARCHAR(255), maxdepth INT) RETURNS SETOF fossa_dependencies_graph_metadata AS $$
DECLARE
  current_depth INT;
  current_results fossa_dependencies_graph_metadata;
  result fossa_dependencies_graph_metadata;
  results fossa_dependencies_graph_metadata[];
  seen VARCHAR[];
BEGIN
  FOR current_results IN SELECT d.parent AS parent, d.child AS child, 1 AS depth FROM "Dependencies" AS d WHERE d.parent=locator LOOP
    seen := ARRAY_APPEND(seen, current_results.child);
    results := ARRAY_APPEND(results, current_results);
  END LOOP;

  current_depth := 1;

  WHILE current_depth < maxdepth LOOP
    FOR result IN SELECT * FROM UNNEST(results) AS d WHERE d.depth = current_depth LOOP
      FOR current_results IN SELECT d.parent AS parent, d.child AS child, (result.depth + 1) AS depth FROM "Dependencies" AS d WHERE d.parent=result.child LOOP
        IF NOT ARRAY[current_results.child]::VARCHAR[] && seen::VARCHAR[] THEN
          seen := ARRAY_APPEND(seen, current_results.child);
          results := ARRAY_APPEND(results, current_results);
        END IF;
      END LOOP;
    END LOOP;

    current_depth := current_depth + 1;
  END LOOP;

  RETURN QUERY SELECT * FROM UNNEST(results);
END; $$
LANGUAGE PLPGSQL;
