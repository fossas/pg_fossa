DROP FUNCTION IF EXISTS fossa_version();
CREATE OR REPLACE FUNCTION fossa_version() RETURNS TEXT as $$
BEGIN
  RETURN '1.8';
END; $$ LANGUAGE PLPGSQL IMMUTABLE;

DROP TYPE IF EXISTS fossa_edge CASCADE;
CREATE TYPE fossa_edge AS (
  parent TEXT,
  child TEXT,
  resolved BOOLEAN,
  origin_paths TEXT[],
  excludes TEXT[],
  tags TEXT[],
  manual BOOLEAN,
  submodule BOOLEAN,
  depth INT
);

DROP TYPE IF EXISTS fossa_node CASCADE;
CREATE TYPE fossa_node AS (
  node TEXT,
  unresolved_locators TEXT[],
  origin_paths TEXT[],
  excludes TEXT[],
  tags TEXT[],
  manual BOOLEAN,
  submodule BOOLEAN,
  depth INT
);

DROP TYPE IF EXISTS fossa_node_count CASCADE;
CREATE TYPE fossa_node_count AS (
  node TEXT,
  count INT
);

DROP FUNCTION IF EXISTS array_symmetric_difference(a1 VARCHAR[], a2 VARCHAR[]) CASCADE;
CREATE OR REPLACE FUNCTION array_symmetric_difference(a1 TEXT[], a2 TEXT[]) RETURNS TEXT[] as $$
BEGIN
  RETURN ARRAY( SELECT DISTINCT e FROM (
    SELECT * FROM ( SELECT UNNEST(a1) EXCEPT SELECT UNNEST(a2) ) AS t1
    UNION ALL
    SELECT * FROM ( SELECT UNNEST(a2) EXCEPT SELECT UNNEST(a1) ) AS t1
  ) dt(e) ORDER BY 1 );
END; $$ LANGUAGE PLPGSQL IMMUTABLE;

DROP FUNCTION IF EXISTS array_sort_unique(a VARCHAR[]) CASCADE;
CREATE OR REPLACE FUNCTION array_sort_unique(a TEXT[]) RETURNS TEXT[] as $$
BEGIN
  RETURN ARRAY(SELECT DISTINCT UNNEST(a) ORDER BY 1);
END; $$ LANGUAGE PLPGSQL IMMUTABLE;

DROP FUNCTION IF EXISTS array_union(a1 VARCHAR[], a2 VARCHAR[]) CASCADE;
CREATE OR REPLACE FUNCTION array_union(a1 TEXT[], a2 TEXT[]) RETURNS TEXT[] as $$
BEGIN
  IF a1 IS NULL AND a2 IS NOT NULL THEN
    RETURN a2;
  ELSEIF a2 IS NULL AND a1 IS NOT NULL THEN
    RETURN a1;
  END IF;
  RETURN ( SELECT COALESCE(ARRAY_AGG(e), '{}'::TEXT[]) FROM (SELECT UNNEST(a1) UNION SELECT UNNEST(a2)) AS dt(e) );
END; $$ LANGUAGE PLPGSQL IMMUTABLE;

DROP AGGREGATE IF EXISTS array_union_agg(TEXT[]) CASCADE;
CREATE AGGREGATE array_union_agg(TEXT[]) (
  SFUNC = array_union,
  STYPE = TEXT[]
);

DROP FUNCTION IF EXISTS array_intersect(a1 VARCHAR[], a2 VARCHAR[]) CASCADE;
CREATE OR REPLACE FUNCTION array_intersect(a1 TEXT[], a2 TEXT[]) RETURNS TEXT[] as $$
BEGIN
  IF a1 IS NULL AND a2 IS NOT NULL THEN
    RETURN a2;
  ELSEIF a2 IS NULL AND a1 IS NOT NULL THEN
    RETURN a1;
  END IF;
  RETURN ( SELECT COALESCE(ARRAY_AGG(e), '{}'::TEXT[]) FROM (SELECT UNNEST(a1) INTERSECT SELECT UNNEST(a2)) AS dt(e) );
END; $$ LANGUAGE PLPGSQL IMMUTABLE;

DROP AGGREGATE IF EXISTS array_intersect_agg(TEXT[]) CASCADE;
CREATE AGGREGATE array_intersect_agg(TEXT[]) (
  SFUNC = array_intersect,
  STYPE = TEXT[]
);

DROP FUNCTION IF EXISTS fossa_child(locator VARCHAR, unresolved_locator VARCHAR) CASCADE;
CREATE OR REPLACE FUNCTION fossa_child(locator TEXT, unresolved_locator TEXT) RETURNS TEXT as $$
BEGIN
  RETURN CASE WHEN fossa_resolved(locator) THEN locator ELSE unresolved_locator END;
END; $$ LANGUAGE PLPGSQL IMMUTABLE;

DROP FUNCTION IF EXISTS fossa_resolved(locator VARCHAR) CASCADE;
CREATE OR REPLACE FUNCTION fossa_resolved(locator TEXT) RETURNS BOOLEAN as $$
BEGIN
  RETURN locator != 'NULL';
END; $$ LANGUAGE PLPGSQL IMMUTABLE;

DROP FUNCTION IF EXISTS fossa_edges(locator VARCHAR(255), filter_tags VARCHAR[], filter_excludes VARCHAR[], filter_origin_paths VARCHAR[], filter_all_origin_paths VARCHAR[], filter_unresolved BOOLEAN, maxdepth INT) CASCADE;
CREATE OR REPLACE FUNCTION fossa_edges(locator TEXT, filter_tags TEXT[], filter_excludes TEXT[], filter_origin_paths TEXT[], filter_all_origin_paths TEXT[], filter_unresolved BOOLEAN, maxdepth INT) RETURNS fossa_edge[] AS $$
DECLARE
  current_depth INT := 2;
  results fossa_edge[] := ARRAY(SELECT CAST(ROW(
      d.parent,
      fossa_child(d.child, d.unresolved_locator),
      fossa_resolved(d.child),
      d.origin_paths,
      d.transitive_excludes,
      d.tags,
      d.manual,
      d.is_submodule,
      1
    ) AS fossa_edge)
    FROM "Dependencies" AS d
    WHERE d.parent=locator
      AND d.parent != d.child
      AND (filter_excludes IS NULL OR d.child != ALL(filter_excludes))
      AND (NOT filter_unresolved OR fossa_resolved(d.child))
      AND (d.manual OR filter_tags IS NULL OR d.child NOT LIKE 'mvn+%' OR d.unresolved_locator NOT LIKE 'mvn+%' OR d.child LIKE 'mvn+%' AND d.tags && filter_tags OR d.unresolved_locator LIKE 'mvn+%' AND d.tags && filter_tags));
  intermediate_edges fossa_edge[] := results;
  working_edges fossa_edge[] := results;
BEGIN
  WHILE current_depth <= maxdepth LOOP
    EXIT WHEN ARRAY_LENGTH(working_edges, 1) = 0;
    EXIT WHEN ARRAY_LENGTH(working_edges, 1) IS NULL;

    intermediate_edges := ARRAY(
      SELECT CAST(ROW(
        d.parent,
        fossa_child(d.child, d.unresolved_locator),
        fossa_resolved(d.child),
        array_union(array_union_agg(d.origin_paths), array_union_agg((w).origin_paths)),
        array_intersect(array_intersect_agg(d.transitive_excludes), array_intersect_agg((w).excludes)),
        array_union(array_union_agg(d.tags), array_union_agg((w).tags)),
        bool_or(d.manual),
        bool_or(d.is_submodule),
        min(current_depth)
      ) AS fossa_edge)
      FROM "Dependencies" AS d, UNNEST(working_edges) AS w
      WHERE d.parent = (w).child
        AND d.parent != d.child
        AND (filter_origin_paths IS NULL OR filter_origin_paths && d.origin_paths)
        AND NOT d.optional
        AND (filter_excludes IS NULL OR d.child != ALL(filter_excludes))
        AND (NOT filter_unresolved OR fossa_resolved(d.child))
        AND ((w).excludes IS NULL OR d.child NOT LIKE ALL((w).excludes))
        AND (d.manual OR filter_tags IS NULL OR d.child NOT LIKE 'mvn+%' OR d.unresolved_locator NOT LIKE 'mvn+%' OR d.child LIKE 'mvn+%' AND d.tags && filter_tags OR d.unresolved_locator LIKE 'mvn+%' AND d.tags && filter_tags)
      GROUP BY d.parent, fossa_child(d.child, d.unresolved_locator), fossa_resolved(d.child)
    );

    results := ARRAY(
      SELECT CAST(ROW(
        (r).parent,
        (r).child,
        (r).resolved,
        array_union(array_union_agg((r).origin_paths), array_union_agg((w).origin_paths)),
        array_intersect(array_intersect_agg((r).excludes), array_intersect_agg((w).excludes)),
        array_union(array_union_agg((r).tags), array_union_agg((w).tags)),
        bool_or((r).manual),
        bool_or((r).submodule),
        min((r).depth)
      ) AS fossa_edge)
      FROM UNNEST(results) AS r
      LEFT JOIN UNNEST(intermediate_edges) AS w
        ON (r).parent = (w).parent
        AND (r).child = (w).child
        AND (r).resolved = (w).resolved
      GROUP BY (r).parent, (r).child, (r).resolved
    );

    working_edges := ARRAY(
      SELECT w FROM UNNEST(intermediate_edges) AS w
      WHERE ((w).parent, (w).child, (w).resolved) NOT IN ( SELECT (r).parent, (r).child, (r).resolved FROM UNNEST(results) AS r )
    );

    results := results || working_edges;

    current_depth := current_depth + 1;
  END LOOP;

  RETURN results;
END; $$ LANGUAGE PLPGSQL STABLE;

DROP FUNCTION IF EXISTS fossa_edges(locator VARCHAR(255)) CASCADE;
CREATE OR REPLACE FUNCTION fossa_edges(locator TEXT) RETURNS fossa_edge[] AS $$
BEGIN
  RETURN fossa_edges(locator, NULL, NULL, NULL, NULL, FALSE, 5);
END; $$ LANGUAGE PLPGSQL;


DROP FUNCTION IF EXISTS fossa_dependencies(locator VARCHAR, filter_tags VARCHAR[], filter_excludes VARCHAR[], filter_origin_paths VARCHAR[], filter_all_origin_paths VARCHAR[], filter_unresolved BOOLEAN, maxdepth INT) CASCADE;
CREATE OR REPLACE FUNCTION fossa_dependencies(locator TEXT, filter_tags TEXT[], filter_excludes TEXT[], filter_origin_paths TEXT[], filter_all_origin_paths TEXT[], filter_unresolved BOOLEAN, maxdepth INT) RETURNS fossa_node[] AS $$
DECLARE
  current_depth INT := 2;
  results fossa_node[] := ARRAY(SELECT CAST(ROW(
      d.child,
      ARRAY[d.unresolved_locator],
      d.origin_paths,
      d.transitive_excludes,
      d.tags,
      d.manual,
      d.is_submodule,
      1
    ) AS fossa_node)
    FROM "Dependencies" AS d
    WHERE d.parent=locator
      AND (filter_origin_paths IS NULL AND filter_all_origin_paths IS NULL OR filter_origin_paths && d.origin_paths OR filter_all_origin_paths @> d.origin_paths)
      AND (filter_excludes IS NULL OR d.child != ALL(filter_excludes))
      AND (NOT filter_unresolved OR fossa_resolved(d.child))
      AND (d.manual OR filter_tags IS NULL OR d.child NOT LIKE 'mvn+%' OR d.unresolved_locator NOT LIKE 'mvn+%' OR d.child LIKE 'mvn+%' AND d.tags && filter_tags OR d.unresolved_locator LIKE 'mvn+%' AND d.tags && filter_tags));
  intermediate_nodes fossa_node[] := results;
  working_nodes fossa_node[] := results;
BEGIN
  WHILE current_depth <= maxdepth LOOP
    EXIT WHEN ARRAY_LENGTH(working_nodes, 1) = 0;
    EXIT WHEN ARRAY_LENGTH(working_nodes, 1) IS NULL;

    intermediate_nodes := ARRAY(
      SELECT CAST(ROW(
        d.child,
        array_agg(d.unresolved_locator),
        array_union_agg(d.origin_paths),
        array_intersect(array_intersect_agg(d.transitive_excludes), array_intersect_agg((w).excludes)),
        array_union(array_union_agg(d.tags), array_union_agg((w).tags)),
        bool_or(d.manual),
        bool_or(d.is_submodule),
        min(current_depth)
      ) AS fossa_node)
      FROM "Dependencies" AS d, UNNEST(working_nodes) AS w
      WHERE d.parent = (w).node
        AND (filter_all_origin_paths IS NULL OR filter_all_origin_paths @> d.origin_paths)
        AND NOT d.optional
        AND (filter_excludes IS NULL OR d.child != ALL(filter_excludes))
        AND (NOT filter_unresolved OR fossa_resolved(d.child))
        AND ((w).excludes IS NULL OR d.child NOT LIKE ALL((w).excludes))
        AND (d.manual OR filter_tags IS NULL OR d.child NOT LIKE 'mvn+%' OR d.unresolved_locator NOT LIKE 'mvn+%' OR d.child LIKE 'mvn+%' AND d.tags && filter_tags OR d.unresolved_locator LIKE 'mvn+%' AND d.tags && filter_tags)
      GROUP BY d.child
    );

    results := ARRAY(
      SELECT CAST(ROW(
        (r).node,
        array_union(array_union_agg((r).unresolved_locators), array_union_agg((w).unresolved_locators)),
        array_union(array_union_agg((r).origin_paths), array_union_agg((w).origin_paths)),
        array_intersect(array_intersect_agg((r).excludes), array_intersect_agg((w).excludes)),
        array_union(array_union_agg((r).tags), array_union_agg((w).tags)),
        bool_or((r).manual),
        bool_or((r).submodule),
        min((r).depth)
      ) AS fossa_node)
      FROM UNNEST(results) AS r
      LEFT JOIN UNNEST(intermediate_nodes) AS w
        ON (r).node = (w).node
      GROUP BY (r).node
    );

    working_nodes := ARRAY(
      SELECT w FROM UNNEST(intermediate_nodes) AS w
      WHERE (w).node NOT IN ( SELECT (r).node FROM UNNEST(results) AS r )
    );

    results := results || working_nodes;

    current_depth := current_depth + 1;
  END LOOP;

  RETURN results;
END; $$ LANGUAGE PLPGSQL STABLE;

DROP FUNCTION IF EXISTS fossa_dependencies(locator VARCHAR(255)) CASCADE;
CREATE OR REPLACE FUNCTION fossa_dependencies(locator TEXT) RETURNS fossa_node[] AS $$
BEGIN
  RETURN fossa_dependencies(locator, NULL, NULL, NULL, NULL, FALSE, 9999);
END; $$ LANGUAGE PLPGSQL;

COMMENT ON FUNCTION fossa_edges(TEXT, TEXT[], TEXT[], TEXT[], TEXT[], BOOLEAN, INT) IS 'pg_fossa version 1.8';
COMMENT ON FUNCTION fossa_edges(TEXT) IS 'pg_fossa version 1.8';
COMMENT ON FUNCTION fossa_dependencies(TEXT, TEXT[], TEXT[], TEXT[], TEXT[], BOOLEAN, INT) IS 'pg_fossa version 1.8';
COMMENT ON FUNCTION fossa_dependencies(TEXT) IS 'pg_fossa version 1.8';
