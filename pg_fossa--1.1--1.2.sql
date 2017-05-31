
DROP FUNCTION fossa_dependencies_maxdepth(locator VARCHAR(255), maxdepth INT);
DROP TYPE fossa_dependencies_graph_metadata CASCADE;

CREATE TYPE fossa_nodes_results AS (
  node VARCHAR,
  level INT,
  tags VARCHAR[],
  manual BOOLEAN,
  submodule BOOLEAN,
  resolved BOOLEAN
);
CREATE TYPE fossa_edges_results AS (
  parent VARCHAR,
  child VARCHAR,
  level INT,
  tags VARCHAR[],
  manual BOOLEAN,
  submodule BOOLEAN,
  resolved BOOLEAN
);

CREATE OR REPLACE FUNCTION array_symmetric_difference(a1 VARCHAR[], a2 VARCHAR[]) RETURNS VARCHAR[] as $$
BEGIN
  RETURN ARRAY( SELECT DISTINCT e FROM (
    SELECT * FROM ( SELECT UNNEST(a1) EXCEPT SELECT UNNEST(a2) ) AS t1
    UNION ALL
    SELECT * FROM ( SELECT UNNEST(a2) EXCEPT SELECT UNNEST(a1) ) AS t1
  ) dt(e) ORDER BY 1 );
END; $$ LANGUAGE PLPGSQL IMMUTABLE;

CREATE OR REPLACE FUNCTION array_sort_unique(a VARCHAR[]) RETURNS VARCHAR[] as $$
BEGIN
  RETURN ARRAY(SELECT DISTINCT UNNEST(a) ORDER BY 1);
END; $$ LANGUAGE PLPGSQL IMMUTABLE;

CREATE OR REPLACE FUNCTION array_union(a1 VARCHAR[], a2 VARCHAR[]) RETURNS VARCHAR[] as $$
BEGIN
  IF a1 is NULL THEN
    RETURN a2;
  ELSEIF a2 is NULL THEN
    RETURN a1;
  END IF;
  RETURN ( SELECT ARRAY_AGG(e) FROM (SELECT UNNEST(a1) UNION SELECT UNNEST(a2)) as dt(e) );
END; $$ LANGUAGE PLPGSQL IMMUTABLE;

DROP AGGREGATE IF EXISTS array_union_agg(VARCHAR[]);
CREATE AGGREGATE array_union_agg(VARCHAR[]) (
  SFUNC = array_union,
  STYPE = VARCHAR[]
);

CREATE OR REPLACE FUNCTION array_intersect(a1 VARCHAR[], a2 VARCHAR[]) RETURNS VARCHAR[] as $$
BEGIN
  IF a1 is NULL THEN
    RETURN a2;
  ELSEIF a2 is NULL THEN
    RETURN a1;
  END IF;
  RETURN ( SELECT ARRAY_AGG(e) FROM (SELECT UNNEST(a1) INTERSECT SELECT UNNEST(a2)) as dt(e) );
END; $$ LANGUAGE PLPGSQL IMMUTABLE;

DROP AGGREGATE IF EXISTS array_intersect_agg(VARCHAR[]);
CREATE AGGREGATE array_intersect_agg(VARCHAR[]) (
  SFUNC = array_intersect,
  STYPE = VARCHAR[]
);

CREATE OR REPLACE FUNCTION fossa_null_dependency_helper(locator VARCHAR, unresolved_locator VARCHAR) RETURNS VARCHAR as $$
BEGIN
  RETURN CASE WHEN locator='NULL' THEN unresolved_locator ELSE locator END;
END; $$ LANGUAGE PLPGSQL IMMUTABLE;

CREATE OR REPLACE FUNCTION fossa_edges(locator VARCHAR(255), tags VARCHAR[], maxdepth INT) RETURNS SETOF fossa_edges_results AS $$
DECLARE
  current_depth INT := 1;
  current_tags VARCHAR[] := tags;
BEGIN
  CREATE TEMP TABLE edges (
    parent VARCHAR(255),
    child VARCHAR(255),
    excludes VARCHAR[],
    level INT,
    tags VARCHAR[],
    manual BOOLEAN,
    submodule BOOLEAN,
    resolved BOOLEAN
  ) ON COMMIT DROP;
  CREATE TEMP TABLE working_edges (
    parent VARCHAR(255),
    child VARCHAR(255),
    excludes VARCHAR[],
    level INT,
    tags VARCHAR[],
    manual BOOLEAN,
    submodule BOOLEAN,
    resolved BOOLEAN
  ) ON COMMIT DROP;
  CREATE TEMP TABLE intermediate_edges (
    parent VARCHAR(255),
    child VARCHAR(255),
    excludes VARCHAR[],
    level INT,
    tags VARCHAR[],
    manual BOOLEAN,
    submodule BOOLEAN,
    resolved BOOLEAN
  ) ON COMMIT DROP;

  INSERT INTO working_edges
    SELECT
      d.parent AS parent,
      fossa_null_dependency_helper(d.child, d.unresolved_locator) AS child,
      d.transitive_excludes AS excludes,
      current_depth AS level,
      d.tags,
      d.manual,
      d.is_submodule AS submodule,
      d.child != 'NULL'
    FROM "Dependencies" AS d
    WHERE d.parent=locator
      AND (d.manual OR current_tags IS NULL OR d.child NOT LIKE 'mvn+%' OR current_tags IS NOT NULL AND d.child LIKE 'mvn+%' AND d.tags && current_tags);
  INSERT INTO edges SELECT * FROM working_edges;

  WHILE current_depth < maxdepth LOOP
    RAISE NOTICE 'Current working table size % at depth %', ( SELECT COUNT(*) FROM working_edges ), current_depth;
    EXIT WHEN ( SELECT COUNT(*) FROM working_edges ) = 0;

    WITH f AS (
      SELECT
        d.parent AS parent,
        fossa_null_dependency_helper(d.child, d.unresolved_locator) AS child,
        array_sort_unique(d.transitive_excludes || w.excludes) AS excludes,
        current_depth AS level,
        d.tags,
        d.manual,
        d.is_submodule AS submodule,
        d.child != 'NULL'
      FROM "Dependencies" AS d
      INNER JOIN working_edges AS w ON d.parent=w.child
      WHERE d.child NOT LIKE ALL(w.excludes)
        AND (d.manual OR current_tags IS NULL OR d.child NOT LIKE 'mvn+%' OR current_tags IS NOT NULL AND d.child LIKE 'mvn+%' AND d.tags && current_tags)
    ) INSERT INTO intermediate_edges SELECT * FROM f;

    TRUNCATE working_edges;

    WITH s1 AS (
      SELECT
        i.parent AS parent,
        i.child AS child,
        array_intersect_agg(i.excludes) AS excludes,
        MIN(i.level) AS level,
        array_union_agg(i.tags) AS tags,
        BOOL_AND(i.manual) AS manual,
        BOOL_AND(i.submodule) AS submodule,
        i.resolved AS resolved
      FROM intermediate_edges AS i
      GROUP BY i.parent, i.child, i.resolved
    ), s2 AS (
      SELECT parent,
             child,
             array_sort_unique(excludes) AS excludes,
             level,
             array_sort_unique(s1.tags) AS tags,
             manual,
             submodule,
             resolved
        FROM s1
    ), insert_edges AS (
      SELECT * FROM s2
        WHERE (s2.parent, s2.child, s2.resolved) NOT IN ( SELECT e.parent, e.child, e.resolved FROM edges AS e )
    ), update_edges AS (
      SELECT s2.parent,
             s2.child,
             array_sort_unique(array_intersect(s2.excludes, e.excludes)),
             s2.level,
             array_sort_unique(array_union(s2.tags, e.tags)),
             s2.manual,
             s2.submodule,
             s2.resolved
        FROM s2
        INNER JOIN edges AS e
        ON e.parent = s2.parent AND e.child = s2.child AND e.resolved = s2.resolved
        WHERE s2.tags != e.tags
          OR s2.excludes != e.excludes
          AND s2.excludes <@ e.excludes
    ), r1 AS (
      INSERT INTO edges SELECT * FROM insert_edges
    ), r2 AS (
      UPDATE edges SET excludes=excludes FROM update_edges
      WHERE edges.parent=update_edges.parent
        AND edges.child=update_edges.child
        AND edges.resolved=update_edges.resolved
    ) INSERT INTO working_edges SELECT * FROM insert_edges;

    TRUNCATE intermediate_edges;

    current_depth := current_depth + 1;
  END LOOP;

  RETURN QUERY SELECT parent, child, level, edges.tags, manual, submodule, resolved FROM edges;
END; $$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION fossa_edges(locator VARCHAR(255), maxdepth INT) RETURNS SETOF fossa_edges_results AS $$
BEGIN
  RETURN QUERY SELECT * FROM fossa_edges(locator, NULL, maxdepth);
END; $$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION fossa_dependencies(locator VARCHAR(255), tags VARCHAR[], maxdepth INT) RETURNS SETOF fossa_nodes_results AS $$
BEGIN
  RETURN QUERY SELECT child, level, e.tags, manual, submodule, resolved FROM fossa_edges(locator, tags, maxdepth) AS e;
END; $$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION fossa_dependencies(locator VARCHAR(255), maxdepth INT) RETURNS SETOF fossa_nodes_results AS $$
BEGIN
  RETURN QUERY SELECT child, level, e.tags, manual, submodule, resolved FROM fossa_edges(locator, maxdepth) AS e;
END; $$ LANGUAGE PLPGSQL;

COMMENT ON FUNCTION fossa_edges(VARCHAR(255), INT) IS 'pg_fossa version 1.2';
COMMENT ON FUNCTION fossa_edges(VARCHAR(255), VARCHAR[], INT) IS 'pg_fossa version 1.2';
COMMENT ON FUNCTION fossa_dependencies(VARCHAR(255), INT) IS 'pg_fossa version 1.2';
COMMENT ON FUNCTION fossa_dependencies(VARCHAR(255), VARCHAR[], INT) IS 'pg_fossa version 1.2';
