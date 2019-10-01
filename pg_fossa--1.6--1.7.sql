CREATE OR REPLACE FUNCTION fossa_version() RETURNS VARCHAR as $$
BEGIN
  RETURN '1.7';
END; $$ LANGUAGE PLPGSQL IMMUTABLE;

CREATE OR REPLACE FUNCTION fossa_edges(locator VARCHAR(255), filter_tags VARCHAR[], filter_excludes VARCHAR[], filter_origin_paths VARCHAR[], filter_all_origin_paths VARCHAR[], filter_unresolved BOOLEAN, maxdepth INT) RETURNS fossa_edge[] AS $$
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

CREATE OR REPLACE FUNCTION fossa_edges(locator VARCHAR(255)) RETURNS fossa_edge[] AS $$
BEGIN
  RETURN fossa_edges(locator, NULL, NULL, NULL, NULL, FALSE, 5);
END; $$ LANGUAGE PLPGSQL;

COMMENT ON FUNCTION fossa_edges(VARCHAR(255), VARCHAR[], VARCHAR[], VARCHAR[], VARCHAR[], BOOLEAN, INT) IS 'pg_fossa version 1.7';
COMMENT ON FUNCTION fossa_edges(VARCHAR(255)) IS 'pg_fossa version 1.7';
COMMENT ON FUNCTION fossa_dependencies(VARCHAR(255), VARCHAR[], VARCHAR[], VARCHAR[], VARCHAR[], BOOLEAN, INT) IS 'pg_fossa version 1.7';
COMMENT ON FUNCTION fossa_dependencies(VARCHAR(255)) IS 'pg_fossa version 1.7';
