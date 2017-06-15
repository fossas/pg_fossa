
CREATE OR REPLACE FUNCTION fossa_version() RETURNS VARCHAR as $$
BEGIN
  RETURN '1.3';
END; $$ LANGUAGE PLPGSQL IMMUTABLE;

CREATE OR REPLACE FUNCTION fossa_edges(locator VARCHAR(255), filter_tags VARCHAR[], filter_excludes VARCHAR[], filter_unresolved BOOLEAN, maxdepth INT) RETURNS fossa_edge[] AS $$
DECLARE
  current_depth INT := 2;
  results fossa_edge[] := ARRAY(SELECT CAST(ROW(
      d.parent,
      fossa_child(d.child, d.unresolved_locator),
      fossa_resolved(d.child),
      d.transitive_excludes,
      d.tags,
      d.manual,
      d.is_submodule,
      1
    ) AS fossa_edge)
    FROM "Dependencies" AS d
    WHERE d.parent=locator
      AND (filter_excludes IS NULL OR d.child != ALL(filter_excludes))
      AND (NOT filter_unresolved OR fossa_resolved(d.child)));
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
        array_intersect(array_intersect_agg(d.transitive_excludes), array_intersect_agg((w).excludes)),
        array_union(array_union_agg(d.tags), array_union_agg((w).tags)),
        bool_or(d.manual),
        bool_or(d.is_submodule),
        min(current_depth)
      ) AS fossa_edge)
      FROM "Dependencies" AS d, UNNEST(working_edges) AS w
      WHERE d.parent = (w).child
        AND NOT d.optional
        AND (filter_excludes IS NULL OR d.child != ALL(filter_excludes))
        AND (NOT filter_unresolved OR fossa_resolved(d.child))
        AND ((w).excludes IS NULL OR d.child NOT LIKE ALL((w).excludes))
        AND (d.manual OR filter_tags IS NULL OR d.child NOT LIKE 'mvn+%' OR filter_tags IS NOT NULL AND d.child LIKE 'mvn+%' AND d.tags && filter_tags)
      GROUP BY d.parent, fossa_child(d.child, d.unresolved_locator), fossa_resolved(d.child)
    );

    results := ARRAY(
      SELECT CAST(ROW(
        (r).parent,
        (r).child,
        (r).resolved,
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

CREATE OR REPLACE FUNCTION fossa_dependencies(locator VARCHAR, filter_tags VARCHAR[], filter_excludes VARCHAR[], filter_unresolved BOOLEAN, maxdepth INT) RETURNS fossa_node[] AS $$
DECLARE
  current_depth INT := 2;
  results fossa_node[] := ARRAY(SELECT CAST(ROW(
      d.child,
      ARRAY[d.unresolved_locator],
      d.transitive_excludes,
      d.tags,
      d.manual,
      d.is_submodule,
      1
    ) AS fossa_node)
    FROM "Dependencies" AS d
    WHERE d.parent=locator
      AND (filter_excludes IS NULL OR d.child != ALL(filter_excludes))
      AND (NOT filter_unresolved OR fossa_resolved(d.child)));
  intermediate_nodes fossa_node[] := results;
  working_nodes fossa_node[] := results;
BEGIN
  WHILE current_depth <= maxdepth LOOP
    EXIT WHEN ARRAY_LENGTH(working_nodes, 1) = 0;
    EXIT WHEN ARRAY_LENGTH(working_nodes, 1) IS NULL;

    intermediate_nodes := ARRAY(
      SELECT CAST(ROW(
        d.child,
        array_union(array_agg(d.unresolved_locator), array_union_agg((w).unresolved_locators)),
        array_intersect(array_intersect_agg(d.transitive_excludes), array_intersect_agg((w).excludes)),
        array_union(array_union_agg(d.tags), array_union_agg((w).tags)),
        bool_or(d.manual),
        bool_or(d.is_submodule),
        min(current_depth)
      ) AS fossa_node)
      FROM "Dependencies" AS d, UNNEST(working_nodes) AS w
      WHERE d.parent = (w).node
        AND NOT d.optional
        AND (filter_excludes IS NULL OR d.child != ALL(filter_excludes))
        AND (NOT filter_unresolved OR fossa_resolved(d.child))
        AND ((w).excludes IS NULL OR d.child NOT LIKE ALL((w).excludes))
        AND (d.manual OR filter_tags IS NULL OR d.child NOT LIKE 'mvn+%' OR filter_tags IS NOT NULL AND d.child LIKE 'mvn+%' AND d.tags && filter_tags)
      GROUP BY d.child
    );

    results := ARRAY(
      SELECT CAST(ROW(
        (r).node,
        array_union(array_union_agg((r).unresolved_locators), array_union_agg((w).unresolved_locators)),
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
