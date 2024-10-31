from __future__ import annotations

from collections.abc import Iterator
from itertools import groupby
from typing import Any

import pycountry
from sqlalchemy import case, event, func, literal_column
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql import expression as sqlexpr
from sqlalchemy.util import symbol


def compare_number(column: ColumnElement, value: int | float, epsilon: float = 0.01):
    """Generate a between expression to compensate for float drift."""
    return column.between(value - epsilon, value + epsilon)


def check_number(column: ColumnElement, term: str, divisor: float = 1):
    """Compare a numeric term against a column with a divisor."""
    if term.isnumeric():
        return compare_number(column, int(term) / divisor)
    try:
        return compare_number(column, float(term) / divisor)
    except ValueError:
        return False


def get_country_code(name: str) -> str | None:
    """Return the country code for a name (case-insensitive)."""
    for country in pycountry.countries:
        if country.name.lower() == name.lower():
            return country.alpha_2
    return None


def check_country(country_code, term: str):
    """Compare a country code column with a term or resolved country code."""
    term_code = get_country_code(term)
    match_code = term_code if term_code is not None else term
    return func.lower(country_code) == match_code.lower()


def get_sort_columns(
    mapping: dict[str, Any], sort_order: list[dict[str, Any]], stable_key: str = "id"
) -> list[Any]:
    """Transform a sort order into SQLAlchemy expressions."""
    sort_columns = []
    sort_has_stable_key = False
    for item in sort_order:
        column = mapping.get(item["name"])
        if column is not None:
            if item["name"] == stable_key:
                sort_has_stable_key = True
            if item["direction"] == "desc":
                sort_columns.append(column.desc().nullslast())
            else:
                sort_columns.append(column.asc().nullsfirst())

    if not sort_has_stable_key and stable_key in mapping:
        if sort_order and sort_order[0]["direction"] == "asc":
            sort_columns.append(mapping[stable_key].asc())
        else:
            sort_columns.append(mapping[stable_key].desc())

    return sort_columns


def labeled(column):
    """Attach a label to a column as table_name + _ + attribute key."""
    return column.label(f"{column.class_.__tablename__}_{column.key}")


def sql_maybe_and(*args):
    """Build a SQL AND expression, allowing empty lists."""
    filtered = list(filter(lambda arg: not isinstance(arg, list) or arg, args))
    return sqlexpr.and_(*filtered) if filtered else sqlexpr.true()


def ilike_substr(term: str):
    """Create a case-insensitive substring filter for a column."""
    escaped = term.replace("%", "\\%").replace("_", "\\_")

    def make_filter(column):
        return column.ilike(f"%{escaped}%")

    return make_filter


class ImmutableColumnError(AttributeError):
    def __init__(self, cls, column, old_value, new_value, message=None):
        self.cls = cls
        self.column = column
        self.old_value = old_value
        self.new_value = new_value
        if message is None:
            self.message = (
                "Cannot update column {} on model {} from {} to {}: column is non-updatable.".format(
                    column, cls, old_value, new_value
                )
            )


def make_immutable(col):
    """Make a SQLAlchemy column immutable once set."""

    @event.listens_for(col, "set")
    def col_set_listener(_target, value, old_value, _initiator):
        if old_value != symbol("NEVER_SET") and old_value != symbol("NO_VALUE") and old_value != value:
            raise ImmutableColumnError(col.class_.__name__, col.name, old_value, value)


def weighted_avg(value, weight):
    """Generate a SQL term for weighted average."""
    return case((func.sum(weight) == 0, None), else_=func.sum(value * weight) / func.sum(weight))


def label_columns(columns: dict[str, Any]) -> list[Any]:
    """Return labeled SQLAlchemy expressions from a dict of columns."""
    return [expr.label(name) for name, expr in columns.items()]


def set_attribute(entity: dict[str, Any], path: str, value: Any):
    """Set a nested attribute by a path separated by '__'."""
    parts = path.split("__")
    last = parts.pop()

    cursor = entity
    for name in parts:
        if name not in cursor:
            cursor[name] = {}
        elif not isinstance(cursor[name], dict):
            raise ValueError("Entity contains conflicting values, expected a dict.")
        cursor = cursor[name]

    cursor[last] = value
    return entity


def entity_to_dict(entity, transform_attrs: bool = True, include_privates: bool = False) -> dict[str, Any]:
    """Convert a SQLAlchemy entity to a dict."""
    if transform_attrs:
        result: dict[str, Any] = {}
        for key, value in entity._asdict().items():
            if not key.startswith("_") or include_privates:
                set_attribute(result, key, value)
    else:
        result = entity._asdict()

    return result


def group_result_set(result: Iterator, key_attr: str = "id") -> list[dict[str, Any]]:
    """Group a result set by a stable key and merge duplicate rows."""
    grouped = []
    for _, group in groupby(result, lambda row: getattr(row, key_attr)):
        grouped_entity: dict[str, Any] = {}
        for entity_dict in result_list_to_dict(group, True):
            for key, value in entity_dict.items():
                existing = grouped_entity.get(key)
                if existing and existing != value:
                    if isinstance(existing, list):
                        grouped_entity[key].append(value)
                    else:
                        grouped_entity[key] = [existing, value]
                else:
                    grouped_entity[key] = value
        grouped.append(grouped_entity)
    return grouped


def result_list_to_dict(result_list, transform_attrs: bool = True, include_privates: bool = False) -> list[dict]:
    """Convert a list of SQLAlchemy entities to dicts."""
    return [entity_to_dict(e, transform_attrs, include_privates) for e in result_list]


def literal_str(value: str):
    """Use a literal string in SQLAlchemy queries (no escaping)."""
    return literal_column("'{}'".format(value))
